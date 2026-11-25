/*
Copyright 2026 The gateway-api-openstack Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	controllermetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
	gatewayconsts "sigs.k8s.io/gateway-api/pkg/consts"

	configv1alpha1 "github.com/gateway-api-openstack/gateway-api-openstack/api/v1alpha1"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/backendca"
	openstackobservability "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/observability"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/certificates"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gateway"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/gatewayclass"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/httproute"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/l4route"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/listenerset"
)

// version is overridden at build time with -X main.version=<release>.
var version = "dev"

var setupLog = ctrl.Log.WithName("setup")

func main() {
	if err := run(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Could not run controller manager")
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	resyncDefault, err := durationEnvOr("GATEWAY_OPENSTACK_RESYNC_INTERVAL", controller.DefaultOpenStackResyncInterval)
	if err != nil {
		return err
	}
	apiQPSDefault, err := float64EnvOr("GATEWAY_OPENSTACK_API_QPS", openstack.DefaultAPIQPS)
	if err != nil {
		return err
	}
	apiBurstDefault, err := intEnvOr("GATEWAY_OPENSTACK_API_BURST", openstack.DefaultAPIBurst)
	if err != nil {
		return err
	}
	var (
		controllerName     string
		gatewayAPIChannel  string
		certificateMap     string
		backendClientMap   string
		backendTLSPolicy   bool
		grpcContentType    bool
		fixedHTTPResponses bool
		weightedTargets    bool
		clusterID          string
		region             string
		microversion       string
		cloudsYAML         string
		cloudName          string
		metricsAddress     string
		healthAddress      string
		leaderElection     bool
		allowInsecure      bool
		operationTimeout   time.Duration
		pollInterval       time.Duration
		resyncInterval     time.Duration
		apiQPS             float64
		apiBurst           int
	)
	var httpsListenerIsolation, initialL7Rules, l7PolicyBatchActivation bool
	var frontendClientValidation bool
	var httpRequestRedirect bool
	var httpRequestURLRewrite bool
	var httpCORS bool
	var gatewayInfrastructureMetadata, gatewayAddressRoutabilityCluster bool
	var httpBackendRequestFilters bool
	var exactHTTPHeaders, exactHTTPQueryParams, httpMethodMatch, requestHeaderModifier, responseHeaderModifier, backendH2C, tlsSNIPassthrough, tlsSNISuffix, tcpTLSTermination bool
	zapOptions := zap.Options{Development: false}
	zapOptions.BindFlags(flag.CommandLine)
	flag.StringVar(&controllerName, "controller-name", envOr("GATEWAY_OPENSTACK_CONTROLLER_NAME", configv1alpha1.GroupName+"/gateway-api-openstack"), "Gateway API controllerName for this installation")
	flag.StringVar(&gatewayAPIChannel, "gateway-api-channel", envOr("GATEWAY_OPENSTACK_GATEWAY_API_CHANNEL", "standard"), "Gateway API release channel: standard or experimental")
	flag.BoolVar(&gatewayInfrastructureMetadata, "gateway-infrastructure-metadata", false, "publish discovered Octavia VIPs through headless Services with Gateway infrastructure metadata")
	flag.BoolVar(&gatewayAddressRoutabilityCluster, "gateway-address-routability-cluster", false, "serve Cluster Gateway addresses through Kubernetes Service ClusterIPs")
	flag.BoolVar(&exactHTTPHeaders, "octavia-http-header-exact", false, "use the advertised Octavia exact HTTP header matching capability")
	flag.BoolVar(&httpRequestRedirect, "octavia-http-request-redirect", false, "use the advertised Octavia capability for request-hostname preservation and redirect path modifiers")
	flag.BoolVar(&exactHTTPQueryParams, "octavia-http-query-param-exact", false, "use the advertised Octavia exact HTTP query parameter matching capability (requires --octavia-fixed-http-responses)")
	flag.BoolVar(&httpMethodMatch, "octavia-http-method-match", false, "use the advertised Octavia exact HTTP method matching capability (requires --octavia-fixed-http-responses)")
	flag.BoolVar(&fixedHTTPResponses, "octavia-fixed-http-responses", false, "use the advertised Octavia fixed HTTP response capability")
	flag.BoolVar(&weightedTargets, "octavia-weighted-http-targets", false, "use the advertised Octavia weighted HTTP target capability (requires --octavia-fixed-http-responses)")
	flag.BoolVar(&tlsSNIPassthrough, "octavia-tls-sni-passthrough", false, "use the advertised Octavia TCP SNI passthrough capability for TLSRoutes")
	flag.BoolVar(&tcpTLSTermination, "octavia-tcp-tls-termination", false, "use the advertised Octavia TCP termination capability for TLSRoutes with operator-enrolled certificates")
	flag.BoolVar(&tlsSNISuffix, "octavia-tls-sni-suffix", false, "use the advertised Octavia wildcard SNI capability (requires --octavia-tls-sni-passthrough)")
	flag.BoolVar(&backendTLSPolicy, "octavia-backend-tls-policy", false, "authenticate BackendTLSPolicy upstreams with the regional Octavia hostname capability and a Barbican CA (requires --octavia-microversion=2.8 or newer and regional h2 enforcement for GRPCRoute)")
	flag.BoolVar(&l7PolicyBatchActivation, "octavia-l7policy-batch-activation", false, "enable prepared HTTP forwarding policies together when the regional Octavia provider advertises support")
	flag.BoolVar(&frontendClientValidation, "octavia-frontend-client-validation", false, "validate client certificates on configured HTTPS ports (requires --tls-certificate-map and Octavia microversion 2.8 or newer)")
	flag.BoolVar(&initialL7Rules, "octavia-l7policy-initial-rules", false, "create disabled HTTP(S) policies with their initial rules when the regional Octavia provider advertises support")
	flag.BoolVar(&httpsListenerIsolation, "octavia-https-listener-isolation", false, "isolate terminated HTTPS sections on a shared port using the regional Octavia capability (requires --tls-certificate-map)")
	flag.BoolVar(&httpBackendRequestFilters, "octavia-http-backend-request-filters", false, "use the advertised Octavia capability for HTTP backendRef request filters (requires --octavia-weighted-http-targets)")
	flag.BoolVar(&httpRequestURLRewrite, "octavia-request-url-rewrite", false, "use the advertised Octavia L7 request URL rewrite capability")
	flag.BoolVar(&httpCORS, "octavia-request-cors", false, "serve rule-level HTTPRoute CORS via the Octavia request_cors_v1 capability")
	flag.BoolVar(&responseHeaderModifier, "octavia-response-header-modifier", false, "use the advertised Octavia L7 response header operations capability")
	flag.BoolVar(&grpcContentType, "octavia-grpc-content-type", false, "use the advertised Octavia native gRPC Content-Type rule for GRPCRoutes")
	flag.BoolVar(&backendH2C, "octavia-backend-h2c", false, "use the advertised Octavia h2c backend transport capability")
	flag.BoolVar(&requestHeaderModifier, "octavia-request-header-modifier", false, "use the advertised Octavia L7 request header operations capability")
	flag.StringVar(&certificateMap, "tls-certificate-map", envOr("GATEWAY_OPENSTACK_TLS_CERTIFICATE_MAP", ""), "operator-managed TLS revision map for terminating listeners")
	flag.StringVar(&backendClientMap, "backend-client-certificate-map", envOr("GATEWAY_OPENSTACK_BACKEND_CLIENT_CERTIFICATE_MAP", ""), "operator-managed TLS revision map for Gateway client certificates to TLS backends (requires --octavia-backend-tls-policy)")
	flag.StringVar(&clusterID, "cluster-id", envOr("GATEWAY_OPENSTACK_CLUSTER_ID", ""), "stable unique identifier for this Kubernetes cluster (required)")
	flag.StringVar(&region, "openstack-region", envOr("OS_REGION_NAME", ""), "OpenStack region; defaults to clouds.yaml or OS_REGION_NAME")
	flag.StringVar(&microversion, "octavia-microversion", "2.5", "Octavia API microversion (2.5 or later is required for resource tags)")
	flag.StringVar(&cloudsYAML, "clouds-yaml", envOr("OS_CLIENT_CONFIG_FILE", ""), "path to clouds.yaml; when empty, standard OS_* variables are used")
	flag.StringVar(&cloudName, "openstack-cloud", envOr("OS_CLOUD", ""), "clouds.yaml entry selected with --clouds-yaml")
	flag.StringVar(&metricsAddress, "metrics-bind-address", ":8080", "metrics endpoint bind address")
	flag.StringVar(&healthAddress, "health-probe-bind-address", ":8081", "health probe bind address")
	flag.BoolVar(&leaderElection, "leader-elect", true, "enable leader election")
	flag.BoolVar(&allowInsecure, "insecure", false, "disable OpenStack TLS verification; test clouds only")
	flag.DurationVar(&operationTimeout, "openstack-operation-timeout", 10*time.Minute, "maximum duration of one OpenStack reconciliation call")
	flag.DurationVar(&pollInterval, "openstack-poll-interval", 2*time.Second, "delay before observing an asynchronous Octavia operation again")
	flag.DurationVar(&resyncInterval, "openstack-resync-interval", resyncDefault, "interval for observing converged OpenStack resources again")
	flag.Float64Var(&apiQPS, "openstack-api-qps", apiQPSDefault, "maximum average OpenStack API requests per second for this process")
	flag.IntVar(&apiBurst, "openstack-api-burst", apiBurstDefault, "maximum burst of OpenStack API requests for this process")
	flag.Parse()
	if weightedTargets && !fixedHTTPResponses {
		return fmt.Errorf("--octavia-weighted-http-targets requires --octavia-fixed-http-responses")
	}
	if httpBackendRequestFilters && !weightedTargets {
		return fmt.Errorf("--octavia-http-backend-request-filters requires --octavia-weighted-http-targets")
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOptions)))
	openStackClientConfig := openstack.ClientConfig{
		Region:         region,
		Microversion:   microversion,
		RequirePoolTLS: backendTLSPolicy,
		CloudsYAMLPath: cloudsYAML,
		CloudName:      cloudName,
		AllowInsecure:  allowInsecure,
		APIQPS:         apiQPS,
		APIBurst:       apiBurst,

		RequireFrontendClientValidation: frontendClientValidation,
	}
	if err := openStackClientConfig.Validate(); err != nil {
		return fmt.Errorf("invalid OpenStack client configuration: %w", err)
	}

	controllerConfig := controller.Config{
		GatewayInfrastructureMetadata: gatewayInfrastructureMetadata,
		ClusterGatewayAddresses:       gatewayAddressRoutabilityCluster,
		ControllerName:                gatewayv1.GatewayController(controllerName),
		ControllerVersion:             version,
		GatewayAPIChannel:             gatewayAPIChannel,
		HTTPHeaderExact:               exactHTTPHeaders,
		HTTPQueryParamExact:           exactHTTPQueryParams,
		HTTPRequestURLRewrite:         httpRequestURLRewrite,
		HTTPCORS:                      httpCORS,
		BackendRequestFilters:         httpBackendRequestFilters,
		HTTPRequestRedirect:           httpRequestRedirect,
		HTTPMethodMatch:               httpMethodMatch,
		FixedHTTPResponses:            fixedHTTPResponses,
		WeightedHTTPTargets:           weightedTargets,
		RequestHeaderModifier:         requestHeaderModifier,
		ResponseHeaderModifier:        responseHeaderModifier,
		BackendH2C:                    backendH2C,
		BackendTLSPolicy:              backendTLSPolicy,
		OperatorHTTPS:                 certificateMap != "",
		HTTPSListenerIsolation:        httpsListenerIsolation,
		TLSSNIPassthrough:             tlsSNIPassthrough,
		TLSSNISuffix:                  tlsSNISuffix,
		TCPTLSTermination:             tcpTLSTermination,
		ClusterID:                     clusterID,
		OpenStackResyncInterval:       resyncInterval,

		BackendClientCertificate: backendClientMap != "",
		FrontendClientValidation: frontendClientValidation,
	}
	if err := controllerConfig.Validate(); err != nil {
		return fmt.Errorf("invalid controller configuration: %w", err)
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(discoveryv1.AddToScheme(scheme))
	utilruntime.Must(apiextensionsv1.AddToScheme(scheme))
	utilruntime.Must(gatewayv1.Install(scheme))
	utilruntime.Must(configv1alpha1.AddToScheme(scheme))
	if controllerConfig.XBackendEnabled() {
		utilruntime.Must(gatewayx.Install(scheme))
	}

	restConfig := ctrl.GetConfigOrDie()
	if err := requireGatewayAPIs(restConfig, controllerConfig); err != nil {
		return err
	}
	manager, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                        scheme,
		Metrics:                       metricsserver.Options{BindAddress: metricsAddress},
		HealthProbeBindAddress:        healthAddress,
		LeaderElection:                leaderElection,
		LeaderElectionID:              "gateway-api-openstack-controller",
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		return fmt.Errorf("create controller manager: %w", err)
	}
	if err := configureOpenStackRequestMetrics(&openStackClientConfig, controllermetrics.Registry); err != nil {
		return err
	}
	serviceClients, err := openstack.NewServiceClients(ctx, openStackClientConfig)
	if err != nil {
		return err
	}
	controllerConfig.OpenStackProjectID = serviceClients.ProjectID
	providerConfig := openstack.ProviderConfig{
		BackendCA:              backendca.New(clusterID, serviceClients),
		BackendTLSPolicy:       backendTLSPolicy,
		HTTPHeaderExact:        exactHTTPHeaders,
		HTTPQueryParamExact:    exactHTTPQueryParams,
		HTTPRequestURLRewrite:  httpRequestURLRewrite,
		HTTPCORS:               httpCORS,
		BackendRequestFilters:  httpBackendRequestFilters,
		HTTPRequestRedirect:    httpRequestRedirect,
		HTTPMethodMatch:        httpMethodMatch,
		FixedHTTPResponses:     fixedHTTPResponses,
		WeightedHTTPTargets:    weightedTargets,
		RequestHeaderModifier:  requestHeaderModifier,
		ResponseHeaderModifier: responseHeaderModifier,
		BackendH2C:             backendH2C,
		GRPCContentType:        grpcContentType,
		TLSSNIPassthrough:      tlsSNIPassthrough,
		TLSSNISuffix:           tlsSNISuffix,
		TCPTLSTermination:      tcpTLSTermination,
		OperationTimeout:       operationTimeout,
		PollInterval:           pollInterval,

		HTTPSListenerIsolation:  httpsListenerIsolation,
		InitialL7Rules:          initialL7Rules,
		L7PolicyBatchActivation: l7PolicyBatchActivation,
	}
	openStackProvider := openstack.NewProvider(serviceClients, providerConfig)
	if err := controller.SetupIndexes(ctx, manager.GetFieldIndexer(), controllerConfig); err != nil {
		return fmt.Errorf("set up controller field indexes: %w", err)
	}
	graphCoordinator := &graph.Coordinator{}
	topologyResolver := openstack.NewTopologyResolver(serviceClients, operationTimeout)
	eventRecorder := manager.GetEventRecorder("gateway-api-openstack-controller")
	if err := (&gatewayclass.Reconciler{Client: manager.GetClient(), APIReader: manager.GetAPIReader(), Config: controllerConfig, TopologyResolver: topologyResolver, Recorder: eventRecorder}).SetupWithManager(manager); err != nil {
		return fmt.Errorf("set up GatewayClass controller: %w", err)
	}
	gatewayReconciler := &gateway.Reconciler{
		TopologyResolver: topologyResolver,
		Client:           manager.GetClient(),
		Coordinator:      graphCoordinator,
		APIReader:        manager.GetAPIReader(),
		Recorder:         eventRecorder,
		Config:           controllerConfig,
	}
	if controllerConfig.OperatorHTTPS || controllerConfig.BackendClientCertificate {
		resolver := &certificates.Resolver{
			Path: certificateMap, BackendClientPath: backendClientMap, Reader: manager.GetAPIReader(),
			ClusterID: controllerConfig.ClusterID, ProjectID: serviceClients.ProjectID, Region: serviceClients.Region,
			DiscoverEndpoint: func() (string, error) {
				// Catalog availability can change while other Gateway listeners continue serving.
				keyManager, err := serviceClients.NewKeyManagerClient()
				if err != nil {
					return "", err
				}
				return keyManager.Endpoint, nil
			},
		}
		if controllerConfig.OperatorHTTPS {
			gatewayReconciler.HTTPSListenerIsolationAvailable = openStackProvider.HTTPSListenerIsolationAvailable
			gatewayReconciler.Certificates = resolver
		}
		if controllerConfig.BackendClientCertificate {
			gatewayReconciler.BackendClientCertificates = resolver
		}
	}
	routeReconciler := &httproute.Reconciler{
		ListenerSetObserver:     gatewayReconciler.ObserveListenerSets,
		BackendTLSAvailable:     openStackProvider.BackendTLSAvailable,
		TCPBackendTLSAvailable:  openStackProvider.TCPBackendTLSAvailable,
		GRPCBackendTLSAvailable: openStackProvider.GRPCBackendTLSAvailable,
		BackendTLSSANAvailable:  openStackProvider.BackendTLSSANAvailable,
		Client:                  manager.GetClient(),
		Coordinator:             graphCoordinator,
		APIReader:               manager.GetAPIReader(),
		Recorder:                eventRecorder,
		Config:                  controllerConfig,
	}
	tcpReconciler := &l4route.Reconciler{
		Protocol: gatewayv1.TCPProtocolType,
		Client:   manager.GetClient(), APIReader: manager.GetAPIReader(), Config: controllerConfig, Coordinator: graphCoordinator,
	}
	udpReconciler := &l4route.Reconciler{
		Protocol: gatewayv1.UDPProtocolType,
		Client:   manager.GetClient(), APIReader: manager.GetAPIReader(), Config: controllerConfig, Coordinator: graphCoordinator,
	}
	writerMetrics, err := graph.RegisterWriterMetrics(controllermetrics.Registry)
	if err != nil {
		return fmt.Errorf("register Gateway graph metrics: %w", err)
	}
	writer := &graph.Writer{
		Observer: writerMetrics, Coordinator: graphCoordinator, Gateway: gatewayReconciler,
		Routes: graph.RouteSources{routeReconciler, tcpReconciler, udpReconciler}, Provider: openStackProvider,
	}
	gatewayReconciler.Writer, routeReconciler.Writer, tcpReconciler.Writer, udpReconciler.Writer = writer, writer, writer, writer
	if err := gatewayReconciler.SetupWithManager(manager); err != nil {
		return fmt.Errorf("set up Gateway controller: %w", err)
	}
	if err := listenerset.SetupWithManager(ctx, manager, controllerConfig.ControllerName); err != nil {
		return fmt.Errorf("set up ListenerSet controller: %w", err)
	}
	if err := routeReconciler.SetupWithManager(manager); err != nil {
		return fmt.Errorf("set up HTTPRoute controller: %w", err)
	}
	if err := tcpReconciler.SetupWithManager(manager); err != nil {
		return fmt.Errorf("set up TCPRoute controller: %w", err)
	}
	if err := udpReconciler.SetupWithManager(manager); err != nil {
		return fmt.Errorf("set up UDPRoute controller: %w", err)
	}
	if err := manager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("add health check: %w", err)
	}
	if err := manager.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("add readiness check: %w", err)
	}
	setupLog.Info(
		"Starting controller manager",
		"version", version,
		"controllerName", controllerConfig.ControllerName,
		"gatewayAPIChannel", controllerConfig.GatewayAPIChannel,
		"clusterID", controllerConfig.ClusterID,
		"octaviaProvider", "amphora",
		"memberMode", "NodePort",
	)
	if err := manager.Start(ctx); err != nil {
		return fmt.Errorf("run controller manager: %w", err)
	}
	return nil
}

func configureOpenStackRequestMetrics(config *openstack.ClientConfig, registerer prometheus.Registerer) error {
	requestMetrics, err := openstackobservability.RegisterRequestMetrics(registerer)
	if err != nil {
		return fmt.Errorf("register OpenStack request metrics: %w", err)
	}
	config.RequestObserver = requestMetrics
	return nil
}

func requireGatewayAPIs(restConfig *rest.Config, cfg controller.Config) error {
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("create Kubernetes discovery client: %w", err)
	}
	resourceList, err := discoveryClient.ServerResourcesForGroupVersion(gatewayv1.GroupVersion.String())
	if err != nil {
		return fmt.Errorf("required Gateway API v1 CRDs are unavailable; install the selected Gateway API %s bundle before starting the controller: %w", gatewayconsts.BundleVersion, err)
	}
	availableResources := make([]string, 0, len(resourceList.APIResources))
	for _, resource := range resourceList.APIResources {
		availableResources = append(availableResources, resource.Name)
	}
	requiredResources := []string{"gatewayclasses", "gateways", "httproutes", "tcproutes", "udproutes", "referencegrants"}
	if cfg.BackendTLSPolicy {
		requiredResources = append(requiredResources, "backendtlspolicies")
	}
	if cfg.TLSRoutesEnabled() {
		requiredResources = append(requiredResources, "tlsroutes")
	}
	for _, required := range requiredResources {
		if !slices.Contains(availableResources, required) {
			return fmt.Errorf("required Gateway API resource %s is missing from %s; install the selected Gateway API %s bundle", required, gatewayv1.GroupVersion, gatewayconsts.BundleVersion)
		}
	}
	if cfg.XBackendEnabled() {
		resources, err := discoveryClient.ServerResourcesForGroupVersion(gatewayx.GroupVersion.String())
		if err != nil {
			return fmt.Errorf("required Experimental XBackend API is unavailable; install the Gateway API %s Experimental bundle: %w", gatewayconsts.BundleVersion, err)
		}
		if !slices.ContainsFunc(resources.APIResources, func(resource metav1.APIResource) bool { return resource.Name == "xbackends" }) {
			return fmt.Errorf("required Experimental API resource xbackends is missing from %s; install the Gateway API %s Experimental bundle", gatewayx.GroupVersion, gatewayconsts.BundleVersion)
		}
	}
	return nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func durationEnvOr(name string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return duration, nil
}

func float64EnvOr(name string, fallback float64) (float64, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}

func intEnvOr(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}
