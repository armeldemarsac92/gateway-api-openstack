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

package ambientmesh

import (
	"context"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"
	"sigs.k8s.io/gateway-api/pkg/features"
)

const (
	ControllerName gatewayv1.GatewayController = "gateway-api-openstack.github.io/istio-ambient"
	DefaultName                                = "hibiscus-istio-ambient"

	istioObservationInterval = 10 * time.Second
)

type reconciler struct {
	client.Client
	apiReader client.Reader
	instance  string
}

// SetupWithManager registers the opt-in status controller and ensures the
// installation has its default XMesh. Both operations require Experimental.
func SetupWithManager(ctx context.Context, manager ctrl.Manager, name, instance string) error {
	r := &reconciler{Client: manager.GetClient(), apiReader: manager.GetAPIReader(), instance: instance}
	if err := ctrl.NewControllerManagedBy(manager).
		Named("istio-ambient-xmesh").
		For(&gatewayx.XMesh{}, builder.WithPredicates(
			predicate.NewPredicateFuncs(ownsMesh), predicate.GenerationChangedPredicate{},
		)).
		Watches(&gatewayv1.GatewayClass{}, handler.EnqueueRequestsFromMapFunc(r.meshesForClass),
			builder.WithPredicates(predicate.NewPredicateFuncs(func(object client.Object) bool {
				return object.GetName() == waypointClassName
			}))).
		Complete(r); err != nil {
		return err
	}
	return r.ensureDefault(ctx, name)
}

func ownsMesh(object client.Object) bool {
	return object.(*gatewayx.XMesh).Spec.ControllerName == ControllerName
}

func (r *reconciler) ensureDefault(ctx context.Context, name string) error {
	var mesh gatewayx.XMesh
	key := client.ObjectKey{Name: name}
	if err := r.apiReader.Get(ctx, key, &mesh); !apierrors.IsNotFound(err) {
		if err == nil && !ownsMesh(&mesh) {
			log.FromContext(ctx).Info("Default XMesh belongs to a different controller", "name", name, "controllerName", mesh.Spec.ControllerName)
		}
		return err
	}
	return client.IgnoreAlreadyExists(r.Create(ctx, &gatewayx.XMesh{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       gatewayx.MeshSpec{ControllerName: ControllerName},
	}))
}

func (r *reconciler) meshesForClass(ctx context.Context, _ client.Object) []reconcile.Request {
	var meshes gatewayx.XMeshList
	if err := r.List(ctx, &meshes); err != nil {
		log.FromContext(ctx).Error(err, "Could not list XMeshes after an Istio GatewayClass change")
		return nil
	}
	var requests []reconcile.Request
	for i := range meshes.Items {
		mesh := &meshes.Items[i]
		if ownsMesh(mesh) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(mesh)})
		}
	}
	return requests
}

func (r *reconciler) Reconcile(ctx context.Context, request reconcile.Request) (ctrl.Result, error) {
	var mesh gatewayx.XMesh
	if err := r.Get(ctx, request.NamespacedName, &mesh); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !ownsMesh(&mesh) || !mesh.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	previous := mesh.DeepCopy()
	accepted := metav1.Condition{
		Type:               string(gatewayx.MeshConditionAccepted),
		Status:             metav1.ConditionTrue,
		Reason:             string(gatewayx.MeshReasonAccepted),
		Message:            "XMesh accepted by " + r.instance,
		ObservedGeneration: mesh.Generation,
	}
	programmed := metav1.Condition{
		Type:               string(gatewayv1.GatewayConditionProgrammed),
		Status:             metav1.ConditionUnknown,
		Reason:             string(gatewayv1.GatewayReasonPending),
		ObservedGeneration: mesh.Generation,
	}
	mesh.Status.SupportedFeatures = nil
	var observationErr error
	if mesh.Spec.ParametersRef != nil {
		accepted.Status = metav1.ConditionFalse
		accepted.Reason = string(gatewayx.MeshReasonInvalidParameters)
		accepted.Message = r.instance + " does not support parametersRef"
		programmed.Status = metav1.ConditionFalse
		programmed.Reason = accepted.Reason
		programmed.Message = accepted.Message
	} else {
		var ready bool
		var detail string
		ready, detail, observationErr = r.observeIstio(ctx)
		programmed.Message = r.instance + " is " + detail
		if ready {
			programmed.Status = metav1.ConditionTrue
			programmed.Reason = string(gatewayv1.GatewayReasonProgrammed)
			mesh.Status.SupportedFeatures = supportedFeatures()
		}
	}

	acceptedChanged := meta.SetStatusCondition(&mesh.Status.Conditions, accepted)
	programmedChanged := meta.SetStatusCondition(&mesh.Status.Conditions, programmed)
	if acceptedChanged || programmedChanged || !slices.Equal(mesh.Status.SupportedFeatures, previous.Status.SupportedFeatures) {
		patch := client.MergeFromWithOptions(previous, client.MergeFromWithOptimisticLock{})
		if err := r.Status().Patch(ctx, &mesh, patch); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}
	if mesh.Spec.ParametersRef != nil || observationErr != nil {
		return ctrl.Result{}, observationErr
	}
	// Recheck the named Istio workload dependencies without watching all
	// Deployments, DaemonSets and ConfigMaps in the cluster.
	return ctrl.Result{RequeueAfter: istioObservationInterval}, nil
}

func supportedFeatures() []gatewayv1.SupportedFeature {
	// Gateway API v1.7.0 mesh cases that passed with the pinned stock Istio.
	// See docs/qualification/v1.7.0/provider/ambient-waypoint-extended-results.json.
	names := []features.FeatureName{
		features.SupportGRPCRoute,
		features.SupportHTTPRoute,
		features.SupportHTTPRoute303RedirectStatusCode,
		features.SupportHTTPRoute307RedirectStatusCode,
		features.SupportHTTPRoute308RedirectStatusCode,
		features.SupportHTTPRouteParentRefPort,
		features.SupportHTTPRouteResponseHeaderModification,
		features.SupportMesh,
		features.SupportMeshClusterIPMatching,
		features.SupportMeshHTTPRouteBackendRequestHeaderModification,
		features.SupportMeshHTTPRouteNamedRouteRule,
		features.SupportMeshHTTPRouteQueryParamMatching,
		features.SupportMeshHTTPRouteRedirectPath,
		features.SupportMeshHTTPRouteRedirectPort,
		features.SupportMeshHTTPRouteRewritePath,
		features.SupportMeshHTTPRouteSchemeRedirect,
	}
	slices.Sort(names)
	result := make([]gatewayv1.SupportedFeature, 0, len(names))
	for _, name := range names {
		result = append(result, gatewayv1.SupportedFeature{Name: gatewayv1.FeatureName(name)})
	}
	return result
}

// Instance describes the installation that has claimed an XMesh.
func Instance(version, namespace string) string {
	base := "Hibiscus Istio ambient " + version
	if namespace != "" {
		return base + " in namespace " + namespace
	}
	return base
}
