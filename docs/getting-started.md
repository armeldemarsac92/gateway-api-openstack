# Getting started with the current controller

The controller is pre-alpha.
The personal fork publishes a linux/amd64 development image. Qualification on a deployed Hibiscus environment remains open. [Image and build record](qualification/v1.7.0/image-build.md)
See the [compatibility record](providers/compatibility.md) for development evidence and the [roadmap](../ROADMAP.md) for remaining work.

Use these base manifests only in a disposable test project.
Keep cloud-provider-openstack responsible for Services of type `LoadBalancer`.
This controller creates separate resources only for its Gateway API objects.

For automated validation, use the [OpenStack E2E runner](testing-openstack-e2e.md), which creates a separate test installation in a dedicated or shared project.
The [conformance plan](testing-conformance.md) describes the separate Gateway API requirements and current gaps.

## Prerequisites

- A Kubernetes cluster with the Gateway API v1.7.0 Standard bundle.
- An OpenStack project with Octavia and Neutron access.
  The controller accepts only Amphora.
  Support claims are limited to environment profiles with published compatibility evidence.
- A Keystone application credential with only the permissions that the controller needs, stored in a named `clouds.yaml` entry.
- A VIP subnet and a member subnet from which Amphora can reach the selected Kubernetes Node addresses and allocated NodePorts.
- Security groups and routing that allow Amphora health checks and the listener's traffic protocol to those Node addresses and NodePorts.
- Optionally, an external network for allocating a Floating IP.
  Without one, Gateway status reports the Octavia VIP, which may be reachable only from private networks.
- The pinned linux/amd64 development image or an operator build of the selected source revision. [Build record](qualification/v1.7.0/image.json)

Each class selects network placement and Node addresses through an OpenStackGatewayClassConfig.
See the [configuration reference](reference/class-configuration.md) for the fields and [worker eligibility rules](reference/class-configuration.md#worker-nodeport-members).
These instructions use NodePort mode.
The experimental [direct endpoint mode](reference/class-configuration.md#direct-endpoint-members) has separate network prerequisites and awaits live validation.

The controller does not create networks, subnets, routers, or backend security groups and does not attach security groups to worker ports.
Establish that connectivity before creating a Gateway.

Install the complete Standard bundle if the cluster does not already have it. Use the intended workload cluster context for both commands.

```sh
gateway_api_manifest=https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.7.0/standard-install.yaml
kubectl apply --server-side -f "$gateway_api_manifest"
kubectl get -f "$gateway_api_manifest" -o 'custom-columns=KIND:.kind,NAME:.metadata.name,VERSION:.metadata.annotations.gateway\.networking\.k8s\.io/bundle-version,CHANNEL:.metadata.annotations.gateway\.networking\.k8s\.io/channel'
```

The Standard manifest contains ten CRDs in `gateway.networking.k8s.io` and the `safe-upgrades.gateway.networking.k8s.io` admission policy and binding. Each has the v1.7.0 and `standard` annotations. Check the manifest inventory and any other installed Gateway API CRDs before proceeding.
For Experimental qualification on a separate cluster, use `experimental-install.yaml` from the same release and set `GATEWAY_OPENSTACK_GATEWAY_API_CHANNEL` to `experimental` in the controller ConfigMap. That manifest has the same ten CRDs plus four in `gateway.networking.x-k8s.io`, all annotated `experimental`. The policy and binding carry `standard` in both manifests.

Startup requires GatewayClass, Gateway, HTTPRoute, TCPRoute, UDPRoute, and ReferenceGrant to be available. `UDPRoute` uses `gateway.networking.k8s.io/v1` in both bundles.
The controller reports bundle version and channel validation through the GatewayClass `SupportedVersion` condition.
A mismatch blocks provisioning and repair. Finalization can continue while the required Kubernetes APIs are served. [Installed Gateway API version](design/architecture.md#installed-gateway-api-version)

## Prepare OpenStack credentials

Create a local `clouds.yaml` containing an application credential.
Do not add this file to the repository.

```yaml
clouds:
  openstack:
    auth:
      auth_url: https://identity.example/v3
      application_credential_id: replace-me
      application_credential_secret: replace-me
    auth_type: v3applicationcredential
    region_name: RegionOne
    interface: public
    identity_api_version: 3
```

Create the controller namespace, then store the file as a Secret.
The Secret name and key match the Deployment volume in `config/manager/deployment.yaml`.

```sh
kubectl apply -f config/default/namespace.yaml
kubectl -n openstack-gateway-system create secret generic openstack-clouds \
  --from-file=clouds.yaml=/absolute/path/to/clouds.yaml
```

The Secret is mounted read-only and credentials are not copied into ConfigMaps, Gateway API status, or examples.

## Configure and install

Edit `config/manager/controller-config.yaml` before applying it:

- Keep the default controller name, or choose an installation identity before creating managed resources.
- Set the cluster ID to a value that is unique to this cluster, and do not change it while managed resources exist.
- Set `OS_CLOUD` to the entry in the mounted `clouds.yaml`.
- Set `OS_REGION_NAME` only when it should override `region_name` in that file.

The default controller name is `gateway-api-openstack.github.io/gateway-api-openstack`.
The same value must appear in every managed `GatewayClass.spec.controllerName`, including `examples/basic.yaml`.
Before changing an existing installation's identity, follow the [compatibility guidance](reference/class-configuration.md#development-compatibility).

Configure VIP and member placement in the OpenStackGatewayClassConfig in `examples/basic.yaml`.
Replace its synthetic subnet IDs with existing resources in the controller project.
The [class configuration reference](reference/class-configuration.md#fields-and-defaults) covers Floating IP selection, health settings, and optional placement.

To request a particular private IPv4 address, use a GatewayClass with `floatingIP.mode: None` and set one `IPAddress` value in `spec.addresses`. An empty value requests automatic allocation. Before using the VIP, check `Programmed=True` for the current Gateway generation and read `status.addresses`. See the [fixed private IPv4 instructions](qualification/v1.7.0/provider/private-gateway-address.md).

Set the image override in your deployment overlay. The checked-in `config/default/kustomization.yaml` starts with an `example.invalid` placeholder. For the published linux/amd64 image, use the `repository` from the build record as `newName` and the `manifestDigest` as `digest`, and remove the placeholder `newTag`. [Current digest](qualification/v1.7.0/image.json)

For an operator build, build the selected revision and push the resulting image to your registry.

```sh
make container-build \
  IMAGE=registry.example/openstack-gateway-controller:v0.3.0-dev \
  VERSION=v0.3.0-dev
docker push registry.example/openstack-gateway-controller:v0.3.0-dev
```

Set `CONTAINER_TOOL=podman` on the `make` command when Podman is preferred.

The base installation includes the cluster-scoped OpenStackGatewayClassConfig CRD.
Inspect its effect on any other installation using this API before applying or removing the base manifests.
The controller also needs Namespace `get`, `list`, and `watch` access and ReferenceGrant `list` and `watch` access for backend input collection and reference events.
Its startup and bundle checks require the ReferenceGrant API from the selected v1.7.0 bundle.
Render and inspect the resources, then install them:

```sh
kubectl kustomize config/default
kubectl apply -k config/default
kubectl -n openstack-gateway-system rollout status \
  deployment/openstack-gateway-controller
```

The Deployment starts two replicas with leader election.
It exposes health and readiness probes on port 8081.
Port 8080 exposes controller-runtime metrics and bounded OpenStack request, duration, mutation attempt, and in-flight metrics inside each Pod.
These OpenStack metrics are process-local and reset when the controller process restarts.
The base manifests do not expose metrics through a Service.

## Deployment settings

The ConfigMap contains process identity and operational settings, with no secrets.
Infrastructure choices belong to the class API.
The ConfigMap uses the environment variables accepted by the controller binary.

| Setting | Environment variable | Current behavior |
| --- | --- | --- |
| Controller identity | `GATEWAY_OPENSTACK_CONTROLLER_NAME` | Defaults to `gateway-api-openstack.github.io/gateway-api-openstack`. Must exactly match GatewayClass. |
| Cluster identity | `GATEWAY_OPENSTACK_CLUSTER_ID` | Required and stable. |
| Gateway API channel | `GATEWAY_OPENSTACK_GATEWAY_API_CHANNEL` | Defaults to `standard`. Use `experimental` with the complete Experimental bundle. |
| OpenStack resync | `GATEWAY_OPENSTACK_RESYNC_INTERVAL` | Base interval for observing converged resources again. Defaults to `10m` |
| OpenStack request rate | `GATEWAY_OPENSTACK_API_QPS` | Average request limit shared by Keystone, Octavia, and Neutron. Defaults to `10` per second |
| OpenStack request burst | `GATEWAY_OPENSTACK_API_BURST` | Short request burst allowed for the process. Defaults to `20` |
| Cloud name | `OS_CLOUD` | Named entry in `clouds.yaml` |
| Cloud file | `OS_CLIENT_CONFIG_FILE` | Mounted `/etc/openstack/clouds.yaml` |
| Region | `OS_REGION_NAME` | Optional override |

At the default resync interval, observations occur about 8 to 12 minutes apart.
Stable jitter based on the resource UID spreads the OpenStack requests.
Set the environment variable or use `--openstack-resync-interval` to change the base interval.
The command line flag takes precedence, and the value must be greater than zero.

Every OpenStack service client in the process shares one request budget.
This also covers authentication and token renewal, so increasing controller worker counts cannot bypass the limit by using another service client.
Both request settings must be greater than zero.
The equivalent command line flags are `--openstack-api-qps` and `--openstack-api-burst`.

The base Deployment also sets these binary flags: leader election enabled, metrics on `:8080`, probes on `:8081`, and Octavia microversion `2.5`.
Optional flavor, availability zone placement, and referenced frontend security groups require higher microversions and additional profile reads, as described in the [configuration reference](reference/class-configuration.md#validation-and-permissions).
The binary additionally supports `--openstack-operation-timeout` and `--openstack-poll-interval`.
The manifests do not override these two flags, so the binary uses defaults of 10 minutes and 2 seconds.
The timeout limits each provider reconciliation call, including all OpenStack requests made by that call.
After starting an asynchronous Octavia change, the controller returns and observes it again after the poll interval instead of waiting for it to finish in the same reconciliation.

## Inspect network prerequisites

Use the read-only diagnostic tool after installing an OpenStackGatewayClassConfig and before creating test Gateways:

```sh
make build-diagnostics
bin/openstack-gateway-diagnostics \
  --class-config=openstack \
  --kubeconfig=<path-to-kubeconfig> \
  --context=<context-name> \
  --clouds-yaml=<path-to-clouds.yaml> \
  --openstack-cloud=<cloud-name> \
  --openstack-region=<region> \
  --octavia-microversion=<controller-microversion> \
  > environment-diagnostics.json
```

Use the same credential scope, region, and microversion as the controller.
The Kubernetes identity needs get access to the selected OpenStackGatewayClassConfig and, for NodePort mode, list access to Nodes.
`--node-selector` can narrow the worker selection.
`--check-barbican` adds a metadata access check without reading secret payloads.
The default deadline is two minutes, with the same 10 QPS and burst 20 defaults as the controller.

The command inspects the current class template, not an existing Gateway's frozen binding.
It checks label-eligible workers, including unready Nodes, without claiming the member eligibility of a particular Service.
Endpoint mode skips Node and worker port checks and leaves endpoint reachability unknown.
Results distinguish `passed`, `failed`, `unknown`, and `skipped` checks.
Update permissions, actual backend reachability, Amphora topology, and the kube-proxy implementation remain unknown because these reads cannot establish them.
Observed IP families describe selected Node addresses, not verified IPv6 or dual-stack support.

The JSON report omits resource names, cloud IDs, addresses, credentials, and raw API errors.
Exit status 0 means no observed failure, 2 means at least one failed check, and 1 means invalid command options or an output error.
Unknown checks still require evidence even when the command exits successfully.
This report does not replace traffic tests or the [ownership audit](operator-recovery.md#run-the-ownership-audit).

## Apply the basic example

Confirm the controller name and class subnet references in `examples/basic.yaml`, then apply it:

```sh
kubectl apply -f examples/basic.yaml
kubectl get openstackgatewayclassconfig openstack -o yaml
kubectl get gatewayclass openstack -o yaml
kubectl -n gateway-api-openstack-demo get gateway edge -o yaml
kubectl -n gateway-api-openstack-demo get httproute basic -o yaml
```

An HTTPRoute can list the managed Gateway alongside a missing Gateway or a Gateway owned by another controller. The managed entry may appear anywhere in `spec.parentRefs`. Look for its exact reference and this controller's name in `status.parents`. Two references handled by this controller, including two sections of the same Gateway, currently give `Accepted=False/UnsupportedValue`. [Attachment and status behavior](qualification/v1.7.0/provider/http-one-managed-parent.md)

The example intentionally uses one Gateway, one HTTP listener, one HTTPRoute, one rule, one PathPrefix match, and one NodePort Service backend.

For a shared Gateway, the platform operator can give its HTTP listener a Namespace selector.

```yaml
allowedRoutes:
  namespaces:
    from: Selector
    selector:
      matchLabels:
        shared-edge: "enabled"
```

Give each participating Namespace the label `shared-edge: "enabled"`. In a tenant HTTPRoute, set the parent reference's `name` and `namespace` to the shared Gateway. An omitted backend namespace selects the Service in the route's own namespace. Removing the Namespace label withdraws that tenant's routes. Check the route's `Accepted` reason and the Gateway listener's `attachedRoutes` count. [Shared listener evidence](qualification/v1.7.0/README.md)
First check that GatewayClass reports `Accepted=True`.
This verifies the class inputs and read-only cloud checks, not network connectivity or provisioning permissions.
Then wait until the Gateway's `Accepted` and `Programmed` conditions are `True`.
In the HTTPRoute parent status, wait for `Accepted`, `ResolvedRefs`, and the `<controller-domain>/Programmed` condition defined by this controller to become `True`.
The address is published at `Gateway.status.addresses[0].value`.

```sh
kubectl -n gateway-api-openstack-demo get gateway edge \
  -o jsonpath='{.status.addresses[0].value}{"\n"}'
```

Send an HTTP request to that address from a network that can reach it.
A VIP without a Floating IP may not be reachable from the public Internet.

See the [supported routing scope](../README.md#current-implementation) and [class configuration lifecycle](reference/class-configuration.md#configuration-after-gateway-creation) before extending this example.

### Serve HTTP on two ports of one Gateway

Use only plain HTTP listeners on a Gateway with several HTTP sections. Give each listener a distinct name. Ensure the frontend network and any referenced security groups admit both ports. The controller manages no rules in referenced groups.

Add these listeners under `Gateway.spec`. The same host can use both ports on the same Gateway address.

```yaml
listeners:
  - name: http
    protocol: HTTP
    port: 80
    hostname: api.example.com
  - name: http-alt
    protocol: HTTP
    port: 8080
    hostname: api.example.com
```

Set one managed parent reference on each HTTPRoute. This reference selects the second port. Create a separate route for the first port with `sectionName: http` and `port: 80`. Give each rule one backend Service reference. The backends can differ while the hostname and path stay the same.

```yaml
parentRefs:
  - name: edge
    sectionName: http-alt
    port: 8080
```

Either selector can be used alone. When both are present, they must match the same listener. The selectors, hostname intersection and namespace authorization must leave exactly one eligible section. A listener defaults to routes in the Gateway's namespace. For cross-namespace routes, set the parent reference's `namespace` and authorize the route namespace on that listener.

Check the conditions and `attachedRoutes` for both named listeners at the current generation. Confirm each route's managed parent reports `Accepted`, `ResolvedRefs` and `<controller-domain>/Programmed` as true. Set `VIP` to the observed Gateway address. From a client with access to it, send the same host and path to both ports and verify each backend's response.

```sh
curl -fsS --resolve "api.example.com:80:$VIP" "http://api.example.com:80/"
curl -fsS --resolve "api.example.com:8080:$VIP" "http://api.example.com:8080/"
```

To share a port between `bar.com` and `foo.com`, declare two sections on that port with those distinct exact hostnames. A port with just one section can use a wildcard or omit its hostname. [Same-port sections](qualification/v1.7.0/provider/http-disjoint-sections.md)

Complete the controller rollout before adding the second port. If you need to revert to a version from before distinct HTTP ports, clean up enrolled Gateways with the current controller first. [Local traffic and status results](qualification/v1.7.0/provider/http-distinct-ports.md) · [Rollback procedure](operator-recovery.md#roll-back-a-gateway-with-distinct-http-ports)

For a WebSocket backend, set `appProtocol: kubernetes.io/ws` on the TCP Service port selected by the HTTPRoute backend reference. Point your WebSocket client at `ws://<gateway-address>/<route-path>`. The HTTP listener forwards the upgrade to the application, which must accept the handshake and handle the subsequent frames. [WebSocket qualification](qualification/v1.7.0/provider/http-websocket.md)

## Serve HTTPS with an operator-provisioned certificate

A tenant can attach an HTTPRoute to a named HTTPS listener when the operator enables the certificate map for that class and approves the exact TLS Secret revision. Use a separate port for each HTTPS listener. A cross-namespace Secret also needs a ReferenceGrant from the certificate owner. [Workload overlay, example Gateway and certificate lifecycle](operator-https.md)

## Forward TCP to a Service

For TCP applications, create a dedicated TCP Gateway. The example uses one listener and the existing `openstack` class, the `gateway-api-openstack-demo` namespace and a Service called `database` exposing port 5432. With the default class member mode, the Service must have an allocated NodePort and ready endpoints on reachable workers. In Endpoint mode the Amphora must be able to reach the pod addresses and ports.

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: database-edge
  namespace: gateway-api-openstack-demo
spec:
  gatewayClassName: openstack
  listeners:
    - name: postgres
      port: 5432
      protocol: TCP
---
apiVersion: gateway.networking.k8s.io/v1
kind: TCPRoute
metadata:
  name: database
  namespace: gateway-api-openstack-demo
spec:
  parentRefs:
    - name: database-edge
      sectionName: postgres
  rules:
    - backendRefs:
        - name: database
          port: 5432
```

Use the Gateway address and listener port with the application's own TCP client. For PostgreSQL, a query such as `select 1` verifies a complete request and response. A successful TCP handshake alone can also occur on an empty listener.

When several routes attach to this listener, `attachedRoutes` counts them all. The oldest route wins. For equal creation timestamps the controller orders routes by namespace and then name. Check each parent's `Accepted`, `ResolvedRefs` and `<controller-domain>/Programmed` conditions. A younger route reports `NotSelected`. A missing or unauthorized positive-weight backend on the oldest route leaves forwarding withdrawn. To allow a Service in a different namespace, create a ReferenceGrant in that namespace with `from.kind: TCPRoute`. The listener's `allowedRoutes` separately controls which route namespaces may attach. [Controller and provider checks](qualification/v1.7.0/provider/stock-tcp.md)

For several TCP Services, select a GatewayClass with `memberMode: Endpoint` and put one `backendRef` per Service port in the same rule. Two references with `weight: 70` and `weight: 30` target those shares of new TCP connections. The controller divides each share across that Service's ready endpoints. A weight of zero adds no members. An invalid zero-weight reference still reports `ResolvedRefs=False`.

The controller withdraws forwarding if any positive-weight Service has no eligible endpoints. It reports `Programmed=False/UnsupportedValue` if the exact ratio requires an Octavia member weight above 256 or the pool would exceed HAProxy's limit of 4,095 active endpoints. Weighted NodePort requests also report `UnsupportedValue`. A weight change detaches the pool until Octavia confirms the new settings. Each active weighted member must report `backup: false` before the controller reattaches the pool. [Measured connection counts](qualification/v1.7.0/provider/tcp-upstream.md#weighted-tcp-connections) [Endpoint mode configuration](reference/class-configuration.md#direct-endpoint-members) · [HAProxy round robin limit](https://github.com/haproxy/haproxy/blob/v3.0.19/doc/configuration.txt#L5631-L5637)

To expose more than one TCP port on this Gateway, configure every listener as TCP and give each a distinct name and port. Use one Gateway parent and one rule per TCPRoute. A Gateway-wide parent attaches to every eligible TCP listener. A parent with `sectionName`, `port` or both selects the exact matching listener. Each listener elects the oldest eligible route, breaking equal creation timestamps by namespace and name. A port with an unusable winning backend stays without forwarding until that route recovers or loses the listener election. A route that wins on multiple ports shares its pool across those ports. Verify that your VIP security group allows each port. Complete the controller rollout before adding the second port to an existing Gateway. After updating the Gateway or a route, check `Programmed=True` for the current generation on the Gateway and its selected listeners and check the route’s `<controller-domain>/Programmed` parent condition for the current route generation. Test a fresh connection to each port. [Enrollment and rollback](operator-recovery.md#multiport-tcp-upgrades)

## Forward UDP to a Service

Create a dedicated Gateway with one UDP listener. This example assumes DNS pods labeled `app: example-dns` in `gateway-api-openstack-demo` that serve UDP on port 53. It uses the existing `openstack` GatewayClass in its default NodePort mode. Amphora needs to reach the allocated UDP NodePort on eligible workers, and the Service needs ready endpoints.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: example-dns
  namespace: gateway-api-openstack-demo
spec:
  type: NodePort
  selector:
    app: example-dns
  ports:
    - name: dns
      protocol: UDP
      port: 53
      targetPort: 53
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: dns-edge
  namespace: gateway-api-openstack-demo
spec:
  gatewayClassName: openstack
  listeners:
    - name: dns
      protocol: UDP
      port: 53
---
apiVersion: gateway.networking.k8s.io/v1
kind: UDPRoute
metadata:
  name: example-dns
  namespace: gateway-api-openstack-demo
spec:
  parentRefs:
    - name: dns-edge
      sectionName: dns
  rules:
    - backendRefs:
        - name: example-dns
          port: 53
```

The backend reference uses the Service's UDP port. The controller uses the allocated UDP NodePort for the Octavia members. Check the Gateway address and the route's `Accepted`, `ResolvedRefs` and `<controller-domain>/Programmed` conditions for its parent. Then query a record served by the backend with `dig @<gateway-address> -p 53 <record-name> +notcp +ignore +tries=1 +time=2` and confirm the expected reply.

A UDPRoute uses one Gateway parent and one rule. In NodePort mode the rule selects a single core Service. Each listener counts its attached UDPRoutes in `attachedRoutes` and selects the oldest eligible route, with ties broken by namespace and name. A younger route takes over when the older route detaches. If the selected backend is missing, unauthorized or exposes only TCP on the requested port, the listener's forwarding is withdrawn and the older route keeps precedence. Cross-namespace Service references require a grant in the Service namespace with `from.kind: UDPRoute`. The listener's `allowedRoutes` separately governs route attachment.

To serve this Service on a second UDP port at the same Gateway address, add a listener with a different name and port and omit `sectionName` from the route’s `parentRefs` entry. The route then attaches to each UDP listener that allows its kind and namespace. Set `port`, `sectionName`, or both on the parent to select a specific listener. When both are present, they must match the same listener. A route selected on several listeners uses one backend pool. Removing a listener keeps the pool when a sibling still uses it. Any custom VIP security group must permit every desired UDP port. [Rollback of an enrolled Gateway](operator-recovery.md)

For multiple UDP Services, choose a GatewayClass with `memberMode: Endpoint` and operator-managed routing from Amphora to the pod IPs. In the rule, give each Service its own `backendRef` with its Service port and relative weight. For example, weights of 70, 30 and 0 send seven parts to the first Service, three to the second and none to the third. The controller divides each Service's share across its ready endpoints, including when the Services have different replica counts. [GatewayClass configuration](reference/class-configuration.md)

The controller requires an exact ratio within Octavia's integer member-weight range of 1–256. When a positive share has no usable destination, the controller withdraws forwarding for the whole route. A denied zero-weight reference still reports `ResolvedRefs=False`. The permitted positive destinations can continue serving. Selective packet loss for an invalid share and weighted NodePort backends remain open. [Qualification record](qualification/v1.7.0/README.md)

## Route several paths to Services

One HTTPRoute can serve several applications on the same HTTP or operator-managed HTTPS listener. The following rules send `/account` and `/me` to the account Service. Other requests below `/catalog` reach the catalog Service. Use the Service port in each backend reference.

```yaml
rules:
  - matches:
      - path: {type: Exact, value: /account}
      - path: {type: Exact, value: /me}
    backendRefs:
      - name: account
        port: 8080
  - matches:
      - path: {type: PathPrefix, value: /catalog}
    backendRefs:
      - name: catalog
        port: 8080
```

For the same hostname, Exact has priority over PathPrefix. Among prefixes, the longest matching path wins. A prefix matches whole path segments. The relative order of rules in the route breaks a tie after the listener has compared paths and route ownership. The controller uses one pool for each distinct Service port in this route. A prefix made of ordinary ASCII path segments can use one native Octavia policy. Other element-wise prefixes can use two policies.

If a rule references a Service in another namespace, create a ReferenceGrant in the Service namespace. Check the route parent's `ResolvedRefs` condition after changing a grant. On stock Amphora, the controller keeps the affected path attached to its pool and disables previously serving members. Once Octavia applies the change, requests to that path return HTTP 503. Other authorized paths keep serving. Gateway API requires HTTP 500 for an invalid backend. [Provider response contract](qualification/v1.7.0/provider/octavia-http-local-responses.md)

Keep the current image running when migrating a multi-rule route back to the single-rule controller. [Rollback procedure](operator-recovery.md#roll-back-httproutes-with-several-paths)

## Safe removal

Keep the controller, its application credential, and OpenStack access running until every managed HTTPRoute, TCPRoute, UDPRoute and Gateway has finished finalization.
Removing the Deployment or Secret first can leave OpenStack resources behind and leave Kubernetes objects stuck in deletion.

For the basic example, delete in this order:

```sh
kubectl -n gateway-api-openstack-demo delete httproute basic
kubectl -n gateway-api-openstack-demo delete gateway edge
kubectl delete gatewayclass openstack
kubectl delete openstackgatewayclassconfig openstack
kubectl delete -f examples/basic.yaml --ignore-not-found
```

Each of the first three commands waits for deletion by default.
If one remains blocked, keep the installation available and follow the [operator recovery guide](operator-recovery.md).
Do not remove controller finalizers manually because doing so bypasses ownership checks and cleanup.

Before uninstalling a shared controller, delete managed HTTPRoutes, TCPRoutes and UDPRoutes before their Gateways, then repeat the GatewayClass deletion for every class using its exact controller name.
Remove a parameter object only after no class needs it.

The base uninstall also deletes the OpenStackGatewayClassConfig CRD, which removes every instance of that API throughout the cluster.
Check for remaining configuration objects:

```sh
kubectl get openstackgatewayclassconfigs.gateway-api-openstack.github.io
```

Only after the list is empty and no other installation needs the CRD, uninstall the base manifests:

```sh
kubectl delete -k config/default
```

If another installation uses this API, remove only the resources owned by this installation and retain the shared CRD and any shared RBAC resources.
The base uninstall removes the dedicated `openstack-gateway-system` namespace and its credential Secret.
It does not remove the upstream Gateway API CRDs.
