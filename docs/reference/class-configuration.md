# GatewayClass configuration

OpenStackGatewayClassConfig supplies the infrastructure settings for new Gateways.
It is a cluster-scoped `v1alpha1` API in the group `gateway-api-openstack.github.io`.
GatewayClass must reference it through `spec.parametersRef`, with no namespace.
The API group and default controller identity use the organization's domain under [ADR 0003](../design/adr/0003-project-organization.md).
[ADR 0002](../design/adr/0002-class-configuration.md) defines class behavior and compatibility limits.
[ADR 0004](../design/adr/0004-network-access-and-recovery.md) adds referenced security and address recovery.

## Configure a class

The following example uses synthetic subnet IDs.
Replace them with existing subnets in the controller's authenticated OpenStack project.

```yaml
apiVersion: gateway-api-openstack.github.io/v1alpha1
kind: OpenStackGatewayClassConfig
metadata:
  name: openstack
spec:
  vipSubnet:
    id: 11111111-1111-4111-8111-111111111111
  memberSubnet:
    id: 22222222-2222-4222-8222-222222222222
  floatingIP:
    mode: None
  nodeAddressType: InternalIP
  healthMonitor:
    path: /
---
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: openstack
spec:
  controllerName: gateway-api-openstack.github.io/gateway-api-openstack
  parametersRef:
    group: gateway-api-openstack.github.io
    kind: OpenStackGatewayClassConfig
    name: openstack
```

The controller name must exactly match the installation's `--controller-name` setting.
Credentials, project scope, cluster identity, region, and request limits remain [deployment settings](../getting-started.md#deployment-settings).
A parameter object cannot select different credentials or a different project.
Amphora is the only provider, and NodePort is the default member mode.
There are no deployment topology flags or fallback settings for a GatewayClass without `parametersRef`.
Gateway `spec.infrastructure.parametersRef` is not supported.

## Fields and defaults

| Field | Default | Behavior |
| --- | --- | --- |
| `vipSubnet` | Required | Selects the existing subnet for the load balancer VIP. |
| `memberSubnet` | Required | Selects the existing subnet supplied to Octavia members. |
| `memberMode` | `NodePort` | Selects worker NodePorts or the experimental `Endpoint` mode described below. |
| `floatingIP.mode` | `None` | `None` publishes the VIP. `Allocate` creates a Floating IP owned by the Gateway. |
| `floatingIP.externalNetwork` | Unset | Required for `Allocate` and forbidden for `None`. Selects an existing external network. |
| `frontendSecurity.mode` | `OctaviaManaged` | `Referenced` uses existing VIP security groups after capability and rule checks. |
| `frontendSecurity.groups` | Unset | Required for `Referenced`. Supply 1 through 16 distinct group references. |
| `backendSecurity.mode` | `Unmanaged` | `Referenced` validates groups already attached to selected worker ports in NodePort mode. Endpoint mode requires `Unmanaged`. |
| `backendSecurity.groups` | Unset | Required for `Referenced`. Supply 1 through 16 distinct group references. |
| `nodeAddressType` | `InternalIP` | Selects `InternalIP` or `ExternalIP` from eligible Nodes in NodePort mode. Endpoint mode does not use it. |
| `flavorID` | Unset | Selects an enabled Amphora flavor by UUID. |
| `availabilityZone` | Unset | Selects an enabled Amphora availability zone by exact name. |
| `healthMonitor.path` | `/` | HTTP path starting with `/`, with no control characters and at most 255 characters. |
| `healthMonitor.delay` | `10` | Interval in seconds, from 1 through 3600. |
| `healthMonitor.timeout` | `5` | Timeout in seconds, from 1 through 3600 and less than `delay`. |
| `healthMonitor.maxRetries` | `3` | Successful checks required for recovery, from 1 through 10. |
| `healthMonitor.maxRetriesDown` | `3` | Failed checks required to mark a member down, from 1 through 10. |

HTTP listeners use an HTTP monitor by default. It sends `GET` to `healthMonitor.path` and accepts status codes `200-399`. TCP and UDP listeners use Octavia's `TCP` and `UDP-CONNECT` monitors respectively. Octavia records the configured delay, timeout and both retry counts. For UDP, Hibiscus enforces an operator-configurable minimum delay that defaults to 3 seconds. The stock Amphora LVS template renders the failure threshold as `retry` and omits the separate recovery threshold. The HTTP path applies only to HTTP monitors. [Stock configuration and packet check](../qualification/v1.7.0/provider/stock-udp.md)
Class configuration always supplies a monitor configuration. The controller omits the monitor for UDP pools enrolled in the [proportional packet discard extension](../operator-octavia-udp-packet-drop.md). The controller allocates new Floating IPs when requested.

Each subnet, external network, or security group reference must contain exactly one selector:

| Selector | Matching rule |
| --- | --- |
| `id` | One UUID. |
| `name` | An exact name of 1 through 255 characters, with no surrounding whitespace or control characters. |
| `tags` | All listed tags must match. Supply 1 through 16 unique tags, each 1 through 60 characters, with no commas, surrounding whitespace, or control characters. |

For example, a subnet reference can use `name: gateway-vip` or `tags: [gateway-vip, development]` instead of `id`.
Names and tags must resolve to exactly one resource across all returned pages.
Zero matches, multiple matches, or a response that contradicts the selector rejects the configuration.

Both subnets must belong to the authenticated project and use the same IP version.
Their parent networks must be administratively enabled and list the selected subnets.
An external network must be visible to the credential, administratively enabled, and marked external.
It may belong to the cloud operator's project, as shared public networks often do.
Floating IP allocation currently requires an IPv4 VIP subnet.
Reading these resources does not give the controller ownership of them.

## Security groups

The default frontend mode leaves listener access to Octavia.
To supply existing groups, add these fields to the class configuration:

```yaml
frontendSecurity:
  mode: Referenced
  groups:
    - id: 33333333-3333-4333-8333-333333333333
backendSecurity:
  mode: Referenced
  groups:
    - id: 44444444-4444-4444-8444-444444444444
```

These synthetic IDs identify groups in the authenticated project.
Names and tags use the same selector rules as subnets.
The controller rejects references that resolve to the same group more than once.
Groups are forbidden with `OctaviaManaged` or `Unmanaged`.
There is no `Managed` backend mode.

Referenced frontend groups require configured and advertised Octavia microversion `2.29` or later.
Octavia applies `vip_sg_ids` to VIP and Amphora VRRP ports while retaining internal VRRP and HAProxy peer rules.
The operator supplies listener ingress rules.
Custom groups cannot be combined with listener `allowed_cidrs` or an SR-IOV flavor.
When a flavor is selected, its readable profile must establish that SR-IOV is disabled.
These restrictions follow the [Octavia custom VIP security group contract](https://docs.openstack.org/octavia/latest/contributor/specs/version15.0/custom-security-groups-for-VIP-ports.html).

Referenced backend groups must already be attached to each selected worker port with port security enabled.
The controller maps the Node's OpenStack provider ID and configured region to a Nova server UUID, then verifies exactly one compute port in the authenticated project with the selected address and member subnet.
Conflicting Node identities for the same address are rejected.
In `Unmanaged` mode, the controller does not impose these port security checks.

Ingress validation checks the IP family, the listener or NodePort range, and the transport protocol. HTTP and TCP listeners require TCP ingress. UDP listeners require UDP ingress on the VIP and UDP NodePort.
It does not prove that a remote group or source CIDR includes Amphora or the client.
The controller never derives backend source CIDRs from the VIP subnet.
Health checks and actual traffic must establish reachability.

Resolved group IDs are fixed in the Gateway binding.
The controller reads their current rules and worker associations before provisioning and during drift observation.
A failed external prerequisite produces a redacted condition and a slow retry, allowing an operator's repair to restore progress.
Deletion does not require referenced groups or workers to remain available.

## Validation and permissions

GatewayClass validation uses read-only Octavia and Neutron calls.
It checks the selected topology, advertised Octavia API version, Amphora availability, and any requested flavor or availability zone.
Unknown or unverifiable capabilities are not accepted.

| Configuration | Minimum configured Octavia microversion | Additional reads |
| --- | --- | --- |
| Base HTTP profile | `2.5` | API version discovery, provider list, selected subnets, and their networks. Endpoint mode requires IPv4 subnets. |
| `flavorID` | `2.6` | Flavor and flavor profile, including proof that the profile uses Amphora. |
| `availabilityZone` | `2.14` | Availability zone and its profile, including proof that the profile uses Amphora. |
| `floatingIP.mode: Allocate` | `2.5` | Selected external network. Recovery also reads Neutron revision extensions. |
| `frontendSecurity.mode: Referenced` | `2.29` | Selected groups and VIP subnet, plus flavor security properties when configured. |
| `backendSecurity.mode: Referenced` | `2.5` | Selected groups and exact worker ports, including existing associations and port security. |

The configured microversion must also be advertised by the cloud.
The base Deployment uses `--octavia-microversion=2.5`.
Raise that value when using optional fields and verify the cloud supports it.
Some clouds restrict flavor or availability zone profile reads.
If the controller cannot read the required profile, it cannot validate that option.

`Accepted=True` means these reads and configuration checks succeeded.
It does not prove permission to create or delete resources, available quota, backend reachability, or Gateway API conformance.
The resolver does not perform trial writes or change Keystone policy.
The deployment credential still needs the Octavia and Neutron permissions required to reconcile its owned graph.
Provisioning failures use fixed conditions and Events without publishing OpenStack response bodies.

Missing parameters, invalid selectors, or unsupported capabilities produce `Accepted=False` with `InvalidParameters` on GatewayClass.
Authentication and authorization failures preserve the last evaluated `ObservedGeneration` and retry slowly after reporting a redacted diagnostic.
Correct the configuration or restore the required access, then let reconciliation retry.

## Configuration after Gateway creation

Before creating cloud resources, the controller records the member mode, resolved IDs, health monitor settings, Node address type, and parameter object identity in the Gateway's durable binding.
It persists that binding with its finalizers and returns before provisioning.
The resulting configuration remains fixed for that Gateway UID.

Editing the parameter object or changing its selectors affects Gateways that have not yet acquired a binding.
It does not change an existing Gateway's member mode, subnet, flavor, availability zone, Floating IP policy, or referenced groups.
Deleting the parameter object makes the referencing GatewayClass invalid for new bindings, but an already bound Gateway continues to use its stored configuration.
Its cleanup does not require the parameter object to exist.

The binding also survives replacement of an owned graph for the same Gateway UID, including a listener port change or cleanup after an invalid Gateway spec.
Correcting the spec rebuilds the graph with the original configuration.
To use different class settings, let the old Gateway finish deletion and create a new Gateway with a new UID.
Do not edit the controller's binding annotations or finalizers to change topology.

A partial or changed binding stops mutation with an ownership conflict.
Recovery requires the original identity and intact binding, as described in the [operator recovery guide](../operator-recovery.md).
Deleting the entire configuration CRD is different from deleting one parameter object: the running controller still requires the API for its watches.
Keep the CRD installed until all installations using it have been removed.

## Direct endpoint members

Set `spec.memberMode: Endpoint` on a class configuration to select the development implementation from [ADR 0007](../design/adr/0007-direct-endpoint-members.md).
This mode has local tests but no verified OpenStack environment yet.
Keep `backendSecurity.mode: Unmanaged` and select IPv4 VIP and member subnets.
The current OpenStack E2E runner exercises NodePort modes and does not validate this path.

For a Service backend, the HTTPRoute specifies a Service port.
The controller resolves that port through EndpointSlices and registers their IPv4 addresses and resolved ports as Octavia members. HTTPRoute and TCPRoute use TCP Service and EndpointSlice ports. UDPRoute requires UDP on both.
It accepts ClusterIP, headless, and NodePort Services, including manually managed EndpointSlices selected by namespace and the `kubernetes.io/service-name` label.
It matches the Service port name rather than the `targetPort` string, allowing different resolved ports in different slices.
LoadBalancer and ExternalName Services remain unsupported.
Endpoint mode does not use Node eligibility, `nodeAddressType`, or Service traffic policies to choose members.

Amphora must reach those addresses from the selected `memberSubnet`, and responses must return to the actual source address of Amphora's backend connection.
An endpoint address may be outside that subnet's CIDR when routing provides the path.
The controller does not configure routes, CNI tunnels, worker ports, security groups, or NetworkPolicies.
Establish and test those prerequisites before cloud use.
Successful class validation or member creation does not establish reachability.

Ready, serving endpoints receive traffic.
Serving endpoints marked terminating use Octavia weight zero until they disappear or stop serving.
This requests draining but does not guarantee that existing connections finish successfully.
Nil ready and serving conditions are treated as true, and nil terminating is false.
Deleting slices are ignored, and conflicting duplicate observations use the less available state.
The controller rejects loopback, link-local, multicast, and the referenced Service's virtual addresses.

Endpoint updates preserve unchanged member IDs.
For an HTTPRoute, a valid Service with no eligible endpoints keeps its pool, monitor, and route policies with an empty member list. The optional Octavia HTTP actions serve HTTP 500 for an invalid backend. [Controller flags](../getting-started.md#optional-octavia-http-actions)
By default, losing the last eligible endpoint of a positive-weight TCPRoute or UDPRoute Service withdraws forwarding and deletes the route's pool. Reconciliation recreates the pool when the endpoints return. Plain TCP can opt into active rejection only for the affected share on a capable regional Amphora deployment. [Proportional TCP rejection](../operator-octavia-tcp-connection-rejection.md) · [Weighted TCP Services](../getting-started.md#forward-tcp-to-a-service) · [Weighted UDP Services](../getting-started.md#forward-udp-to-a-service)
IPv6 endpoint forwarding, broader route mappings, and conformance remain separate work.

### Select Pods with an Experimental XBackend

An HTTPRoute on an Endpoint-mode Gateway can select Pods in its namespace through an `XBackend`. Install the v1.7.0 Experimental bundle and the `config/experimental` controller overlay. The following objects use an existing HTTP Gateway named `edge` in `apps`. The application must listen on port 8080 and its Pods must carry both labels. [Experimental installation](../getting-started.md#prerequisites)

```yaml
apiVersion: gateway.networking.x-k8s.io/v1alpha1
kind: XBackend
metadata:
  name: reports
  namespace: apps
spec:
  type: EndpointSelector
  endpointSelector:
    matchLabels:
      app: reports
      track: green
  port:
    number: 8080
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: reports
  namespace: apps
spec:
  parentRefs:
    - name: edge
  rules:
    - backendRefs:
        - group: gateway.networking.x-k8s.io
          kind: XBackend
          name: reports
```

The backend supplies the port, so omit `backendRefs[].port`. The controller selects ready, nonterminating Pods with routable IPv4 addresses. Pods using the host network are ineligible for this endpoint profile. Changes to the selector, Pod labels, readiness and Pod addresses update pool membership. The Route and XBackend must share a namespace. Use the HTTPRoute parent conditions and the XBackend's `status.ancestors` to check the current generation for your Gateway. [XBackend specification](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apisx/v1alpha1/xbackend_types.go) · [Namespace contract](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/geps/gep-4894/index.md#route-attachment-and-consumer-overrides)

#### Forward a GRPCRoute to an XBackend

Use the Experimental bundle and controller overlay on an Endpoint-mode Gateway with unmanaged backend security and a network path to Pod IPv4 addresses. Enable the [native GRPCRoute frontend](../qualification/v1.7.0/provider/grpc-h2c-controller.md), including `--octavia-backend-h2c` and either `--octavia-fixed-http-responses` or `--octavia-weighted-http-targets`. For multiple primary references use `--octavia-weighted-http-targets`.

An `EndpointSelector` XBackend in the GRPCRoute's namespace supplies the port. Omit the reference's `port`. An XBackend in a different namespace remains forbidden even with a ReferenceGrant. For example, the `reports` XBackend above can serve the following native route if its selected Pods implement gRPC. Use a listener and hostname combination available to GRPCRoute.

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: GRPCRoute
metadata:
  name: reports-rpc
  namespace: apps
spec:
  parentRefs:
    - name: edge
  rules:
    - matches:
        - method:
            service: reports.v1.Reports
            method: GetReport
      backendRefs:
        - group: gateway.networking.x-k8s.io
          kind: XBackend
          name: reports
```

Omit `spec.protocol` to inherit gRPC, or set it to `GRPC`. With omitted TLS or `tls.mode: None`, the backend uses h2c and requires regional `backend_h2c_v1`. Explicit `H2C` also works in this mode. With `ServerOnly` or `ClientAndServer`, the backend uses authenticated TLS with mandatory `h2` ALPN. Explicit `HTTP2` also works in these modes. Enable backend TLS support and the matching regional Octavia capabilities, and enroll the client certificate for mutual TLS. [Backend TLS configuration](../operator-octavia-backend-tls-policy.md#protect-an-experimental-xbackend)

The GRPCRoute profile supports primary EndpointSelector references. ExternalHostname, XBackend session persistence and XBackend references in RequestMirror are unsupported in this profile. The GRPCRoute parent reports unresolved or incompatible references, and a weighted invalid reference returns gRPC `UNAVAILABLE` for its assigned share. The XBackend publishes its decision per Gateway in `status.ancestors`. For a Gateway with both GRPCRoute and HTTPRoute consumers, one compatible profile is sufficient for the ancestor to accept the backend. Check each route's `ResolvedRefs` for its own result. Kubernetes envtest checks route and backend status and the desired graph. Regional dataplane qualification remains open.

#### Select an application protocol for an HTTPRoute

Set `spec.protocol` on an Experimental `EndpointSelector` or operator-authorized `ExternalHostname` XBackend. The HTTPRoute matches the request and forwards it to the selected backend.

| Protocol | Connection from Amphora to the backend |
| --- | --- |
| `GRPC` | With `tls.mode: None` or omitted TLS, use h2c. Enable `--octavia-backend-h2c` and verify regional `backend_h2c_v1` support. With `ServerOnly` or `ClientAndServer`, require `h2` ALPN on authenticated TLS. |
| `WSS` | Use the RFC 6455 HTTP/1.1 Upgrade handshake over authenticated TLS. The pool offers only `http/1.1` in ALPN. Set `tls.mode: ServerOnly` or `ClientAndServer`. |
| `MCP` | Use Streamable HTTP over HTTP/1.1. Use `tls.mode: None` for a cleartext origin, or configure `ServerOnly` or `ClientAndServer` for an authenticated origin. |

Authenticated modes require `--octavia-backend-tls-policy`, Octavia API microversion 2.24 or newer and regional `backend_tls_hostname_v1`. Authenticated gRPC also requires `backend_tls_h2_alpn_v1`. For mutual TLS, enroll the XBackend client certificate. The client-facing listener must support the client protocol, including HTTP/2 for native gRPC and HTTP/1.1 Upgrade for WebSocket. [Backend TLS configuration](../operator-octavia-backend-tls-policy.md#protect-an-experimental-xbackend) · [RFC 6455](https://www.rfc-editor.org/rfc/rfc6455)

For MCP, route the endpoint's POST, GET and DELETE requests as needed by the server. The HTTP pool forwards the endpoint's HTTP traffic, including SSE and headers such as `Origin`, `MCP-Session-Id`, `MCP-Protocol-Version` and `Last-Event-ID`. Keep these headers intact when applying route filters. For header affinity on an MCP backend, use a dedicated name such as `X-Backend-Affinity`. The controller rejects `MCP-Session-Id` regardless of case. For gRPC over h2c or TLS, use a custom ASCII metadata key without the `-bin` suffix. The MCP server handles JSON-RPC, authentication, Origin validation and session semantics. Existing Amphora connection timeouts apply to WebSocket and SSE streams. [MCP Streamable HTTP](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports#streamable-http)

These XBackend pools use Octavia TCP health monitors with the class timing and retry settings, because a plain HTTP GET can legitimately fail on an otherwise healthy protocol endpoint. These probes measure connection establishment. Controller tests cover reference resolution, transport capability checks and Octavia pool settings. Earlier local traffic trials observed ALPN `http/1.1` and required `h2` for these underlying TLS pool configurations. Application traffic qualification for the new protocol values is pending. [HTTP/1.1 TLS pool receipt](../qualification/v1.7.0/provider/xbackend-server-tls-evidence/receipt.json) · [HTTP/2 transport observations](../qualification/v1.7.0/provider/xbackend-http2.md)

#### Roll back to a controller before XBackend application protocol support

Keep the current controller running while you migrate the XBackends. The earlier controller treats a TCP monitor on an HTTP pool as an ownership conflict and can pause reconciliation for the Gateway.

1. Change each XBackend using `GRPC`, `WSS` or `MCP` to a previously supported protocol. Use `H2C` for cleartext gRPC, `HTTP2` for authenticated gRPC, and `HTTP11` for WebSocket and MCP, with the existing TLS settings. Ensure the origin returns 200–399 to an ordinary GET at the GatewayClass health monitor path over the selected transport. If the origin cannot serve that check, move its traffic and wait for the affected pools to be removed.
2. Wait for `Accepted=True` on each affected XBackend ancestor and for `ResolvedRefs=True` and `Programmed=True` on each affected HTTPRoute parent for the new configuration. In Octavia, verify that every remaining affected pool is active and has an active `HTTP` health monitor. Check that the expected members are healthy.
3. Deploy the earlier controller after those checks succeed.

#### Keep a session on one selected endpoint

For core Services, configure affinity with [a Service policy or a route rule](../operator-octavia-backend-retry-budget.md#configure-session-persistence-on-a-route-rule).

With the Experimental bundle, add `--octavia-xbackend-session-persistence` to the controller. The effective regional Amphora provider must advertise `http_cookie_name_v1`. Add the following under `spec` of an `EndpointSelector` XBackend:

```yaml
sessionPersistence:
  type: Cookie
  cookie:
    name: reports_session
    path: /
    lifetimeType: Session
```

Omit `name` to generate a stable name from the XBackend UID. All routes to that XBackend use the same generated name, including across route and port edits. Give explicitly named cookies distinct names for different backends sharing a host. Names may contain 1–255 ASCII letters, digits, underscores, dots or hyphens. The prefixes `__Host-`, `__Secure-` and `__Http-` are reserved regardless of case.

Traffic splitting still selects a backend according to the rule's weights. Its cookie selects an eligible endpoint within that backend. The XBackend cookie profile covers HTTP and HTTP/1.1, including [ServerOnly backend TLS](../operator-octavia-backend-tls-policy.md#protect-an-experimental-xbackend) with its separate opt-in and Octavia microversion 2.24 or newer. Cookies default to path `/` and a session lifetime. For a custom scope, set `spec.sessionPersistence.cookie.path` on the Experimental EndpointSelector XBackend and enable `enabled_http_cookie_path_providers = amphora` in Octavia’s `[api_settings]`. Verify that the region advertises `http_cookie_path_v1` alongside the cookie capabilities your profile uses. With `/reports`, clients reuse the affinity cookie on that path and its descendants. The permanent option below gives the client an expiry date. The XBackend API permits session persistence only for EndpointSelector. [Cookie Path measurements](../qualification/v1.7.0/provider/xbackend-cookie-path.md) · [Octavia packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-http-cookie-path-3d73d50eb78d)

For a cleartext HTTP/2 origin, use `protocol: H2C` and `tls.mode: None` on the same XBackend. Add `--octavia-backend-h2c` alongside `--octavia-xbackend-session-persistence` and enroll the effective Amphora provider in both `enabled_backend_h2c_providers` and `enabled_http_cookie_name_providers` in Octavia's `[api_settings]`. The regional capability endpoint must advertise both `backend_h2c_v1` and `http_cookie_name_v1`, plus the existing capabilities for any custom cookie options. The H2C setting controls the connection from Amphora to the selected endpoint.

For cookie affinity over authenticated HTTP/2, set `protocol: HTTP2` and use `ServerOnly` or `ClientAndServer` with the corresponding trust and client certificate setup. Enable `--octavia-backend-tls-policy` and `--octavia-xbackend-session-persistence` with Octavia API microversion 2.24 or newer. Verify that the effective Amphora provider advertises `backend_tls_hostname_v1`, `backend_tls_h2_alpn_v1`, `http_cookie_name_v1` and any capabilities required by the selected trust or cookie options. The controller checks the pool's TLS settings, required `h2` ALPN and cookie settings before admitting endpoints. [Configure authenticated XBackends](../operator-octavia-backend-tls-policy.md#protect-an-experimental-xbackend)

To limit the age of endpoint affinity, set `spec.sessionPersistence.absoluteTimeout` on the XBackend, for example `1s500ms`. The existing controller option also requires the region to advertise `http_cookie_absolute_timeout_v1`. Configure the shared worker signing key, apply the database migrations and use eligible Amphora agents before enabling the regional capability. [Octavia configuration and packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-xbackend-cookie-deadline-ebc23c65cea7)

The time starts when the proxy issues the cookie. Reusing it on another route to the same XBackend keeps the original deadline. At expiry the proxy selects an eligible endpoint again. A zero duration expires on the next request. With `lifetimeType: Session`, removing the timeout restores ordinary named session affinity. Timed cookies use the Amphora wall clock, so clock skew between instances and HAProxy time sampling affect the observed deadline.

For a cookie that the client can retain when a session ends, configure the following fields on the XBackend. A permanent cookie requires an absolute timeout. Enable `http_cookie_permanent_v1` in addition to the two named and timed cookie capabilities after updating the database, Octavia workers and eligible Amphora agents. [Octavia packages and pool API](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-xbackend-cookie-permanent-0a0c55cbc7c3)

```yaml
sessionPersistence:
  type: Cookie
  cookie:
    name: app_affinity
    lifetimeType: Permanent
  absoluteTimeout: 30m
```

The proxy issues `Max-Age` in seconds. It rounds a positive fraction up to the next whole second and enforces the original millisecond deadline on requests. For example, `1s500ms` gives `Max-Age=2` and the gateway expires affinity after 1.5 seconds. A zero duration gives `Max-Age=0`. When switching the same timed cookie back to `Session`, the next request using the old persistent cookie receives a session cookie. [Gateway API cookie lifetimes](https://github.com/kubernetes-sigs/gateway-api/blob/v1.7.0/apis/v1/shared_types.go)

On Experimental, a client saved a five-minute permanent cookie, reopened its cookie jar, and reached the same Pod through both HTTPRoutes. Switching the XBackend to Session replaced the still-valid permanent cookie. The next client save retained no session cookie. With a 1.5-second deadline the proxy issued Max-Age=2. The client kept its endpoint at 0.604 seconds and an explicit replay at 1.664 seconds received a new cookie. A zero deadline issued Max-Age=0 and left the client jar empty. On Standard the ordinary Service answered all 3 requests with HTTP 200. The trial used local kind and HAProxy. Client retention was exercised with Python’s MozillaCookieJar. [Cookie attributes, client storage and endpoint observations](../qualification/v1.7.0/provider/xbackend-permanent-cookie-evidence/receipt.json)

Check the XBackend's current-generation `Accepted` condition under the Gateway ancestor and the HTTPRoute's `ResolvedRefs` and `Programmed` conditions. A missing provider capability reports `UnsupportedValue`. A failed discovery reports `Pending` while the controller retries. During either condition, a direct match uses an empty pool and a weighted rule returns HTTP 500 for the affected share. The controller empties an affected pool before changing or withdrawing the cookie. Once the replacement settings are verified, the pool can serve its eligible endpoints again. [Session persistence contract](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/geps/gep-1619/index.md)

On Experimental, two HTTPRoutes shared a cookie for one XBackend. With a 1.5-second timeout, five requests kept the original cookie and endpoint through 1.315 seconds. Starting at 1.951 seconds, eight replays of that cookie received a fresh cookie. Both routes reached both Pods. A zero timeout issued a cookie on all four requests. Removing the timeout restored native session affinity across both routes. Route and pool identities were stable. When the policy changed, the pools drained and Octavia assigned new member IDs to the same Pod addresses and weights. Traffic during the drain was not sampled. On Standard, the ordinary Service returned three HTTP 200 responses without a cookie. The local experiment used kind, the Octavia API and pool-update worker, the Amphora renderer and real HAProxy, with cloud identity, inventory, provisioning completion and transports supplied by fixtures. Regional deployment and failover remain open. [Timed cookie observations](../qualification/v1.7.0/provider/xbackend-cookie-deadline-evidence/receipt.json)

In a local kind trial, two HTTPRoutes shared an EndpointSelector. Across their two pools, the client sent 9 requests to one Pod. When it became unready, the next request with the old cookie reached the other Pod and its response renewed the cookie. A separate ServerOnly backend issued its own session cookie. [Requests and pool membership](../qualification/v1.7.0/provider/xbackend-session-cookie-evidence/receipt.json)

#### Keep endpoint affinity with a request header

With the Experimental bundle and an Amphora GatewayClass in Endpoint mode, enable `--octavia-xbackend-session-persistence` and set this under `spec` of an EndpointSelector XBackend. Clients store the header value returned by the Gateway and send it on subsequent requests to the same backend. The signed value is scoped to the Octavia pool.

```yaml
sessionPersistence:
  type: Header
  header:
    name: X-Reports-Session
  absoluteTimeout: 5m
```

Install the matching Octavia and octavia-lib packages, upgrade the database, and configure `[haproxy_amphora] http_cookie_signing_key_file` on every regional worker with the same private 32-byte key. Enroll `amphora` in `[api_settings] enabled_http_header_persistence_providers` after updating the serving Amphorae. Verify that the region advertises `http_header_persistence_v1`. Use the separate backend transport configuration for HTTP/2 origins. [Octavia packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-http-header-persistence-3aa1cc04e714) · [octavia-lib packages](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-http-header-persistence-eb4c646b5975) · [Backend HTTP/2 setup](../qualification/v1.7.0/provider/xbackend-http2.md)

On a local Experimental kind Gateway, HTTP/1.1 clients replayed signed headers to two Pods over h2c. With one Pod removed from the selector, the remaining Pod served the old token's next request and the Gateway issued a replacement. Removing the signing key returned three HTTP 503 responses with zero Pod requests. The controller cleared and restored persistence on the original pool, and traffic resumed after the key returned. An ordinary route answered its probes throughout, including on Standard, where XBackend is absent. [Gateway traffic and recovery](../qualification/v1.7.0/provider/xbackend-session-header-evidence/receipt.json)

A separate local run used the pinned Octavia and octavia-lib wheels with HAProxy 3.0.19. It observed one token issue and three pinned replays per member over both h2c and TLS with ALPN `h2`. The wrong TLS name and an untrusted CA each returned HTTP 503 with zero origin requests. This run exercised provider traffic with HTTP/1.1 clients. The kind trial used the signed controller image's binary, the installed provider wheels, the Amphora reload method and real HAProxy and Pods, with cloud provisioning and Amphora transport supplied by fixtures. Qualification on a deployed Hibiscus region remains open. [Provider and Gateway receipts](../qualification/v1.7.0/provider/xbackend-session-header-evidence/receipt.json)

### Route HTTP to an Experimental XBackend hostname

Install the v1.7.0 Experimental bundle and controller overlay. Select an Amphora GatewayClass with `memberMode: Endpoint` and `backendSecurity.mode: Unmanaged`. Add these manager arguments for each permitted GatewayClass and destination prefix. The example uses a documentation-only IPv4 range.

```text
--gateway-api-channel=experimental
--xbackend-external-hostname-class=edge
--xbackend-external-hostname-cidr=198.51.100.0/24
--xbackend-service-dns-domain=cluster.example
```

The CIDR allow-list applies to all enabled classes in the manager. To turn off routed hostnames, remove the `--xbackend-external-hostname-class` arguments and restart the manager. The manager can keep the IPv4 and Service DNS options while the feature is off. List every additional Service DNS suffix with `--xbackend-service-dns-domain`, which is repeatable. The controller already rejects `cluster.local`. Use lowercase DNS labels in a fully qualified name without a trailing dot, and omit the HTTPRoute's `backendRefs[].port`. Put the port on the same-namespace XBackend.

```yaml
apiVersion: gateway.networking.x-k8s.io/v1alpha1
kind: XBackend
metadata:
  name: origin
  namespace: apps
spec:
  type: ExternalHostname
  port:
    number: 8080
  externalHostname:
    hostname: origin.example.test
```

Use the same `group`, `kind` and `name` fields as for an EndpointSelector reference. IP literals and names equal to or below a configured Service DNS suffix are rejected. The controller queries its system resolver using an absolute name and takes the distinct, sorted IPv4 unicast results within the allowed CIDRs. AAAA answers, loopback and link-local destinations are ineligible. Each eligible address receives an ordinary pool member and an equal initial weight. Health checks use the class's HTTP monitor settings.

The manager's resolver view must return addresses reachable from the Amphora network, including where split DNS is in use. Verify the routes, egress and origin ingress for that path. Octavia receives the IP and port with `subnet_id` omitted. The operator supplies routing and security before enabling a class. This mode cannot satisfy the `Referenced` contract for exact Neutron compute ports and security groups. Reserve the CIDRs for the intended destinations and exclude internal ranges that Amphora should never reach, including addresses a DNS alias could return. Plain HTTP sends traffic in cleartext. Restrict XBackend creation to the namespaces and principals allowed to use this egress path. [Gateway API DNS trust model](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/geps/gep-4894/index.md#dns-trust-model-decision)

The controller shares one DNS observation across the mutations of a Gateway graph for a 24–36 second refresh window. At expiry it queues the Gateway once, using the ordinary HTTPRoute resync interval for individual routes. This also works when there is no Kubernetes event. A change to the backend or loss of attachment invalidates the affected observation. A change to the Gateway's class or topology invalidates the Gateway's observations. A manager restart starts with an empty cache. Each DNS lookup has a two-second timeout. The resolver's cache and Octavia provisioning affect when a new answer reaches traffic. With NXDOMAIN, no permitted IPv4 answer, or a transient resolver failure, the controller withdraws the members from the same pool. A pool without members returns HTTP 503. DNS recovery repopulates it. DNS data changes leave the XBackend ancestor `Accepted` and HTTPRoute parent `ResolvedRefs` conditions based on the configured reference. An unsupported spec or class sets `Accepted=False` on the XBackend ancestor and `ResolvedRefs=False` on the HTTPRoute parent. The controller reports `ResolvedRefs=Unknown` for an otherwise valid external reference while the Gateway member mode is pending. Namespace and missing-backend errors use their usual reference reasons.

ExternalHostname and EndpointSelector support HTTP and HTTP/1.1. For a cleartext HTTP/2 origin, use `protocol: H2C` and `tls.mode: None`, enable `--octavia-backend-h2c` and verify the region advertises `backend_h2c_v1`. Use `protocol: HTTP2` with authenticated TLS for a server negotiating `h2`. [HTTP/2 transport and health probes](../qualification/v1.7.0/provider/xbackend-http2.md) TLS defaults to `None`. Use `ServerOnly` with authenticated Octavia backend TLS and an explicit `validation.hostname`. `ClientAndServer` also requires an operator-enrolled XBackend client certificate. In a local cleartext trial, an unchanged HTTPRoute followed A to B, returned HTTP 503 on NXDOMAIN and served A again after recovery. The sibling Service continued to return HTTP 200. Standard installations omit XBackend. A local Experimental mutual TLS run followed DNS from A to B and back. A served HTTP 200. B presented a certificate from the trusted CA with a different DNS name, and the Gateway returned HTTP 503. The HTTPRoute stayed accepted throughout. Qualification on a deployed Hibiscus region and attachments to other route kinds remain open. The upstream API allows session persistence only on EndpointSelector. [Mutual TLS across a DNS change](../qualification/v1.7.0/provider/xbackend-external-client-tls-evidence/receipt.json) [DNS traffic and member changes](../qualification/v1.7.0/provider/xbackend-external-hostname-evidence/receipt.json) · [Configure XBackend server authentication](../operator-octavia-backend-tls-policy.md#protect-an-experimental-xbackend) · [Enroll an XBackend client certificate](../operator-octavia-backend-tls-policy.md#present-an-xbackend-client-certificate) · [XBackend specification](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apisx/v1alpha1/xbackend_types.go)

## Worker NodePort members

A Node is eligible only when it is ready, schedulable, not being deleted, and has a valid IP address of the configured `nodeAddressType`.
The presence of either `node-role.kubernetes.io/control-plane` or `node-role.kubernetes.io/master` excludes it, regardless of the label's value.
A `node-role.kubernetes.io/worker` label is not required and cannot override a control plane label.
There is no control plane opt-in.

The `node.kubernetes.io/exclude-from-external-load-balancers` label applies to the remaining Nodes:

| Label value | Membership |
| --- | --- |
| Absent | Eligible if the other checks pass. |
| A false boolean value | Eligible if the other checks pass. |
| Empty, true, or invalid | Excluded. |

Boolean parsing accepts Go's standard spellings, including `false`, `False`, `FALSE`, `f`, `F`, and `0` as false.
Use `"false"` when an explicit value is needed.

Both supported traffic policies require a ready, nonterminating backend endpoint for the selected Service port.
The EndpointSlice port must match the Service port name and protocol and provide a resolved port number.
For a UDP NodePort, the chosen worker address and ready EndpointSlice must use the VIP’s IP family. Amphora’s IPVS virtual server can only forward to members in that family.
Its address family must be one of the Service's configured families and match the selected Node address.
With `externalTrafficPolicy: Cluster`, eligible workers can use a ready endpoint in that family anywhere in the cluster.
With `externalTrafficPolicy: Local`, that endpoint must also identify the worker through `endpoints[].nodeName`.
The route remains pending when no worker is eligible.
Node and EndpointSlice changes recalculate membership, preserving members whose address and NodePort have not changed.

Operators must ensure that the selected Node addresses belong to the configured member network and that Amphora can reach their NodePorts.
This also applies when selecting `ExternalIP` instead of the default `InternalIP`.
The structural checks do not verify that path, and the controller does not create backend security groups or attach groups to worker ports.

## Development compatibility

The API group, default controller name, and binding format are pre-alpha contracts.
There is no automatic binding migration or resource adoption path.
Before changing the API group, controller name, cluster identity, or project scope, finalize the development resources with the original controller and credentials, then recreate the installation and its resources.
