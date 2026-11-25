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

HTTP listeners use an HTTP monitor that sends `GET` to `healthMonitor.path` and accepts status codes `200-399`. TCP and UDP listeners use Octavia's `TCP` and `UDP-CONNECT` monitors respectively. Octavia records the configured delay, timeout and both retry counts. For UDP, Hibiscus enforces an operator-configurable minimum delay that defaults to 3 seconds. The stock Amphora LVS template renders the failure threshold as `retry` and omits the separate recovery threshold. The HTTP path applies only to HTTP monitors. [Stock configuration and packet check](../qualification/v1.7.0/provider/stock-udp.md)
There is no option to disable the health monitor or supply an existing Floating IP.

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

#### Keep a session on one selected endpoint

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

Traffic splitting still selects a backend according to the rule's weights. Its cookie selects an eligible endpoint within that backend. The profile covers HTTP and HTTP/1.1, including [ServerOnly backend TLS](../operator-octavia-backend-tls-policy.md#protect-an-experimental-xbackend) with its separate opt-in and Octavia microversion 2.24 or newer. The supported cookie has path `/` and a session lifetime. Other paths, absolute timeouts, permanent cookies, header persistence, h2c and ExternalHostname are unsupported. The deprecated placement on HTTPRoute and GRPCRoute rules and Service XBackendTrafficPolicy is unsupported.

Check the XBackend's current-generation `Accepted` condition under the Gateway ancestor and the HTTPRoute's `ResolvedRefs` and `Programmed` conditions. A missing provider capability reports `UnsupportedValue`. A failed discovery reports `Pending` while the controller retries. During either condition, a direct match uses an empty pool and a weighted rule returns HTTP 500 for the affected share. The controller empties an affected pool before changing or withdrawing the cookie. Once the replacement settings are verified, the pool can serve its eligible endpoints again. [Session persistence contract](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/geps/gep-1619/index.md)

In a local kind trial, two HTTPRoutes shared an EndpointSelector. Across their two pools, the client sent 9 requests to one Pod. When it became unready, the next request with the old cookie reached the other Pod and its response renewed the cookie. A separate ServerOnly backend issued its own session cookie. [Requests and pool membership](../qualification/v1.7.0/provider/xbackend-session-cookie-evidence/receipt.json)

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

The ExternalHostname profile supports HTTP and HTTP/1.1. TLS defaults to `None`. Use `ServerOnly` with authenticated Octavia backend TLS and an explicit `validation.hostname`. `ClientAndServer` also requires an operator-enrolled XBackend client certificate. In a local cleartext trial, an unchanged HTTPRoute followed A to B, returned HTTP 503 on NXDOMAIN and served A again after recovery. The sibling Service continued to return HTTP 200. Standard installations omit XBackend. Live mutual TLS qualification and attachments to other route kinds remain open. The upstream API allows session persistence only on EndpointSelector. [DNS traffic and member changes](../qualification/v1.7.0/provider/xbackend-external-hostname-evidence/receipt.json) · [Configure XBackend server authentication](../operator-octavia-backend-tls-policy.md#protect-an-experimental-xbackend) · [Enroll an XBackend client certificate](../operator-octavia-backend-tls-policy.md#present-an-xbackend-client-certificate) · [XBackend specification](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apisx/v1alpha1/xbackend_types.go)

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
