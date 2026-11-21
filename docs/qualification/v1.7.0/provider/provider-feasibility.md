# Stock Hibiscus feasibility for Gateway API v1.7.0

On loopback, stock Octavia and HAProxy forwarded TCP and routed two exact gRPC methods on a single TLS-protected HTTP/2 connection. The gRPC fixture used both a terminating HTTPS listener and TLS-enabled HTTP pools. [TCP traffic](stock-tcp.md) · [gRPC traffic](stock-grpc.md)

Full `GATEWAY-HTTP` requires rule-level arbitrary request mutations and defined 404/500 outcomes, including the invalid backend's weighted share. Full `GATEWAY-GRPC` requires more than exact path matching. `GATEWAY-TLS` requires SNI selection before TLS termination. `MESH-HTTP` and `MESH-GRPC` require a dataplane that handles calls to Kubernetes Service frontends. The remaining mappings below identify requirements and source limits. [Profile registry][profiles] · [Public API][octavia-api]

For UDP, a public listener update selected different pools on the same VIP. The stock Amphora worker generated the Keepalived configuration used by kernel IPVS in a disposable guest. Both selected backends echoed all 256 payload bytes. Clearing the pool removed the virtual service and the next client datagram received `ECONNREFUSED`. The controller maps a dedicated UDP Gateway and its oldest attached UDPRoute to this single-pool interface. The public API selects a pool, with member weights inside that pool. The studied public interface exposes no explicit drop target to preserve an invalid backend's share alongside valid backends. Endpoint mode can assign a valid Service's share to its pod IPs within one pool. Weighted NodePort backends and the selective packet-loss contract remain open. [UDP API and packet study](stock-udp.md) · [Operator example](../../../getting-started.md#forward-udp-to-a-service)

## Verified versions

The official Hibiscus index gives the following release pins. The Gateway API Git checkout's tag and HEAD both resolved to the commit shown. Cloud package versions, enabled drivers, the Amphora image and HAProxy build, and permissions are deployment facts to verify separately. [Hibiscus index][hibiscus] · [Gateway API release][gateway-release]

| Component | Release | Commit |
|---|---|---|
| Gateway API Standard and Experimental | v1.7.0 | [`e26d183dbbc1bb782b78addbd26faba03955c152`][gateway] |
| Octavia, including Amphora driver and templates | 19.0.0 | [`93689cb7701da65b5aad387bb824c477c64b0f2b`][octavia] |
| octavia-lib | 3.12.0 | [`bc03e1856b60517e1b75db4fdf9b3edf55f3a019`][octavia-lib] |
| OVN Octavia provider | 11.0.0 | [`485b18e661d368feb82f87c499777052ee749458`][ovn] |
| Neutron | 29.0.0 | [`1ca1e2bb5b3c6b47913f25544d02bba20b198fac`][neutron] |
| Keystone | 30.0.0 | [`8755458dc5399d3ae7a9ae31f9f738da0427ac0d`][keystone] |
| Barbican | 23.0.0 | [`1efe16cd73b6e7d7c827aa8090665f2564de632e`][barbican] |

## What the public operations establish

| API | Available operations | Qualification boundary |
|---|---|---|
| Keystone v3 | Authenticate via `POST /v3/auth/tokens`; discover the catalog for the selected project, region and interface. Application credentials can delegate a subset of project roles and can expire. | Use a project-scoped token and verify the roles and policy of each target service. Application credentials do not grant arbitrary administrator or provider-agent access. [Authentication and application credentials][identity] |
| Octavia v2 | Version discovery, enabled drivers at `GET /v2/lbaas/providers`, flavor/AZ capability discovery, and CRUD for load balancers, listeners, pools, members, monitors, L7 policies and rules. `GET /v2/lbaas/loadbalancers/{id}/status` reports provisioning and operating states. `/v2.0` is an alias for `/v2`. | Tagged discovery advertises `v2.30` as CURRENT. Flavor/AZ discovery describes configuration for those purposes. An enabled provider name, API field, or ACTIVE status does not establish traffic behavior or a feature set. [Version discovery][api-versions] · [Octavia endpoints and status][octavia-api] |
| Neutron v2.0 | Discover extensions and inspect network, subnet, port and security-group resources. Associate a floating IP with the load balancer's Neutron VIP port, optionally selecting its fixed IP. | FIP, routing and security-group policy affect reachability. Member address and subnet fields require a valid reachable path. Registering an overlay Pod IP does not create that path. These operations provide no awareness of HTTP rules, ClientHello SNI or Kubernetes Service interception. [Networking API][network-api] · [Member API][member-api] |
| Barbican v1 | Store a PKCS#12 Secret and return a reference; pass the reference to an Octavia terminated listener via `default_tls_container_ref` or `sni_container_refs`. Pool TLS and CA settings are separate. | Confirm actual key manager, cloud and service identity access, and a safe ownership/rotation design. Secret metadata writes are separate operations and cannot establish ownership at creation if the create response is lost. Secrets used for TLS termination give no passthrough SNI selector. [Barbican Secrets][secrets] · [Secret metadata][secret-metadata] · [Octavia PKCS#12 reference][pkcs12] |

The tagged schema exposes four L7 actions (`REJECT`, `REDIRECT_TO_URL`, `REDIRECT_TO_POOL`, `REDIRECT_PREFIX`) and eight predicate types (host, path, file type, header, cookie and three client-certificate attributes). The redirect code enum is 301, 302, 303, 307 and 308. L7 key/value fields have a 255-character limit. Amphora accepts L7 policies on HTTP and TERMINATED_HTTPS listeners. OVN's protocol allow-list is TCP, UDP and SCTP, with SOURCE_IP_PORT and SOURCE_IP algorithms. Its driver supplies no L7 policy/rule implementation and inherits the driver's unsupported defaults. Its member create/update paths do not relay member weights. [Tagged schema snapshot](stock-api-snapshot.txt) · [Policy schema][policy] · [Rule schema][rule] · [Amphora driver][amp-driver] · [OVN constants][ovn-constants] · [OVN driver][ovn-driver] · [Provider base][provider-base]

## Minimal traffic cases

### Stock API checks for HTTP headers and responses

On the public Octavia 19.0.0 WSGI API, an HTTP listener accepted `X-Forwarded-Port` and rejected `X-Tenant` with HTTP 400. The original forwarding header was still present on readback. A forwarding L7 policy rejected the same `insert_headers` field as an unknown attribute and kept its original pool. The redirect API accepted 301, and returned 400 for both 404 and 500. Its status field only selects the HTTP code for a redirect. The separate HAProxy traffic run observed 503 on a route miss. Together these observations document the gap behind the proposed per-route header and fixed-response fields. [Local API probe](stock-http-api.py) · [HTTP traffic record](../cloud-contract.md#http-traffic-from-the-adapters-own-writes)

The API probe uses Octavia's upstream SQLite fixture and no-op provider. It checks validation and reads persisted fields. Run it from the fork root with the [TCP probe's Python environment](stock-tcp.md#reproduce).

```sh
/tmp/octavia-tcp-venv/bin/python docs/qualification/v1.7.0/provider/stock-http-api.py
```

### Wire scenarios

Use [the Route and Gateway specimens](qualification-routes.yaml) as independent inputs. The multi-listener Gateway is an inventory of the three test listeners. For isolated proofs, keep only the relevant listener and its Routes in each pass. Use the namespace `feasibility`, a GatewayClass named `qualification` bound to the implementation under test, and Services named as in the YAML. Use HTTP echoes identifiable as A and B that return the received request headers, with two reachable A endpoints and one B endpoint for the weight case. Register reachable backend addresses via the selected test topology. Have the two gRPC test Services both implement `feasibility.v1.Echo/First` and `/Second`, with distinct backend IDs and a controlled upstream TLS trust setup. Supply a frontend TLS Secret valid for `grpc.example.test`. Have the two passthrough backends serve distinct trusted leaf certs and responses. In the mesh experiment, use a ClusterIP `frontend` Service with baseline endpoints A on port 80. Give traffic a known-good control before evaluating negative cases.

| Case | Traffic evidence to collect | Stock feasibility |
|---|---|---|
| Arbitrary rule-level request headers | Send `X-Test-Add: client-added`, `X-Test-Set: client-set`, `X-Test-Remove: client-remove` to `/mutate` and `/plain`. Echo A sees both add values (whether serialized as separate fields or a comma list), one replacement set value, and no remove field on `/mutate`. The same client values reach `/plain` intact. Run the upstream Core case as well. | Amphora's public `insert_headers` takes allow-listed listener switches. Its template populates some fixed forwarding or TLS-derived values. The accepted key list is wider than the implementations emitted by its template. A `HEADER` rule is a predicate. No rule-scoped arbitrary add/set/remove action exists. [Gateway filter][http-types] · [Upstream header test][headers-test] · [Listener validation][listener-validate] · [Amphora rendering][amp-headers] |
| Unmatched request | First confirm `/plain` reaches echo A with 200, then request the same listener and host at `/unmatched`. Expect 404 with no echo backend selected. | The stock HTTP traffic test returned 200 for a match and 503 for an unmatched request. A catch-all REJECT would give 403. Public L7 policy has no configurable 404 or route-miss response. Keeping the forwarding control prevents an infrastructure 404 from appearing to pass. [Adapter traffic](../cloud-contract.md#http-traffic-from-the-adapters-own-writes) · [HTTPRoute match requirements][http-types] · [Frontend rendering][amp-render] · [HAProxy backend selection][haproxy-backend] · [HAProxy deny][haproxy-deny] |
| Matched rule, omitted or empty backend list | `/omitted` and `/empty` have a matching rule without a response-producing filter. Expect HTTP 500, and no unresolved reference for these two rules (`ResolvedRefs=True` on their Route). | An empty member pool can expose availability failure, not chosen HTTP 500. REJECT is unparameterized; its code cannot be set through `redirect_http_code`. An error-serving application would change the stock direct architecture. [Upstream omitted-backend test][empty-test] · [Policy sanitization][policy-validate] · [Frontend rendering][amp-render] |
| Valid and invalid weighted BackendRefs | `/weighted-valid` yields approximately 75% A, 25% B with enough requests and independent connections. Then `/weighted-invalid` yields approximately 75% echo A and 25% 500, and `/only-invalid` always yields 500. The separate invalid Route reports `ResolvedRefs=False/BackendNotFound`. Keep response histograms and backend IDs; count transport errors or other HTTP codes independently. | A single Amphora ROUND_ROBIN pool with member weights 3, 3 for two A endpoints and 2 for B is a feasible 3:1 positive-control candidate. Rerun after an A endpoint changes readiness; a flattened pool changes the ratio unless it is rebalanced and does not prove independent group selection. Octavia limits members to integer weights 0–256, has a single pool target per L7 action and provides no synthetic 500 target. A dead member or pool is not a weighted fixed-response branch. A real Service with no ready endpoints has the separate recommended 503 behavior in the contract. [HTTP BackendRefs contract][http-types] · [Upstream invalid reference test][invalid-test] · [Member weight schema][member-schema] · [Amphora rendering][amp-render] · [OVN driver][ovn-driver] |
| Exact gRPC service and method | Over one HTTPS listener with ALPN h2, call `feasibility.v1.Echo/First` and `/Second` using authority `grpc.example.test`. Expect A and B respectively, successful application gRPC statuses and trailers, including on a reused HTTP/2 connection. Use the upstream two-backend exact matching test for formal suite evidence. Its fixtures use an HTTP listener and cleartext backends, so that run needs separate h2c proof. Check case sensitivity, same-length method names, a negative near-match, correct host with/without explicit port, and long valid identifiers separately. | The local Amphora test returned A, B, A, B on a single TLS-protected HTTP/2 connection, with a native gRPC success status and backend trailer on each response. Octavia can render `PATH/EQUAL_TO` as a case-sensitive HAProxy path ACL for `/feasibility.v1.Echo/First`, and another policy for `Second`, forwarding to separate HTTP pools. Use an HTTPS-terminating listener with `alpn_protocols: ["h2"]` plus HTTP pools with `tls_enabled: true` and `alpn_protocols: ["h2"]` on a compatible image. Frontend ALPN was added in Octavia v2.20 and pool ALPN in v2.24. The pool template emits ALPN only for TLS-enabled pools when image compatibility enables it. An exact or wildcard hostname containing an optional authority port needs separate host normalization evidence. An ordinary TCP listener can transport gRPC unchanged but cannot choose a backend by RPC. [Stock gRPC probe](stock-grpc.md) · [gRPC framing][grpc-framing] · [Gateway gRPC types][grpc-types] · [Upstream exact-match test][grpc-test] · [ALPN release note][pool-alpn] · [Rendering and gating][amp-jinja] |
| Complete gRPC behavior | Extend the positive gRPC test to include request and response metadata mutation, the share of calls to an invalid backend that produce `UNAVAILABLE`, and prior-knowledge h2c for an HTTP listener if that listener protocol is advertised. | gRPC name components can be 1,024 characters each; the short-path representation does not cover the allowed inputs. For supported service-only matching, slash-boundary prefix matching may be investigated. Method-only or regex matching is implementation-specific. Public listener or pool TLS ALPN confers no h2c prior-knowledge configuration. An installed HAProxy could behave differently at ingress and egress, so check both; the tagged render shows no explicit public h2c selection. Octavia has no arbitrary route-scoped request/response mutation or weighted synthetic gRPC response branch. gRPC clients commonly map bare HTTP 503 without grpc-status to UNAVAILABLE; observing that on a dead pool cannot prove either explicit gRPC response semantics or correct weighted routing when valid targets coexist. [Gateway gRPC types][grpc-types] · [gRPC client fallback][grpc-status] · [Public schemas][octavia] |
| Same-port TLS/SNI passthrough | Connect to the same VIP and TCP port 9443, varying only ClientHello SNI between `a.example.test` and `b.example.test`. Confirm each returns its own backend leaf fingerprint and application ID. Keep the TCP connection fresh for each SNI. Also test a nonmatched name against the intended route isolation and exact-versus-wildcard precedence. | Amphora TCP or its opaque HTTPS listener and OVN TCP can carry a TLS stream to one selected pool. A constant backend pool per distinct port can demonstrate transport. Neither exposes selective ClientHello SNI on a single public listener. Amphora's `timeout_tcp_inspect` renders only a delay. L7 policy admission is restricted to HTTP and TERMINATED_HTTPS. Certificate predicates inspect client certificates in terminated TLS. The native Amphora API probe returned HTTP 501 before dispatching a TCP listener's requested L7 policy. [Public API refusal](stock-tcp.md) [TLSRoute types][tls-types] · [Upstream hostname precedence test][tls-test] · [Amphora driver][amp-driver] · [Mode mapping][amp-jinja] · [Frontend rendering][amp-render] |
| Service-parent east-west | From ordinary in-cluster pods, call `frontend.feasibility.svc` as deployed. Before attaching the producer route, the healthy Service should reach baseline A. With the producer route accepted, `/split` reaches B for clients in the service namespace and a second namespace, and the unmatched path is rejected with 404. Query the original Service DNS throughout and compare against an unrelated Service. | A Service can be an Octavia member destination or can have a separate externally addressed LB, but Octavia CRUD and Neutron VIP/FIP attachment do not place a proxy in the path of ordinary calls to this Kubernetes Service. Core Service parents are ClusterIP Services; producer routing is the baseline. Explicit matching by raw ClusterIP and cross-namespace consumer routes are Extended features, so report them separately if tested. [ParentReference][shared-types] · [Mesh traffic rules][mesh-overview] · [Mesh feature registry][mesh-features] · [Mesh split test][mesh-split] |

For the stock gRPC smoke test, these are the relevant public JSON fields. Use the created load balancer and listener IDs and the correct Barbican reference. Create both TLS-enabled HTTP pools on that load balancer, register their members, then create one policy and path rule for each RPC. Read back the graph after provisioning settles.

| Resource | Request shape for the First RPC |
|---|---|
| Listener, `POST /v2/lbaas/listeners` | `{"listener":{"loadbalancer_id":"<lb>","protocol":"TERMINATED_HTTPS","protocol_port":8443,"default_tls_container_ref":"<Barbican reference>","alpn_protocols":["h2"]}}` |
| Pool, `POST /v2/lbaas/pools` | `{"pool":{"loadbalancer_id":"<lb>","protocol":"HTTP","lb_algorithm":"ROUND_ROBIN","tls_enabled":true,"alpn_protocols":["h2"]}}` |
| Policy, `POST /v2/lbaas/l7policies` | `{"l7policy":{"listener_id":"<listener>","action":"REDIRECT_TO_POOL","redirect_pool_id":"<pool-a>","position":1}}` |
| Rule, `POST /v2/lbaas/l7policies/{policy_id}/rules` | `{"rule":{"type":"PATH","compare_type":"EQUAL_TO","value":"/feasibility.v1.Echo/First"}}` |

The pool example shows routing and transport fields. Configure and validate backend server trust separately. This graph is a qualification specimen for a dedicated test listener. It omits the general Gateway API hostname matcher, error semantics and the rest of the profile. [Public operations][octavia-api] · [Tagged schema snapshot](stock-api-snapshot.txt)

## Architecture decisions available for qualification

**Direct stock mode.** Keep this mode constrained to validated model and provider capabilities. It is a candidate way to offer a documented subset, even when the entire profile is blocked. Test each stock solution through public Octavia operations. Use the Octavia status tree, configured graph, and protocol observations to distinguish API availability, successful provisioning and actual traffic. A rejected unsupported Route is a truthful controller outcome for a subset and leaves its conformance case unevaluated or failing.

**Bounded public Octavia extension.** For direct mode to grow beyond that subset, start with typed HTTP route mutations and response semantics in a provider-facing API. Mutations need add/set/remove and defined order relative to route matching and backend selection. Independent outcomes need a listener miss response, an ordered per-match fixed response, and a weighted target union of pool or response, with explicit code and size bounds and invalid-target shares preserved as backends change. Existing one-pool and redirect shapes can remain valid, with an explicit version and advertised provider capabilities for each new primitive. Validate names, values, order, numeric bounds, provider support and mutual exclusion before cloud mutation. Avoid custom templates, arbitrary HAProxy directives and metadata-as-config. If gRPC is a target, qualify the path subset first and add the necessary explicit transport and gRPC response semantics, including metadata/trailers, independent of protocol-neutral HTTP status. TLS SNI requires a separate typed TCP/TLS ClientHello match with bounded inspection, hostname precedence and route selection before forwarding. Check the installed image against a declared version floor, upgrades/failover and asynchronous reconciliation through the public API. These are extension boundaries for design review, with Amphora as the first possible implementation and explicit unsupported outcomes from OVN. [Tagged public API][octavia] · [Current provider abstraction][provider-base]

**Explicit mesh mode.** Attach standard Routes to the Kubernetes Service only when an operator has installed a mesh dataplane, explicitly selected the mesh implementation in configuration, and enrolled the intended namespaces or workloads using that implementation's mechanism. A Service ParentRef has no GatewayClass selector, so turning on an ingress GatewayClass does not opt a namespace or workload into mesh interception. Status and ownership must identify the mesh reconciler and avoid two writers claiming one parent; publish mesh conformance separately after proving actual in-cluster traffic and opt-in boundaries. Continue using the independent ingress class for Octavia VIPs if desired. A separately chosen L7 dataplane behind Octavia would also be an architecture decision for ingress, not a property of stock direct mode. [Standard mesh overview][mesh-overview] · [ParentReference][shared-types] · [Profile definitions][profiles]

### First public API change for ingress

The smallest proposed addition is an optional `request_header_operations` field on an L7 policy that forwards to a pool. The public request would set the ordered operations on the policy that owns the path predicates. For the first probe a single element would be `{"operation":"SET","name":"X-Tenant","value":"blue"}`. A GET would return the same typed fields, and a PUT with an empty list would remove the operations. Existing requests that omit the field retain their forwarding behavior. This is a proposed interface. The installed Hibiscus API has no such field. [Stock policy schema][policy] · [Header modifier contract][http-types]

At request time the provider would select the first matching policy using the original request, apply that policy’s operations once and forward to the selected pool. `SET` replaces all existing values of the case-insensitive name, `ADD` preserves existing values and adds the configured value, and `REMOVE` removes the field. A second path forwarding to the same pool must receive its original headers. The provider owns validation of HTTP field names and values, limit enforcement, persistence and rendering. Expose support for this operation in public provider capability discovery. The controller would require affirmative capability on the selected provider and reject an unrepresentable rule before creating forwarding resources. A generic API version alone cannot establish that a given provider implements the operation.

Acceptance for a prototype would require a real Octavia API request and readback, a healthy two-path echo test, cold worker restart and Amphora failover with the same observations. A provider that lacks the operation must return an explicit unsupported result. Public API types and validation, database migration, octavia-lib provider model, Amphora rendering and Gophercloud support would all need changes. A separate new action would be needed for default or weighted fixed responses. The local pilot already measures the stock route miss as `503`.

### First opt-in mesh experiment

The candidate for a separate experiment is Istio 1.31.0 ambient with a Service waypoint in the workload cluster. Pin the existing [studied revision](../reference-implementations.md) and the component image digests before any deployment. The operator would install istiod, the Istio CNI node agent, ztunnel and the waypoint. They would enroll two disposable client namespaces in ambient mode and assign only the test destination Service to the waypoint. That changes the path of traffic to the normal Service address. The external Amphora GatewayClass would retain its own controller name. [Istio waypoint configuration](https://istio.io/latest/docs/ambient/usage/waypoint/) · [Pinned waypoint tests](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/tests/integration/ambient/waypoint_test.go)

Before enrollment, both healthy clients must get response A via the ordinary Service DNS. After enrollment, a producer HTTPRoute attached to that Service selects B and adds the configured test header. Repeat with the GRPCRoute and record its backend ID and metadata. A neighboring Service supplies the isolation control. Collect route parent status from the mesh’s own `controllerName`. The Octavia writer must never claim the Service parent or allocate a VIP for this traffic. Removing the route must restore the original Service behavior before the operator removes enrollment and the waypoint. Consumer-route scoping, direct ClusterIP, XMesh status, interoperability with the deployed CNI and separate profile reports still need their own checks. The pinned Istio report covers an earlier release and cluster. [Service-parent contract][mesh-overview] · [Pinned HTTP/gRPC test](https://github.com/istio/istio/blob/eed52ebb66ade45214088877b0f82855d954060d/tests/integration/pilot/gateway_test.go#L860-L887)

Both proposals are under study. Implementing the public API extension or installing the mesh requires a separate operator decision.

## Reproduce local evidence

The source snapshot script uses Python's AST and reads the tagged public POST/PUT and graph-create classes and constant definitions. It also reads the Amphora protocol guard and v2 discovery. Use local shallow clones or other checkouts at the listed commits and run the following from this directory.

```sh
python3 inspect-stock-api.py /path/to/octavia /path/to/octavia-lib /path/to/ovn-octavia-provider
```

The exact command succeeded with the three official tag checkouts and produced [stock-api-snapshot.txt](stock-api-snapshot.txt). The seven specimen documents passed the following offline check against a Gateway API v1.7.0 checkout, with PyYAML and python-jsonschema installed.

```sh
python3 - /path/to/gateway-api qualification-routes.yaml <<'PYTHON'
from pathlib import Path
import sys
import jsonschema
import yaml
gateway, specimen = map(Path, sys.argv[1:])
for resource in yaml.safe_load_all(specimen.read_text()):
    kind = resource['kind']
    crd_path = gateway / 'config/crd/standard' / f'gateway.networking.k8s.io_{kind.lower()}s.yaml'
    versions = yaml.safe_load(crd_path.read_text())['spec']['versions']
    v1 = next(version for version in versions if version['name'] == 'v1' and version['served'])
    jsonschema.Draft7Validator(v1['schema']['openAPIV3Schema']).validate(resource)
    print(f"Pass: {kind}/{resource['metadata']['name']}")
PYTHON
```

The offline YAML check covers the structural fields. Kubernetes CEL, defaulting, attachment and controller status require the API server or controller. The local Octavia and HAProxy probes above report their wire results separately. Deployment, mesh traffic and full conformance require a designated cloud.

[hibiscus]: https://releases.openstack.org/hibiscus/
[gateway-release]: https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.7.0
[gateway]: https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152
[profiles]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/suite/profiles.go#L79-L191
[octavia]: https://github.com/openstack/octavia/tree/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/types
[octavia-lib]: https://github.com/openstack/octavia-lib/blob/bc03e1856b60517e1b75db4fdf9b3edf55f3a019/octavia_lib/common/constants.py
[ovn]: https://github.com/openstack/ovn-octavia-provider/tree/485b18e661d368feb82f87c499777052ee749458
[neutron]: https://github.com/openstack/neutron/tree/1ca1e2bb5b3c6b47913f25544d02bba20b198fac
[keystone]: https://github.com/openstack/keystone/tree/8755458dc5399d3ae7a9ae31f9f738da0427ac0d
[barbican]: https://github.com/openstack/barbican/tree/1efe16cd73b6e7d7c827aa8090665f2564de632e
[identity]: https://docs.openstack.org/api-ref/identity/v3/#authentication-and-token-management
[octavia-api]: https://docs.openstack.org/api-ref/load-balancer/v2/index.html
[api-versions]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/root_controller.py#L128-L160
[network-api]: https://docs.openstack.org/api-ref/network/v2/
[member-api]: https://docs.openstack.org/api-ref/load-balancer/v2/#create-member
[secrets]: https://docs.openstack.org/barbican/2026.2/api/reference/secrets.html
[secret-metadata]: https://docs.openstack.org/barbican/2026.2/api/reference/secret_metadata.html
[pkcs12]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/releasenotes/notes/Support-PKCS12-certificate-objects-1c6e896be9d35977.yaml
[policy]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/types/l7policy.py#L83-L129
[rule]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/types/l7rule.py#L57-L75
[policy-validate]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/validate.py#L292-L325
[amp-driver]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/drivers/amphora_driver/v2/driver.py#L390-L410
[amp-jinja]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/jinja/haproxy/combined_listeners/jinja_cfg.py
[amp-render]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/jinja/haproxy/combined_listeners/templates/macros.j2
[amp-headers]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/jinja/haproxy/combined_listeners/templates/macros.j2#L398-L451
[listener-validate]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/controllers/listener.py#L146-L171
[ovn-constants]: https://github.com/openstack/ovn-octavia-provider/blob/485b18e661d368feb82f87c499777052ee749458/ovn_octavia_provider/common/constants.py#L106-L116
[ovn-driver]: https://github.com/openstack/ovn-octavia-provider/blob/485b18e661d368feb82f87c499777052ee749458/ovn_octavia_provider/driver.py
[provider-base]: https://github.com/openstack/octavia-lib/blob/bc03e1856b60517e1b75db4fdf9b3edf55f3a019/octavia_lib/api/drivers/provider_base.py#L377-L446
[http-types]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/httproute_types.go
[headers-test]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-request-header-modifier.go
[empty-test]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-omitted-backendrefs.go
[invalid-test]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-invalid-nonexistent-backendref.go
[member-schema]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/types/member.py#L63-L84
[haproxy-backend]: https://docs.haproxy.org/3.0/configuration.html#4.2-use_backend
[haproxy-deny]: https://docs.haproxy.org/3.0/configuration.html#4.4-http-request%20deny
[grpc-types]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/grpcroute_types.go
[grpc-framing]: https://grpc.github.io/grpc/core/md_doc__p_r_o_t_o_c_o_l-_h_t_t_p2.html
[grpc-status]: https://grpc.github.io/grpc/core/md_doc_http-grpc-status-mapping.html
[grpc-test]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/grpcroute-exact-method-matching.go
[pool-alpn]: https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/releasenotes/notes/add-pool-tls-alpn-support-68cb94b828c9ba37.yaml
[tls-types]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/tlsroute_types.go
[tls-test]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/tlsroute-multiple-routes-attachment-same-hostname-intersection.go
[shared-types]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/shared_types.go
[mesh-overview]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/site/content/en/docs/mesh/mesh-overview.md#connecting-routes-and-services-
[mesh-features]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/pkg/features/mesh.go
[mesh-split]: https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/mesh/mesh-split.go
