# Per-Service h2c backend pools

Start the controller with `--octavia-backend-h2c` and enroll the effective OpenStack
provider in Octavia's `[api_settings] enabled_backend_h2c_providers`. In the Service,
set `appProtocol: kubernetes.io/h2c` on the TCP port referenced by an HTTPRoute.
The pool keeps protocol `HTTP` and uses cleartext HTTP/2 for traffic to its
members. Ordinary Service ports on the same Gateway continue to use stock HTTP
pools. The existing listener and health monitor profiles still apply.

Before enabling h2c or reactivating an h2c pool, the controller reads
`GET /lbaas/providers/{driver}/gateway_capabilities` on the authenticated Octavia
endpoint. The driver must advertise `backend_h2c_v1`. An explicit negative
response for a requested h2c pool sets the affected route parent's
`ResolvedRefs=False` with reason `UnsupportedProtocol` and leaves
`Programmed=False`. If discovery fails, programming waits for another
observation.

When an already managed Service port changes protocol, the controller disables
the old members before updating the owned pool. Removing `appProtocol` from the
Service port clears an existing `backend_h2c: true` setting, including after
regional enrollment ends. The controller observes the new transport on the
active pool before restoring members. A TLS mismatch drains the affected pool.
Other pools on the Gateway can continue to reconcile.

## GRPCRoute matching

Enable `--octavia-fixed-http-responses` alongside `--octavia-backend-h2c`.
The HTTP listener uses the enrolled fixed HTTP 404 fallback for unmatched
paths. gRPC clients map that response to `UNIMPLEMENTED`. A rule can select one
method of a named gRPC service, all methods of that service, or all calls on its
attached hostnames. Within a hostname, a matching exact method takes precedence
over a service-wide match, then the default. Each policy requires a native
gRPC Content-Type, such as `application/grpc` or `application/grpc+proto`.
By default, the controller uses an Octavia `HEADER REGEX` rule compatible with
HAProxy's POSIX and PCRE engines. The rule matches the media type
case-insensitively, including optional parameters and a custom `+codec` suffix.
Supported codec characters are the HTTP
token characters except `#`, `&` and `'`, which the Octavia template cannot pass
to both regex engines. The template uses HAProxy's `req.hdr` fetch, which can
match a value within a malformed comma-separated Content-Type field. HTTP
requests with ordinary media types fall through to the listener's other
policies or its default response. The backend validates the gRPC protocol and
message framing. Each rule selects one Service backend.
A rule can have a unique name. Rule names do not change matching or precedence.
The selected TCP port must declare `appProtocol: kubernetes.io/h2c`. GRPCRoute pools use TCP health
checks with the GatewayClass timing and retry settings. Status and finalizers are
written to the GRPCRoute. The existing Gateway graph writer reconciles the
HTTP and gRPC routes together. When their effective hostnames overlap on a
listener, the oldest eligible Route wins, with namespace and name breaking
ties.

The local controller tests cover mixed h2c and stock HTTPRoute backends,
Octavia capability discovery, pool requests, and transport drift. The envtest
loads the original v1.7.0 `GRPCExactMethodMatching` manifest into a local
Kubernetes API server and observes two exact paths and h2c backends at the
Gateway writer. It also verifies Route status, overlapping HTTPRoute rejection,
and finalization. Its recording provider does not serve traffic.

See the [two-channel image qualification](opt-in-grpc-h2c.md) for native case transcripts, post-monitor-window RPC traffic and provider readbacks.

### Regional Content-Type rule

Add `--octavia-grpc-content-type` to use Octavia's `GRPC_CONTENT_TYPE`
rule for GRPCRoute policies. This option defaults to off. The existing gRPC
backend transport, default response and metadata settings still apply. The
native rule checks that the request has a single Content-Type field. It
matches `application/grpc` case-insensitively, including an optional HTTP
token after `+` and optional media type parameters. The Amphora rule accepts
quoted parameter values containing commas and excludes gRPC-Web.

Apply the Octavia database migration `4f1a36c90e8b` and upgrade the regional
API, workers and Amphora fleet. Validate the Amphora configuration with the
fleet's HAProxy build, then include the effective provider name in
`[api_settings] enabled_grpc_content_type_providers` on each regional API
replica. The controller reads `GET /lbaas/providers/{driver}/gateway_capabilities`
on its authenticated regional endpoint before creating or activating a
native policy and before writing a native rule. The provider on the load
balancer must advertise `grpc_content_type_v1`.

When changing modes, the controller disables the owned policy. Enabling native
matching converts the Content-Type rule in place. Reverting to portable
matching deletes the native rule and creates an owned header rule after
Octavia confirms the policy is disabled. This also works after regional
enrollment has been withdrawn. The controller restores the policy after
observing the new rule. The path, hostname and metadata rules retain their
IDs. Before interrupting a verified portable policy to adopt native matching,
the controller checks the regional capability. An inconclusive response
keeps the verified policy serving and delays migration. An explicit absence
of `grpc_content_type_v1` with the flag enabled withdraws the portable
policy and leaves the GRPCRoute at `Programmed=False`.

If Octavia explicitly withdraws the capability from a listener already using
it, the controller closes that listener after verifying Octavia ownership
and the current Gateway configuration. The closure can proceed while
Neutron's floating IP inventory is unavailable. All routes on the affected
port report progress while the listener is closed, including ordinary HTTP
routes. Routes on other ports continue independently. To revert to the
portable rule, remove the controller flag and let the graph converge before
rolling back Octavia.

The manager at `abcd5fd75914` passed all seven upstream request-header checks
on both CRD channels. Two additional h2c calls checked the metadata at the
echo backends. With the regional capability explicitly withdrawn, Octavia
reported the listener closed, a new call returned `UNAVAILABLE` and the
GRPCRoute reported `Programmed=False/Pending`. During withdrawal the
Neutron fixture returned HTTP 503 to three floating IP reads on each channel.
Disabling the controller option restored both backend calls and all three
positive route conditions. The two forwarding policies and two non-media
rules kept their IDs, and two native media rules were replaced.

Both runs used the binary extracted from the same OCI image, Kubernetes
envtest, Octavia REST, Amphora and HAProxy. The previous manager
`702f18dc1ba8` had passed the registered case on both channels and timed out
waiting for the withdrawn route status. The release includes both revisions'
receipts and the replay inputs. [Image and per-channel results](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-abcd5fd75914f043df9ae000aa92664c481ba74a)

## Modify request metadata on a gRPC rule

Enable the controller's `--octavia-request-header-modifier` option together with the gRPC h2c and fixed-response options. The selected Amphora region must advertise `request_header_operations_v1`. Place a `RequestHeaderModifier` filter in the GRPCRoute rule. The selected method's backend receives its set, add and remove operations. Rules can share a Service and use different operations. If equally specific rules match the same call, the first rule's filter applies.

On both v1.7.0 bundles, the executable from image revision `2944c353dcb0afa7b8756373bf87a51001c33ad2` passed the seven original `GRPCRouteRequestHeaderModifier` subtests. Two additional RPCs read the headers reported by the echo backends. One method overwrote an existing value and preserved an unrelated header. Another appended a second value, removed the selected key and reached its own backend. On each channel the route parent eventually reported `Accepted=True`, `ResolvedRefs=True` and the controller's `Programmed=True`. The local run used envtest, Octavia REST, the Amphora renderer, native HAProxy and gRPC echo processes. Keystone, Neutron and worker completion used local fixtures. [Per-channel results](grpc-metadata-and-hostname-results.json) · [Image and replay](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-2944c353dcb0afa7b8756373bf87a51001c33ad2)

## Mirror gRPC requests

An operator can enable gRPC mirroring for an Amphora region. A GRPCRoute can send a copy of a matching RPC to another Service. A mirror on a weighted backend receives a copy only when that backend is selected. A denied status from the mirror leaves the primary response intact. [Operator settings](../../../getting-started.md) · [Octavia packages and native streaming results](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-grpc-request-mirror-31a05857f3b3)

The local Standard and Experimental runs each sent six rule-matched calls, 144 calls to a weighted method and six unfiltered calls per phase. Removing the mirror Service’s ReferenceGrant stopped its copies while both primary backends kept serving. Restoring the grant resumed the copies. Removing the filters stopped them again. Every client received its primary reply with gRPC status OK. [Calls, copies and route conditions by channel](grpc-request-mirror-evidence/receipt.json)

## Modify request metadata on a selected gRPC backend

A request header modifier on a GRPCRoute rule applies to matching calls. A modifier on a backend reference applies after the rule's operations and only to the selected occurrence. An enrolled Amphora sends comma-separated binary values as individual HTTP/2 metadata fields. [Operator settings](../../../getting-started.md) · [Octavia packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-grpc-request-binary-metadata-2972069e0a3c)

The local Standard and Experimental runs sent 144 RPCs to the weighted method and 144 to an unfiltered control in each phase. The route kept its UID, generation and 3:1 weights across withdrawal and restoration of the cross-namespace ReferenceGrant. With the grant removed, the route reported `Accepted=True` and `ResolvedRefs=False` with `RefNotPermitted`. The protected backend received zero calls and its share returned `UNAVAILABLE`. After restoration, `ResolvedRefs=True` and the protected backend again received its binary metadata. Removing the modifiers restored the incoming metadata on both backends. The two local grpcio servers were reached through Kubernetes Services and EndpointSlices. [Per-channel counts, metadata and conditions](grpc-request-metadata-evidence/receipt.json)

## Modify response metadata on a gRPC rule

A rule-level `ResponseHeaderModifier` sets, adds and removes application metadata on calls that match the rule. With the regional native gRPC matcher, binary lists in a SET or ADD produce separate HTTP/2 metadata fields. The method retains the status and details returned by the service. The provider enforces a limit of 48 SET and ADD fields after expanding binary lists. [Operator settings](../../../getting-started.md) · [Octavia release](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-grpc-binary-response-metadata-76f937d37f4a)

On local kind with the Standard and Experimental bundles, two methods shared one h2c Service. The filtered method returned three binary values after ADD and two after SET. Removing the filter restored the original values. The control method kept the origin metadata in both generations. A late error kept its separate trailers and INVALID_ARGUMENT status. On an early PERMISSION_DENIED response, the client observed the selected metadata in the trailers-only response. Both route generations reported Accepted, ResolvedRefs and Programmed. The EndpointSlice pointed to a local grpcio server. Keystone, Neutron, worker completion and Amphora transport were simulated. [Calls, route conditions and Octavia operations](grpc-response-metadata-evidence/receipt.json)

## Default and service-only matches

A GRPCRoute can select a fully qualified service, an exact service and method, or all gRPC calls. A service-only match respects the service name boundary. The gRPC policies check the request media type before routing. The local graph tests also exercised ordinary HTTP requests. [Controller source](https://github.com/armeldemarsac92/gateway-api-openstack/tree/2944c353dcb0afa7b8756373bf87a51001c33ad2)

On the earlier local kind build `d5bc7e000c27c6c3a9296aa46b6ef6ef1f9ec2b3`, both channels passed the two requests in `GRPCRouteMultipleRoutesAttachmentSameHostnameIntersection`. Two other original cases failed on both channels. `GRPCRouteListenerHostnameMatching` passed 0 of 8 subtests. Its Gateway rejected the four shared HTTP listeners with `Accepted=False/ListenersNotValid`. `GRPCRouteMultipleGateways` passed 0 of 6. The two dedicated routes were programmed, while the shared route reported `Accepted=False/UnsupportedValue` on both parents. The results identify the earlier build and its provider wheel digests separately. [Kind observations](grpc-metadata-and-hostname-results.json)

## Portable gRPC media type matching

The earlier manager `2944c353dcb0` returned HTTP 404 for both calls in the original `GRPCRouteMultipleRoutesAttachmentSameHostnameIntersection` case on each channel with the libc-regex HAProxy binary. The four GRPCRoutes reported `Accepted=True`, `ResolvedRefs=True` and `Programmed=True`. HAProxy rejected the inline PCRE option in the generated ACL. The two HAProxy builds both reported version 3.0.19.

The manager extracted from image revision `a1807b022ba8` passed both calls with the same kind fixture, Octavia wheels and libc-regex HAProxy on each channel. With PCRE2 HAProxy and envtest it also passed the seven original request metadata checks and both supplementary backend RPCs on each channel. Separate Octavia probes recorded 16 HTTP outcomes per regex engine, including the punctuation and comma-handling limits described above. The image manifest, binary digests, original reports and both sets of receipts are available in the [qualification release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-a1807b022ba8bdd06a7dca3990cb0b861711ecdd).
