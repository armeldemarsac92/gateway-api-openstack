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

The regional option has local controller coverage for discovery, API
payloads, mode changes and capability withdrawal. The native traffic
qualification remains pending.

## Modify request metadata on a gRPC rule

Enable the controller's `--octavia-request-header-modifier` option together with the gRPC h2c and fixed-response options. The selected Amphora region must advertise `request_header_operations_v1`. Place a `RequestHeaderModifier` filter in the GRPCRoute rule. The selected method's backend receives its set, add and remove operations. Rules can share a Service and use different operations. If equally specific rules match the same call, the first rule's filter applies.

On both v1.7.0 bundles, the executable from image revision `2944c353dcb0afa7b8756373bf87a51001c33ad2` passed the seven original `GRPCRouteRequestHeaderModifier` subtests. Two additional RPCs read the headers reported by the echo backends. One method overwrote an existing value and preserved an unrelated header. Another appended a second value, removed the selected key and reached its own backend. On each channel the route parent eventually reported `Accepted=True`, `ResolvedRefs=True` and the controller's `Programmed=True`. The local run used envtest, Octavia REST, the Amphora renderer, native HAProxy and gRPC echo processes. Keystone, Neutron and worker completion used local fixtures. [Per-channel results](grpc-metadata-and-hostname-results.json) · [Image and replay](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-2944c353dcb0afa7b8756373bf87a51001c33ad2)

## Default and service-only matches

A GRPCRoute can select a fully qualified service, an exact service and method, or all gRPC calls. A service-only match respects the service name boundary. The gRPC policies check the request media type before routing. The local graph tests also exercised ordinary HTTP requests. [Controller source](https://github.com/armeldemarsac92/gateway-api-openstack/tree/2944c353dcb0afa7b8756373bf87a51001c33ad2)

On the earlier local kind build `d5bc7e000c27c6c3a9296aa46b6ef6ef1f9ec2b3`, both channels passed the two requests in `GRPCRouteMultipleRoutesAttachmentSameHostnameIntersection`. Two other original cases failed on both channels. `GRPCRouteListenerHostnameMatching` passed 0 of 8 subtests. Its Gateway rejected the four shared HTTP listeners with `Accepted=False/ListenersNotValid`. `GRPCRouteMultipleGateways` passed 0 of 6. The two dedicated routes were programmed, while the shared route reported `Accepted=False/UnsupportedValue` on both parents. The results identify the earlier build and its provider wheel digests separately. [Kind observations](grpc-metadata-and-hostname-results.json)

## Portable gRPC media type matching

The earlier manager `2944c353dcb0` returned HTTP 404 for both calls in the original `GRPCRouteMultipleRoutesAttachmentSameHostnameIntersection` case on each channel with the libc-regex HAProxy binary. The four GRPCRoutes reported `Accepted=True`, `ResolvedRefs=True` and `Programmed=True`. HAProxy rejected the inline PCRE option in the generated ACL. The two HAProxy builds both reported version 3.0.19.

The manager extracted from image revision `a1807b022ba8` passed both calls with the same kind fixture, Octavia wheels and libc-regex HAProxy on each channel. With PCRE2 HAProxy and envtest it also passed the seven original request metadata checks and both supplementary backend RPCs on each channel. Separate Octavia probes recorded 16 HTTP outcomes per regex engine, including the punctuation and comma-handling limits described above. The image manifest, binary digests, original reports and both sets of receipts are available in the [qualification release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-a1807b022ba8bdd06a7dca3990cb0b861711ecdd).
