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
gRPC Content-Type, such as `application/grpc` or `application/grpc+proto`,
using an Octavia `HEADER` rule. HTTP requests with ordinary media types fall
through to the listener's other policies or its default response. The
Content-Type rule classifies requests by media type. The backend validates
the gRPC protocol and message framing. Each rule selects one Service backend.
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


## Modify request metadata on a gRPC rule

Enable the controller's `--octavia-request-header-modifier` option together with the gRPC h2c and fixed-response options. The selected Amphora region must advertise `request_header_operations_v1`. Place a `RequestHeaderModifier` filter in the GRPCRoute rule. The selected method's backend receives its set, add and remove operations. Rules can share a Service and use different operations. If equally specific rules match the same call, the first rule's filter applies.
