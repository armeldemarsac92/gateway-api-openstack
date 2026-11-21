# Per-Service h2c backend pools

Start the controller with `--octavia-backend-h2c` and enroll the effective OpenStack
provider in Octavia's `[api_settings] enabled_backend_h2c_providers`. In the Service,
set `appProtocol: kubernetes.io/h2c` on the TCP port referenced by an HTTPRoute.
The pool keeps protocol `HTTP` and uses cleartext HTTP/2 for traffic to its
members. Ordinary Service ports on the same Gateway continue to use stock HTTP
pools. The existing listener and health monitor profiles still apply.

Before a pool operation sends `backend_h2c`, the controller reads
`GET /lbaas/providers/{driver}/gateway_capabilities` on the authenticated Octavia
endpoint. The driver must advertise `backend_h2c_v1`. An explicit negative
response for a requested h2c pool sets the affected route parent's
`ResolvedRefs=False` with reason `UnsupportedProtocol` and leaves
`Programmed=False`. If discovery fails, programming waits for another
observation.

When an already managed Service port changes protocol, the controller disables
the old members before updating the owned pool. Removing the annotation sends
`backend_h2c: false` if the existing pool still has h2c enabled. The controller
observes the pool change before restoring members. The predicate stays in place
during the transition.

The local tests cover lowering an HTTPRoute with both h2c and stock Service
ports, effective provider capability discovery, pool requests, and transport
drift. The upstream v1.7.0 cases are `HTTPRouteBackendProtocolH2C` and
`GRPCExactMethodMatching`. The latter attaches a real GRPCRoute to an HTTP
Gateway, checks two distinct exact RPC backends, and expects `Unimplemented`
for an unmatched RPC. These controller transport tests cover the pool leg.
