# Local qualification of named HTTPRoute and GRPCRoute rules

The original Gateway API v1.7.0 Go cases ran with their unmodified route manifests. The two cases use a Gateway parent and a mix of named and unnamed rules on the same Route. The manager came from the [recorded linux/amd64 OCI image](../image.json) and reported source revision `adb6f0fc44dee7d9016bca79442e68f94eb6e15e` at startup. [Pinned upstream tests](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests)

| Upstream case | Standard | Experimental | Observed backend selection |
| --- | --- | --- | --- |
| `HTTPRouteNamedRule` | PASS | PASS | HTTP 200 from `infra-backend-v1` for the named `/named` rule and from `infra-backend-v2` for the unnamed `/unnamed` rule. |
| `GRPCRouteNamedRule` | PASS | PASS | `Echo` reached `grpc-infra-backend-v1` through the named rule, and `EchoTwo` reached `grpc-infra-backend-v2` through the unnamed rule. |

The first Standard attempt on this image failed `HTTPRouteNamedRule`. The HTTP transcript recorded 480 HTTP 404 status lines and no HTTP 200 before the upstream deadline. The subsequent gRPC case passed. A SQLite error ended that run before the combined receipt was written. The final fixture sent its provider observations through the same HTTP service as the controller. Both channels then passed with the original upstream timeouts. The cause of the first HTTP failure is undetermined. [First Standard HTTP transcript](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-adb6f0fc44dee7d9016bca79442e68f94eb6e15e/named-route-standard-first-http-transcript.log.gz)

Both channels returned current-generation `Accepted=True`, `ResolvedRefs=True` and controller `Programmed=True` conditions for each route. The API kept `named-rule` on the first rule and no name on the second. The Gateway listener reported one attached route in each case. No HTTPRoute shadow existed for the GRPCRoute.

The supplemental HTTP calls identified the two expected backends at the Gateway VIP. Octavia exposed two active non-TLS HTTP pools for each case. Only the gRPC pools enabled h2c, and HAProxy rendered `proto h2` on all four gRPC member directives. The supplemental RPCs repeated both backend selections after six seconds on the same HAProxy, with both TCP monitors still active. HAProxy returned HTTP 404 for the unmatched `EchoThree` and `echoTwo` methods, which the gRPC client mapped to `UNIMPLEMENTED`. [Standard receipt](named-route-rules-evidence/standard.json) · [Experimental receipt](named-route-rules-evidence/experimental.json)

The local environment used Kubernetes 1.36.2 and etcd, Octavia's WSGI API and Amphora renderer with SQLite, HAProxy 3.0.19 and the pinned upstream echo-basic executable. The fixture supplied Keystone, Neutron, worker completion, Amphora agent transport and Kubernetes endpoint and readiness state. The run covers the two Gateway-parent cases. Live OpenStack and Magnum deployment, full Gateway and Mesh profiles, and rule-targeted policies remain unqualified.

The source-only replay archive `hibiscus-named-route-rules-adb6f0fc44de.tar.gz` has SHA-256 `fc2ef9d848741156b45d14f6c87649c86e80364993c6aa3016ce5ba524311e91`. Its README explains how to replay both channels with the accompanying OCI archive and pinned inputs.
