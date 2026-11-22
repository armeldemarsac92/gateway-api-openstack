# Local qualification of Amphora h2c and exact GRPCRoute methods

A controller with `--octavia-backend-h2c` forwards a Service port declaring `appProtocol: kubernetes.io/h2c` through an Amphora pool that uses cleartext HTTP/2 to its members. With `--octavia-fixed-http-responses` also enabled, it accepts exact service-and-method GRPCRoute rules on an HTTP listener. Octavia must advertise the corresponding opt-in capabilities for the configured Amphora provider. The controller and provider source revisions are recorded with the receipts. [Controller image identity](https://github.com/armeldemarsac92/gateway-api-openstack/blob/82c11958e1e64b681e7cf6ad5d89a7dd938c4983/docs/qualification/v1.7.0/image.json) · [Standard receipt](grpc-h2c-evidence/standard.json) · [Experimental receipt](grpc-h2c-evidence/experimental.json)

The Hibiscus wheels for the qualified provider pair match the local builds. [Octavia wheel release](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-backend-h2c-bdcd6e3de928) · [octavia-lib wheel release](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-backend-h2c-bb1a993ac954)

## Weighted GRPCRoute backends

On an HTTP listener, `--octavia-weighted-http-targets` adds support for rules with multiple Service references to the gRPC h2c profile. The configured Octavia region must advertise `weighted_http_targets_v1`. Each forwarding Service uses its own h2c pool. A reference with omitted weight has weight 1. A zero-weight reference is still validated, including any incompatible BackendTLSPolicy, and receives no traffic. A rule with zero total weight sends its matching RPCs to the empty pool and leaves other rules active. Filters on individual references are unsupported.

An invalid positive-weight reference in a weighted rule sets the parent’s `ResolvedRefs` to `False`. Before activating weighted rules, the controller prepares an empty pool owned by the route. It can move the revoked share to that pool in place, where HTTP 503 maps to gRPC `UNAVAILABLE`. On restoration, the share stays on the empty pool until the authorized pool is ready. If an existing weighted policy predates the empty pool or the pool is unavailable at withdrawal, the controller closes the affected policy until it can safely configure the target again.

The original Gateway API v1.7.0 `GRPCRouteWeight` case passed on Standard and Experimental with the entrypoint from the linux/amd64 image at `f70be140741c`. The default upstream gRPC client checked the 70 / 30 / 0 distribution. The earlier controller at `82274f3036f4` reported `Accepted=False/UnsupportedValue` on Standard. [Upstream case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/grpcroute-weight.go) · [Image, checksums and per-channel results](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-f70be140741c0b15fe4b0da6b440661fc4641ef2)

| Supplemental sample of 500 RPCs | Standard, v1 / v2 / v3 / unavailable | Experimental, v1 / v2 / v3 / unavailable |
| --- | --- | --- |
| Original 70 / 30 / 0 rule | 342 / 158 / 0 / 0 | 349 / 151 / 0 / 0 |
| Cross-namespace reference authorized | 341 / 159 / 0 / 0 | 350 / 150 / 0 / 0 |
| ReferenceGrant withdrawn | 341 / 0 / 0 / 159 | 337 / 0 / 0 / 163 |
| ReferenceGrant restored | 348 / 152 / 0 / 0 | 343 / 157 / 0 / 0 |

Each supplemental sample started after the route status and Octavia readback converged. The 30-weight reference pointed to the Service protected by the grant. The route stayed accepted. Its `ResolvedRefs` condition reported `False/RefNotPermitted` on withdrawal and returned to `True/ResolvedRefs` on restoration. In both channels, the separate all-zero rule returned `UNAVAILABLE` on 500 of 500 RPCs while its healthy sibling continued forwarding. [Status and traffic by transition](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-f70be140741c0b15fe4b0da6b440661fc4641ef2)

The runs used Kubernetes v1.36.2 envtest, Octavia 19.0.1.dev32, octavia-lib 3.12.1.dev9, HAProxy 3.0.19 and six live echo processes. The fixture supplied project identity, networking, worker completion, agent transport and Kubernetes Pod readiness. Calls in flight at grant withdrawal and a deployed Hibiscus region require separate measurements. [Provider wheels](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-url-rewrite-provider-ebc2e6f04562) · [Provider library](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-url-rewrite-provider-3009042e8073)

## Gateway API v1.7.0 cases

Both channels ran the original upstream Go cases and applied their unmodified route manifests. The adapter selected one case at a time with upstream conformance options. The manager came from the qualified linux/amd64 OCI archive. It reported revision `e6fbd1a853aceaed3a141b406d5860f5242b9ecb` at startup. The registry manifest was `sha256:d9e0126c2d7f46fa919fb99a8bba36c54c6570b6f7e05609d177e944ebea7c60`. [Upstream case sources](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests)

| Unchanged upstream case | Standard | Experimental |
| --- | --- | --- |
| `HTTPRouteBackendProtocolH2C` | Exit 0, HTTP/2 prior-knowledge subtest passed | Exit 0, HTTP/2 prior-knowledge subtest passed |
| `GRPCExactMethodMatching` | Exit 0, three RPC subtests passed | Exit 0, three RPC subtests passed |

The HTTPRoute case observed an HTTP/2 200 from the original echo server. The GRPCRoute case sent `Echo` to the v1 Service, `EchoTwo` to the v2 Service and received `UNIMPLEMENTED` for `EchoThree`. After the persisted TCP health-check window elapsed, a second gRPC client observed the same backend selection and `UNIMPLEMENTED` on the case variant `echoTwo`. Both unmatched calls mapped HAProxy's HTTP 404 to the gRPC status. The two active TCP monitors stayed on the same HAProxy process and configuration. Standard observed 6.000 s with 2 s intervals, a 1 s timeout and 3 failed checks required. Experimental observed 6.000 s with 2 s intervals, a 1 s timeout and 3 failed checks required. The controller wrote current-generation `Accepted=True`, `ResolvedRefs=True` and its own `Programmed=True` condition, with one attached route on the listener. Octavia reported one ACTIVE non-TLS HTTP pool with `backend_h2c=true` for the HTTPRoute and two for the GRPCRoute. The active generated HAProxy configuration placed `proto h2` on the backend server directives. [Standard HTTP transcript](grpc-h2c-evidence/standard-httproute-backend-protocol-h2c.log) · [Standard gRPC transcript](grpc-h2c-evidence/standard-grpcroute-exact-method-matching.log) · [Experimental HTTP transcript](grpc-h2c-evidence/experimental-httproute-backend-protocol-h2c.log) · [Experimental gRPC transcript](grpc-h2c-evidence/experimental-grpcroute-exact-method-matching.log)

Review of the earlier `3cfa135` build reproduced a late matched-RPC 503 even though both official cases passed on both channels. The final source uses TCP monitoring for these gRPC pools. The replay archive preserves the earlier upstream results.

Kubernetes v1.36.2, etcd, Octavia's WSGI API, the Amphora provider and renderer, HAProxy 3.0.19, and the pinned Gateway API echo-basic processes ran in disposable local namespaces. The fixture supplied Keystone, Neutron, worker completion and Amphora agent transport. Local DB migration checks on MariaDB 11.4.13 preserved a legacy pool and demonstrated a locked transport read on the Hibiscus and master-based provider pairs. Live OpenStack, Amphora fleet upgrade or failover, Kubernetes cluster deployment and a full Gateway or Mesh conformance profile remain unqualified. [Hibiscus migration receipt](grpc-h2c-evidence/mariadb-hibiscus.json) · [Master-based migration receipt](grpc-h2c-evidence/mariadb-master.json)

The local replay archive `hibiscus-grpc-h2c-e6fbd1a853ac.tar.gz` has SHA-256 `01d956f817211bd7745fdd4a8bc929be7a79a578484b2d1eaaf7aba2da7a7110`. It contains the harness, pinned source and image metadata, four provider diffs, original-case transcripts filtered to remove environment details and readback receipts.

## HTTPRoute ReferenceGrant regression

The manager from this image retained the route’s 3:1 weights through grant removal and restoration on both local channels. Removing the grant produced zero calls to the protected Service in 2,048 requests per channel. Restoring the grant returned HTTP 200 from that Service for 494 Standard requests and 521 Experimental requests. [Observed route conditions and response counts](weighted-referencegrant-e6fbd1a/validation-referencegrant.md)
