# Weighted Service backends on a passthrough TLSRoute

A local Gateway served two TLS names on one listener. The first TLSRoute gave weight 3 to its primary Service and weight 1 to a Service in another namespace. A second TLSRoute sent the sibling name to its own Service. Each backend returned its identity and the SNI received from the client. The client verified the hostname, certificate chain and backend leaf fingerprint on every successful connection. [Route conditions, pool weights and TLS connection counts](tlsroute-weighted-evidence/receipt.json) · [Gateway API v1.7.0 backend weight and rejection contract](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/tlsroute_types.go#L149-L168)

| Channel | Minority Service | Primary | Minority | Rejected | Sibling |
|---|---|---:|---:|---:|---:|
| Standard | Initially authorized | 600 | 200 | 0 | 80 |
| Standard | Grant withdrawn | 609 | 0 | 191 | 80 |
| Standard | Grant restored | 600 | 200 | 0 | 80 |
| Standard | No ready endpoints | 598 | 0 | 202 | 80 |
| Standard | Endpoints restored | 600 | 200 | 0 | 80 |
| Experimental | Initially authorized | 600 | 200 | 0 | 80 |
| Experimental | Grant withdrawn | 583 | 0 | 217 | 80 |
| Experimental | Grant restored | 600 | 200 | 0 | 80 |
| Experimental | No ready endpoints | 607 | 0 | 193 | 80 |
| Experimental | Endpoints restored | 600 | 200 | 0 | 80 |

Each row counts 800 fresh connections to the weighted name and 80 to its sibling after reconciliation. Standard started with an empty load-balancer inventory, the SNI option enabled, and proportional rejection disabled in the controller and region. The subsequent Standard phases and all Experimental phases used proportional rejection. Both TLSRoutes kept the same UID, generation and spec throughout their run. Both stayed accepted and the controller reported them programmed after the pools and SNI policies converged. With the grant withdrawn, the weighted route reported `ResolvedRefs=False` with `RefNotPermitted`. Restoring the grant returned it to `ResolvedRefs=True`. Removing the minority Service's endpoints kept its reference resolved. In both withdrawal phases, the minority Pod accepted zero connections from the Gateway and answered a direct check.

The local kind cluster ran the Kubernetes API, EndpointSlice controller and TLS applications. The Gateway controller was extracted from the qualified linux/amd64 OCI image. The public Octavia API, TaskFlow member-batch worker, Amphora renderer and HAProxy served the test traffic with simulated cloud inventory, ordinary provisioning, RPC and Amphora agent transport. Weighted backend re-encryption and trials in a deployed Hibiscus region remain open. [Operator setup](../../../operator-octavia-tls-sni-passthrough.md#distribute-connections-among-services) · [Regional trial ledger](../live-runs.md)

Run the controller and provider checks for this split from the repository root. [Kubernetes resolution and route status](../../../../internal/controller/gateway/gateway_sni_weighted_test.go) · [Octavia staging, withdrawal and recovery](../../../../internal/cloud/openstack/graph/tls_sni_weighted_test.go)

```sh
go test ./internal/controller/gateway ./internal/cloud/openstack/graph -run 'TestWeightedSNIRoute|TestSNIWeighted|TestSNIPartialBatch|TestSNIFailedDetachedBatch'
```
