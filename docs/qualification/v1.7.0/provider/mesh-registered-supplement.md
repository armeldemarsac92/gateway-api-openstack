# Registered mesh cases outside the two profile tables

Gateway API v1.7.0 registers 23 Service-parent tests. The Core and Extended features listed under MESH-HTTP and MESH-GRPC enable 16 unique cases on each channel. Seven further registrations reference generic HTTPRoute features outside that union. The supplemental run selected those seven test bodies on the same disposable cluster after the formal fixture drained. Each channel used its own genuine release bundle. [Formal results](../complete-local-profiles.md) · [Case registrations](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/mesh)

| Additional upstream case | Standard | Experimental |
|---|---|---|
| `MeshConsumerRoute` | FAIL | FAIL |
| `MeshFrontend` | FAIL | FAIL |
| `MeshFrontendHostname` | FAIL | FAIL |
| `MeshHTTPRoute303Redirect` | FAIL | FAIL |
| `MeshHTTPRoute307Redirect` | FAIL | FAIL |
| `MeshHTTPRoute308Redirect` | FAIL | FAIL |
| `MeshPorts` | FAIL | FAIL |

The echo pods used ordinary Kubernetes Services. On both channels the upstream shared mesh setup completed. The controller recorded zero Gateway graph operations. A passing upstream assertion on this fixture can result from Service load balancing. No enrolled mesh dataplane served these requests. Both the formal upstream reports and the separate seven-case receipts are included with the runner and bundle hashes. [Download the receipts](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-719490a9b1fa73b0d80a0c5512c2c8107b16a235/hibiscus-local-profiles-719490a9b1fa.tar.gz)

## Reproduce the supplement

Build the checked-in runner using Go 1.27.4 with the `envtest,kindtest` tags. Use the disposable two-node Linux kind setup and the exact image pins in the formal results. Set `GATEWAY_API_CHANNEL`, `KIND_CLUSTER_NAME` and `HIBISCUS_TEST_KUBECONFIG` to the selected test cluster. On each node, the advanced v0.1.0 echo image must be available through its original registry reference. Use the formal `all` mode separately for the original upstream profile reports. [Measured runner source](https://github.com/armeldemarsac92/gateway-api-openstack/tree/20d0fd72329ec26eade4990688a8d2220f80cde5) · [Official suite](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152/conformance)

```sh
cases='MeshConsumerRoute|MeshFrontend|MeshFrontendHostname|MeshHTTPRoute303Redirect|MeshHTTPRoute307Redirect|MeshHTTPRoute308Redirect|MeshPorts'
HIBISCUS_MESH_STUDY=registered go test -count=1 -v -tags envtest,kindtest \
  -timeout=50m -run "^TestKindStockMeshStudy$/^($cases)$" ./internal/controller
```
