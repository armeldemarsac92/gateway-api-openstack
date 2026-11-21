# HTTPRoute ParentRef selector status

The focused envtest runs the three original Gateway API v1.7.0 case bodies and their embedded manifests through `ConformanceTest.Run`. The shared base uses the upstream `same-namespace` Gateway and `infra-backend-v1` Service. The assertions use Kubernetes Route and Gateway status, so the base needs no running backend workloads. Each channel runs on its own Kubernetes 1.36.2 API server with the matching release CRDs and admission policy. [Upstream fixtures](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152/conformance) · [Envtest setup](../../../../internal/controller/envtest_upstream_status_test.go)

| Original case | Standard baseline | Standard | Experimental |
|---|---|---|---|
| `HTTPRouteInvalidParentRefNotMatchingListenerPort` | FAIL | PASS | PASS |
| `HTTPRouteInvalidParentRefNotMatchingSectionName` | PASS | PASS | PASS |
| `HTTPRouteInvalidParentRefSectionNameNotMatchingPort` | PASS | PASS | PASS |

The baseline controller reported `Accepted=False/NoMatchingParent` for the port mismatch, but left `ResolvedRefs=Unknown/Pending` even though the backend Service and port existed. The controller now evaluates supported backend references independently of selector matching. The case expects `ResolvedRefs=True/ResolvedRefs`, `Accepted=False/NoMatchingParent`, and zero attached Routes. [Port case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-invalid-parentref-not-matching-listener-port.go)

An additional controller regression covers two listeners where the section matches the first and the port matches the second. It checks the exact controller-owned ParentRef, current-generation `Accepted=False/NoMatchingParent` and `ResolvedRefs=False/BackendNotFound`, and absence of forwarding state. These runs cover status. Native programming of two HTTP listeners and full GATEWAY-HTTP profile qualification are separate work. [Regression](../../../../internal/controller/httproute/reconciler_test.go)
