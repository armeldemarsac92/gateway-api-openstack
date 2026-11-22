# Request filters on individual HTTPRoute backends

Tenants can set, add or remove request headers and rewrite the host or path for an individual weighted Service reference. An operator enables the weighted target and backend request filter options on the controller and the regional Amphora provider. The route first matches the original request. The selected backend receives its own transformations after the rule-level filters. [Operator configuration](../../../operator-octavia-http-actions.md#enable-request-filters-on-http-backend-references) · [Octavia distributions](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-target-request-filters-76828b5ac955) · [Provider library](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-url-rewrite-provider-3009042e8073)

## Local Gateway API v1.7.0 results

The entrypoint from the linux/amd64 OCI archive at `470e23cab5da` ran the original upstream cases against real Pods, Services and EndpointSlices in fresh Standard and Experimental kind clusters. The local Octavia REST API used the installed `19.0.1.dev37` wheel with octavia-lib `3.12.1.dev9`. Amphora rendered the rules used by HAProxy. Keystone, Neutron, RPC completion and the agent transport used fixtures. [Image, reports and replay](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-470e23cab5da6aba5dbdb313ba655c9bf13d00a5)

| Original case | Standard | Experimental |
|---|---|---|
| `HTTPRouteBackendRequestHeaderModifier` | 7/7 HTTP subtests | 7/7 HTTP subtests |
| `HTTPRouteRequestHeaderModifierBackendWeights` | 100/100 header-to-Pod checks | 100/100 header-to-Pod checks |
| `HTTPRouteBackendURLRewrite` | 2/2 HTTP subtests | 2/2 HTTP subtests |

The weighted test compares the received `Backend` header with the Pod that served each request. The rewrite case checks full-path replacement and prefix replacement at the backend, including preservation of the suffix. All three HTTPRoutes converged to `Accepted=True`, `ResolvedRefs=True` and the controller's `Programmed=True` for the observed generation on each channel. These last conditions were recorded after the original tests returned. The dev32 baseline stopped before traffic in all three cases. A region running Hibiscus still requires qualification. [Case reports](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-470e23cab5da6aba5dbdb313ba655c9bf13d00a5)

## Capability withdrawal

The regional API keeps the existing values readable after an operator withdraws the per-backend filter capability. It allows removal of a filter while preserving an unchanged filter on a second occurrence of the same pool, and rejects changes that would expand an existing transform to a new share of traffic. The controller closes the affected listener before it removes a filtered policy and verifies the replacement before reopening it. With regional capabilities available, an unresolved backend filter reference keeps its weight as a local HTTP 500. [Provider implementation and qualification](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-target-request-filters-76828b5ac955)

The Go graph regression also covers revocation of backend TLS trust on a second port. After Octavia confirms closure of the listener with incomplete filter inventory, the controller disables and drains the revoked pool. The healthy pool stays enabled. The controller restores the withdrawn backend after the CA is authorized and the filter inventory is complete. [Graph regression](https://github.com/armeldemarsac92/gateway-api-openstack/blob/470e23cab5da6aba5dbdb313ba655c9bf13d00a5/internal/cloud/openstack/graph/backend_tls_test.go)
