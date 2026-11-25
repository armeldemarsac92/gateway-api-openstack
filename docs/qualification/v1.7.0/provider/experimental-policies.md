# Experimental policies and XMesh

At controller revision `ce6f51797fb7`, the genuine v1.7.0 Experimental bundle admitted the three resources below. The Kubernetes 1.36.2 API server used the release's safe-upgrades admission policy. The manager owned the Gateway and observed an independent listener change from port 80 to 81. It reported `Programmed=True` at the new generation, confirming that its Gateway watch was active. [Full sanitized observation](experimental-policy-results.json)

| Resource | Selection | Stored status |
|---|---|---|
| XBackendTrafficPolicy | A Service in the same namespace with a retry constraint | No policy status |
| XMesh | The running manager's controller name | `Accepted=Unknown/Pending` |
| XTelemetryPolicy | The managed Gateway with tracing disabled | No policy status |

Two explicitly requested Gateway reconciliations produced identical desired graphs before and after admission. That snapshot recorded API admission only. Later trials below record Service retry budgets, XMesh readiness and trace export to a collector Pod. [Retry budget API](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apisx/v1alpha1/xbackendtrafficpolicy_types.go#L61-L125) · [XMesh status](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apisx/v1alpha1/xmesh_types.go#L56-L126) · [Telemetry targets](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apisx/v1alpha1/xtelemetrypolicy_types.go#L90-L155)

The source archive includes the observer patch, the original-log checksum and the command for running it against the pinned release and envtest assets. The earlier XBackend EndpointSelector observation records the fourth Experimental kind. [Reproduction files](stock-tcp.md#reproduce-the-multiple-listener-probes) · [XBackend and its HTTPRoute](xbackend-endpoint-selector.md)

## Service retry budget

In the local Experimental run, the controller reported the policy accepted on its Gateway ancestor at the current generation. The patched HAProxy returned HTTP 503 with one backend attempt when Service A exhausted its budget. Service B continued to return the backend's HTTP 500 after two attempts. Deleting the policy cleared the budget on A's Octavia pool and restored its two attempts. In the Standard run the API omitted the policy CRD and both routes kept their ordinary retries. The receipt covers a targeted local HTTP lifecycle using kind, published provider wheels and real HAProxy with cloud fixtures. [Recorded results and artifact digests](backend-retry-budget-evidence/receipt.json) · [Enablement and process limits](../../../operator-octavia-backend-retry-budget.md)

## XMesh readiness on Istio ambient

The controller ran from its reproducible release image on Kubernetes v1.36.4 with the Gateway API v1.7.0 Experimental bundle. Istio 1.31.0 supplied the pilot, CNI and ztunnel. Two manager replicas reported the installation’s XMesh. A separate Pod provided the local OpenStack API. [Operator configuration](../../../../integrations/istio-ambient/README.md)

| Observed configuration | Accepted | Programmed | Reported features |
|---|---|---|---:|
| Istio ready with the Helm CNI settings | True / Accepted | True / Programmed | 16 |
| istiod scaled to zero | True / Accepted | Unknown / Pending | 0 |
| istiod restored | True / Accepted | True / Programmed | 16 |
| CNI Pods read ambient mode from a ConfigMap | True / Accepted | Unknown / Pending | 0 |
| ConfigMap corrected, same CNI Pods | True / Accepted | Unknown / Pending | 0 |
| CNI Pods updated with literal ambient settings | True / Accepted | True / Programmed | 16 |

The XMesh kept its UID and generation across all six observations. Editing the ConfigMap preserved the CNI Pod UIDs and the DaemonSet generation. The controller restored the original 16 features after both CNI Pods rolled out with the explicit settings. The upstream HTTP path rewrite and gRPC request-header modification cases both passed on the waypoint. The Gateway API v1.7.0 registry has no dedicated XMesh status case. [Conditions, CNI settings and image digests](xmesh-status.json)

## Gateway request tracing

An Experimental XTelemetryPolicy targeted one of two Gateways. Its collector ran as a Pod in a separate namespace and a ReferenceGrant authorized its Service. On the target Gateway, the application received the child span ID and the collector recorded the same ID, the supplied parent, the configured service and span names, and HTTP 200.

| Grant for the collector Service | Selected policy ancestor | Request span observed | Both applications |
|---|---|---|---|
| Present | `Accepted=True`, `ResolvedRefs=True` | Matching SERVER span | HTTP 200 |
| Withdrawn | `Accepted=True`, `ResolvedRefs=False/RefNotPermitted` | Zero matching spans during four seconds. The original request context reached the application. | HTTP 200 |
| Restored | `Accepted=True`, `ResolvedRefs=True` | Matching SERVER span | HTTP 200 |

The independent Gateway retained the original context and emitted zero matching spans in each four-second observation window. Standard had no policy CRD and both applications received HTTP 200 with the original context. The local setup used kind, the Octavia listener TaskFlow, Amphora’s renderer, HAProxy and OpenTelemetry Collector. OpenStack infrastructure and Amphora agent transport used fixtures. Regional deployment and Gateway-to-collector TLS remain separate qualifications. [Configuration](../../../operator-octavia-telemetry.md) · [Traffic, policy conditions and package digests](gateway-telemetry-evidence/receipt.json)
