# Experimental policies and XMesh

At controller revision `ce6f51797fb7`, the genuine v1.7.0 Experimental bundle admitted the three resources below. The Kubernetes 1.36.2 API server used the release's safe-upgrades admission policy. The manager owned the Gateway and observed an independent listener change from port 80 to 81. It reported `Programmed=True` at the new generation, confirming that its Gateway watch was active. [Full sanitized observation](experimental-policy-results.json)

| Resource | Selection | Stored status |
|---|---|---|
| XBackendTrafficPolicy | A Service in the same namespace with a retry constraint | No policy status |
| XMesh | The running manager's controller name | `Accepted=Unknown/Pending` |
| XTelemetryPolicy | The managed Gateway with tracing disabled | No policy status |

Two explicitly requested Gateway reconciliations produced identical desired graphs before and after admission. That snapshot recorded API admission only. The targeted retry budget and XMesh trials below include manager reconciliation and traffic. Collector exports remain outside this evidence. [Retry budget API](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apisx/v1alpha1/xbackendtrafficpolicy_types.go#L61-L125) · [XMesh status](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apisx/v1alpha1/xmesh_types.go#L56-L126) · [Telemetry targets](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apisx/v1alpha1/xtelemetrypolicy_types.go#L90-L155)

The source archive includes the observer patch, the original-log checksum and the command for running it against the pinned release and envtest assets. The earlier XBackend EndpointSelector observation records the fourth Experimental kind. [Reproduction files](stock-tcp.md#reproduce-the-multiple-listener-probes) · [XBackend and its HTTPRoute](xbackend-endpoint-selector.md)

## Service retry budget

In the local Experimental run, the controller reported the policy accepted on its Gateway ancestor at the current generation. The patched HAProxy returned HTTP 503 with one backend attempt when Service A exhausted its budget. Service B continued to return the backend's HTTP 500 after two attempts. Deleting the policy cleared the budget on A's Octavia pool and restored its two attempts. In the Standard run the API omitted the policy CRD and both routes kept their ordinary retries. The receipt covers a targeted local HTTP lifecycle using kind, published provider wheels and real HAProxy with cloud fixtures. [Recorded results and artifact digests](backend-retry-budget-evidence/receipt.json) · [Enablement and process limits](../../../operator-octavia-backend-retry-budget.md)

## XMesh readiness on Istio ambient

With the Experimental v1.7.0 bundle and the opt-in XMesh component, the manager created `hibiscus-istio-ambient` on Kubernetes v1.36.4. The local cluster ran the official Istio 1.31.0 pilot, CNI and ztunnel. All five Istio Pods and both manager replicas were ready at the first sample. OpenStack identity and discovery came from a local fixture. [Operator configuration](../../../../integrations/istio-ambient/README.md)

| istiod replicas | Accepted | Programmed | Reported features | XMesh resource version |
|---|---|---|---:|---:|
| 1, ready | True / Accepted | True / Programmed | 16 | 1081 |
| 0 | True / Accepted | Unknown / Pending | 0 | 1172 |
| 1, ready again | True / Accepted | True / Programmed | 16 | 1227 |

The XMesh kept the same UID and generation. Recovery restored the same 16 feature names. The upstream mesh cases for HTTP path rewriting and gRPC request-header modification passed on this installation. These two cases exercise waypoint traffic. XMesh status has no registered v1.7.0 case. The local receipt identifies the signed controller source, the executable and the test image observed in the running Pods. [Conditions, feature names, images and case names](xmesh-status.json)
