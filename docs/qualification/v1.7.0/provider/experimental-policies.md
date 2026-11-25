# Experimental policies and XMesh

At controller revision `ce6f51797fb7`, the genuine v1.7.0 Experimental bundle admitted the three resources below. The Kubernetes 1.36.2 API server used the release's safe-upgrades admission policy. The manager owned the Gateway and observed an independent listener change from port 80 to 81. It reported `Programmed=True` at the new generation, confirming that its Gateway watch was active. [Full sanitized observation](experimental-policy-results.json)

| Resource | Selection | Stored status |
|---|---|---|
| XBackendTrafficPolicy | A Service in the same namespace with a retry constraint | No policy status |
| XMesh | The running manager's controller name | `Accepted=Unknown/Pending` |
| XTelemetryPolicy | The managed Gateway with tracing disabled | No policy status |

Two explicitly requested Gateway reconciliations produced identical desired graphs before and after admission. That snapshot recorded API admission only. The later XMesh trial below includes manager reconciliation and mesh traffic. Retry budgets and collector exports were outside both trials. [Retry budget](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apisx/v1alpha1/xbackendtrafficpolicy_types.go#L61-L125) · [XMesh status](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apisx/v1alpha1/xmesh_types.go#L56-L126) · [Telemetry targets](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apisx/v1alpha1/xtelemetrypolicy_types.go#L90-L155)

The source archive includes the observer patch, the original-log checksum and the command for running it against the pinned release and envtest assets. The earlier XBackend EndpointSelector observation records the fourth Experimental kind. [Reproduction files](stock-tcp.md#reproduce-the-multiple-listener-probes) · [XBackend and its HTTPRoute](xbackend-endpoint-selector.md)

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
