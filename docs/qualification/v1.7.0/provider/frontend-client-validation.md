# HTTPS client certificates and delegated certificate grants

With frontend client validation enabled, a Gateway can require a client certificate signed by the CA in its referenced ConfigMap. A port may choose a different CA. Gateway and ListenerSet references to a server certificate each require their own cross-namespace authorization. [Operator configuration](../../../operator-https.md)

| Registered case | Standard and Experimental observations |
| --- | --- |
| `GatewayFrontendClientCertificateValidation` | Clients using the matching CA reached the expected echo backend on 443 and 8443. Each listener rejected a client signed by the other CA. |
| `GatewayFrontendInvalidDefaultClientCertificateValidation` | Invalid default CA configuration closed HTTPS. HTTP on port 80 still returned 200 from the expected pod. |
| `GatewayInvalidFrontendClientCertificateValidation` | All four upstream status assertions passed, including invalid kind and missing ReferenceGrant. |
| `ListenerSetReferenceGrant` | The upstream status assertions passed for the permitted and denied ListenerSets. Supplemental grant removal and restoration checks exercised both directions on the Kubernetes API and inspected the provider graph. |

The first two cases ran using the executable from the signed controller image with Octavia and Barbican REST, native HAProxy, kind and echo Pods. Keystone, Neutron network allocation, RPC scheduling and the Amphora agent transport were simulated. The two status cases ran on Kubernetes 1.36.2 envtest with a recording provider. The registered ListenerSet case sends zero traffic. The Standard traffic supplement also showed TCP refusal on 8443 and HTTP 200 on the independently authorized 443 after withdrawal of the former port's CA. [Source revisions, original reports and observations](frontend-client-validation-evidence/receipt.json)

A separate Experimental traffic probe on the initial shared-port implementation observed interruptions to the surviving HTTPS hostname during withdrawal and restoration of its sibling grant. After convergence, the retained name returned 200 and the removed hostname returned 404. The mixed SNI and HTTP authority probe returned 421. Continuous availability across those transitions needs its own traffic result.
