# Verify TLS to an HTTPRoute HTTP authorizer

An Experimental HTTPRoute rule can call an HTTP authorization Service over TLS. Set a BackendTLSPolicy for the authorization Service and enroll Amphora in `enabled_l7policy_http_external_auth_tls_providers`. Set the Service port to `appProtocol: https` when it requires TLS. [Operator instructions](../../../../operator-octavia-http-actions.md#httproute-external-authorization) · [Octavia release](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-http-route-http-authorizer-tls-be4a61a490b0)

| Local Experimental trial | Kubernetes conditions | Protected rule | Public sibling after convergence |
| --- | --- | --- | --- |
| Cross-namespace grant and CA present | Route `ResolvedRefs=True/ResolvedRefs` and `Programmed=True/Programmed`. BackendTLSPolicy accepted. | HTTP 200. The authorizer received one request using TLSv1.3 and HTTP/1.1 after HAProxy verified the private CA and exact DNS name and sent the configured SNI. The origin received one request. | HTTP 200 |
| ReferenceGrant withdrawn | Route `ResolvedRefs=False/RefNotPermitted` and `Programmed=False/Pending`. | HAProxy returned HTTP 500 locally with zero authorizer or origin requests. | HTTP 200 |
| ReferenceGrant restored | Route `ResolvedRefs=True/ResolvedRefs` and `Programmed=True/Programmed`. BackendTLSPolicy accepted again. | HTTP 200 with one encrypted authorization request and one origin request. | HTTP 200 |
| CA ConfigMap removed | BackendTLSPolicy `Accepted=False/NoValidCACertificate` and `ResolvedRefs=False/InvalidCACertificateRef`. Route `ResolvedRefs=Unknown/Pending` and `Programmed=False/Pending`. | HAProxy returned HTTP 500 locally with zero authorizer or origin requests. | HTTP 200 |

The authorizer detected a cleartext control probe and recorded zero cleartext connections during the four Gateway phases. Those phases used the same route generation and Gateway address. Strict server-side dry-run on the Standard CRDs rejected ExternalAuth and admitted an ordinary HTTPRoute.

The Gateway trials on Kubernetes v1.36.2 used the signed controller image, installed Octavia and Octavia-lib wheels, Barbican, Amphora configuration rendering and native HAProxy. Keystone, Neutron, RPC completion and Amphora agent transport used fixture adapters. The stable and master Octavia packages each reproduced in two builds and passed 5 installed-package tests and 9 native HTTP authorizer TLS cases. Qualification on a deployed Hibiscus region remains open. [Signed revisions, artifact digests, conditions and peer calls](receipt.json)
