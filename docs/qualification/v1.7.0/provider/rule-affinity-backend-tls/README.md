# Session cookies on authenticated Service endpoints

An Experimental HTTPRoute used a named cookie on a rule pointing to a Service with two endpoints. A BackendTLSPolicy selected its HTTPS port. Each request opened a new connection.

| Trust on the Service pool | Request carrying a cookie | Independent clients | Ordinary sibling |
| --- | --- | --- | --- |
| Original CA | 3 replays reached the issuing endpoint | Four clients reached both endpoints | HTTP 200 |
| Different valid CA | HTTP 503 with the original cookie | HTTP 503 without a cookie | HTTP 200 |
| Original CA restored | 3 replays of a newly issued cookie reached the issuing endpoint | Four clients reached both endpoints | HTTP 200 |

During the wrong-CA probes the peers recorded a TLS unknown-CA alert, zero application requests and zero cleartext attempts. The Service had two configured endpoints at all three checks. At each of the three checks the route reported Accepted, ResolvedRefs and Programmed, and the policy reported Accepted and ResolvedRefs.

The trial used Kubernetes envtest, the controller extracted from an OCI image built from a signed commit, installed Octavia packages, Barbican, the Amphora renderer and native HAProxy. Keystone, Neutron, RPC completion and Amphora agent transport used fixtures. The traffic measurements cover an Experimental HTTPRoute with Cookie affinity. gRPC Header and negotiated h2 were checked in the controller and graph tests. [Measured responses, conditions and artifact hashes](receipt.json) · [Operator guide](../../../../operator-octavia-backend-tls-policy.md)
