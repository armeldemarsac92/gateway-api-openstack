# Verify a Service backend with the Amphora system CA bundle

An HTTPRoute used a Service protected by a BackendTLSPolicy with `wellKnownCACertificates: System`. Each phase changed the endpoint selected by the Service. The HTTPRoute, policy, and protected Octavia pool retained their identities. [Operator configuration](../../../operator-octavia-backend-tls-policy.md#use-the-amphora-system-trust-store)

| Channel | Origin | VIP responses | Application hits | Witness HTTP 200 |
|---|---|---|---:|---:|
| Standard | trusted | 200, 200, 200 | 3 | 3 |
| Standard | untrusted-issuer | 503, 503, 503 | 0 | 3 |
| Standard | wrong-name | 503, 503, 503 | 0 | 3 |
| Standard | trusted-restored | 200, 200, 200 | 3 | 3 |
| Experimental | trusted | 200, 200, 200 | 3 | 3 |
| Experimental | untrusted-issuer | 503, 503, 503 | 0 | 3 |
| Experimental | wrong-name | 503, 503, 503 | 0 | 3 |
| Experimental | trusted-restored | 200, 200, 200 | 3 | 3 |

The policy and HTTPRoute reported current `Accepted=True` and `ResolvedRefs=True` throughout. The controller’s programming condition stayed true. HAProxy checked the issuer and the policy’s DNS name when connecting to each origin. The trusted origin received three requests on initial attachment and three after restoration. [Per-phase conditions, counters and certificate fingerprints](backend-tls-system-ca-evidence/receipt.json)

Before the first member was submitted, the Amphora agent confirmed the running HAProxy generation with the bundle configured as the empty backend’s default CA. The controller then read the same SHA-256 on the active, empty pool through the public Octavia API. A separate route on the Gateway served all its probes.

The local trials used a disposable kind cluster, the binary from the controller image, installed Octavia and octavia-lib wheels, the Amphora agent reload method, HAProxy and real backend Pods. Cloud identity, network provisioning, Amphora transport and the service manager were simulated. Regional Hibiscus qualification remains pending. The preceding Standard build rejected the policy and returned three HTTP 503 responses. Its scenario completed successfully and its shell launcher exited 127 after teardown.
