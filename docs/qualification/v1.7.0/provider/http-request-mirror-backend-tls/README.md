# Backend TLS for an HTTPRoute mirror

A supplemental Gateway API v1.7.0 Standard run sent one GET per phase through an HTTPRoute with 100% request mirroring. A `BackendTLSPolicy` targeted the mirror Service's named port. The test changed the policy's CA ConfigMap reference to a different valid CA, then restored the original reference.

| Trust in the mirror pool | Primary | Mirror |
| --- | --- | --- |
| Correct CA | HTTP 200 | One GET over TLS 1.3 with the configured SNI |
| Different valid CA | HTTP 200 | A TLS unknown-CA alert and zero HTTP requests |
| Correct CA restored | HTTP 200 | One GET over TLS 1.3 with the configured SNI |

At all three checks, the policy reported Accepted and ResolvedRefs, and the route reported Programmed. Each GET opened one private HAProxy mirror session. The active mirror pool used the same endpoints and the configured CA fingerprint in each phase. The mirror peer observed zero cleartext attempts.

Kubernetes envtest ran the API server and etcd. The running controller matched the binary extracted from the OCI image built from the signed source commit. It configured the installed Octavia packages, and the Amphora driver rendered the configuration for native HAProxy. Keystone, Neutron, RPC completion and Amphora agent transport used local fixtures. The peers ran locally, and the evidence covers the Standard bundle on this local setup. [Artifact hashes, readback and traffic counters](receipt.json)
