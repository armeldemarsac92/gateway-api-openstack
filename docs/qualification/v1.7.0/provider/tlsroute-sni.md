# TLSRoute SNI passthrough on one Amphora listener

Two TLSRoutes shared one dedicated `TLS/Passthrough` listener on port 9443. The route for `a.example.test` referenced the TCP Service `tls-a` in another namespace, which granted access by name. The route for `b.example.test` referenced its own namespace's Service. The Endpoint class resolved the Services and EndpointSlices to one IPv4 backend per route. [Operator setup](../../../operator-octavia-tls-sni-passthrough.md)

| State | a.example.test | b.example.test | Attached routes | Route A status |
| --- | --- | --- | ---: | --- |
| Both routes and grant present | Backend A certificate and echo | Backend B certificate and echo | 2 | Accepted and ResolvedRefs true |
| Grant removed | Connection rejected | Backend B certificate and echo | 2 | Accepted true, ResolvedRefs false with RefNotPermitted |
| Grant restored | Backend A certificate and echo | Backend B certificate and echo | 2 | Accepted and ResolvedRefs true |
| Route A deleted | Connection rejected | Backend B certificate and echo | 1 | Resource deleted |
| Route A recreated | Backend A certificate and echo | Backend B certificate and echo | 2 | Accepted and ResolvedRefs true |

Gateway API v1.7.0 Standard and Experimental each gave **8 successful exchanges and 12 rejections** on fresh connections to the VIP across these five states. Every success verified the backend certificate fingerprint against a direct handshake, the SNI observed by the backend and the 256 echoed bytes. Unknown names and connections without SNI were rejected in all five states. Every rejection added zero HAProxy backend sessions and zero backend payloads. Route B kept its UID and pool throughout. Route A kept its UID through the grant changes and received a new UID when recreated. [Standard receipt](tlsroute-sni-evidence/standard.json) · [Experimental receipt](tlsroute-sni-evidence/experimental.json)

An authenticated Octavia capability read returned `tls_sni_passthrough_v1` for Amphora. The public API exposed a TCP listener without a default pool or TLS container, `REDIRECT_TO_POOL` policies and `TLS_SNI` rules using `EQUAL_TO`. The receipts record current-generation Kubernetes Route conditions alongside the provider inventory and traffic.

During the additional Standard check, the Neutron fixture returned HTTP 503 from its Floating IP inventory. The fixture recorded 2 failed Floating IP reads from the controller. Grant removal still rejected A, while B returned its 256 bytes. After the API recovered and the grant was restored, A returned its echo. These samples are outside the five-state totals.

For each channel, the Gateway was deleted while the manager was stopped and both routes still existed. The Gateway still held its finalizer before restart. The same manager restarted with the SNI flag off, closed the VIP and finalized the Gateway. Its former routes no longer reported the parent as accepted or programmed. The Octavia fixture returned empty inventories for load balancers, listeners, pools, L7 policies and health monitors.

The manager came from the linux/amd64 OCI archive at source revision `6c74e8eaa94054527823c5c7576da96e84d5f6f1`. Both startup phases reported that revision. The receipts pin the OCI manifest, archive, executable, source tree and fixture inputs. The Standard and Experimental runs used separate Kubernetes v1.36.2 envtest instances and separate Octavia WSGI/SQLite instances. Octavia `19.0.1.dev4` and octavia-lib `3.12.1.dev3` loaded from the release wheels, including the package's functional API fixture and the Amphora renderer templates used by HAProxy 3.0.19. Each receipt records the wheel hashes and the runtime module paths relative to the installation. [OCI build record](../image.json) · [Reproduce the OCI build](../image-build.md) · [Octavia wheel and source](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-tls-sni-0e37f9873147) · [octavia-lib wheel and source](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-backend-h2c-bb1a993ac954)

The fixture supplied Keystone, Neutron, provisioning completion and the transport to the Amphora agent. The result covers this local exact-name, single-rule, IPv4 Endpoint profile. Deployed OpenStack, a real Amphora fleet, Magnum lifecycle and full upstream TLSRoute conformance remain unqualified.

The focused upstream cases `TLSRouteListenerPassthroughSupportedKinds` and `TLSRouteListenerTerminateNotSupported` passed on both bundles. They ran on envtest with the controller built from the same source revision and its in-process recording provider. [Upstream TLS listener status receipt](tlsroute-sni-evidence/tls-listener-status-receipt.json)

The source replay asset `hibiscus-tlsroute-sni-6c74e8eaa940.tar.gz` contains the runner, the extraction and replay instructions, and both receipts. SHA-256 `eaa77d5f3a5f90a8d02d5d24a203cd339eacff54017ba251bf4f8f63492847af`.
