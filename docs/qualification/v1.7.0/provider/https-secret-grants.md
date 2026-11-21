# HTTPS certificate grants and conflicting ports

With operator HTTPS enabled, tenants can inspect certificate authorization even when another listener claims the same port. Both conflicting listeners report `Accepted=False/PortUnavailable`, `Conflicted=True/ProtocolConflict` and `Programmed=False`. Each listener also reports whether the operator-approved Secret revision is authorized. A missing cross-namespace grant yields `ResolvedRefs=False/RefNotPermitted`. [Operator guide](../../../operator-https.md)

## Registered Gateway API v1.7.0 cases

| Upstream case | Standard | Experimental | Original assertion observed |
|---|---|---|---|
| `GatewaySecretInvalidReferenceGrant` | PASS | PASS | A grant with the wrong scope leaves the certificate reference unpermitted. |
| `GatewaySecretMissingReferenceGrant` | PASS | PASS | Both HTTPS sections on port 443 report `ResolvedRefs=False/RefNotPermitted`. |
| `GatewaySecretReferenceGrantAllInNamespace` | PASS | PASS | The grant permits the approved Secret and the listener becomes programmed. |
| `GatewaySecretReferenceGrantSpecific` | PASS | PASS | The named grant permits the approved Secret and the listener becomes programmed. |

Each original case ran in its own Kubernetes 1.36.2 envtest control plane on the official release bundle, with the upstream defaults for assertions and cleanup. The production reconcilers used a recording provider. The operator helper processed the fixture's TLS Secret and a synthetic Barbican receipt. On the preceding revision, the missing-grant case timed out on both channels because both conflicting listeners reported `ResolvedRefs=True`. The other three cases passed. [Final per-channel assertions](https-secret-grants-results.json) · [Previous controller](https://github.com/armeldemarsac92/gateway-api-openstack/tree/0f0b18e3cfa17b007a14c6397b7b2942cba7a2fe) · [Original missing-grant case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/gateway-secret-missing-reference-grant.go)

## Fresh connections on the release image

On each channel, the test first served a known backend on port 443 with a verified certificate chain. Two conflicting HTTPS sections then claimed 443 without a matching cross-namespace grant. The native controller withdrew that frontend. Both new TLS connection attempts to 443 were refused or rejected, while verified HTTPS on 8443 and HTTP on 18080 still returned their expected backends. The receipts record conditions on both listeners, Octavia readback, and fresh network requests. [Base traffic and rotation exercise](https-operator-ports.md)

The twelve lifecycle phases per channel also recorded 17 successful TLS requests, 7 refused or rejected fresh TLS connections and 12 successful plain HTTP requests. They cover Secret revision A to B, grant withdrawal and restoration, cold manager restart and owned Octavia cleanup. The operator separately deleted three Barbican payloads and verified HTTP 404 on each. The original `GatewayWithAttachedRoutes` status case passed its three assertions on both channels in this mode. The default-mode outcome remains in the inventory.

The network fixture uses the release executable, stock Octavia 19.0.0, Barbican 23.0.0 and HAProxy 3.0.19. Keystone, Neutron, RPC and remote Amphora delivery are simulated. Loopback processes serve the EndpointSlice backends. All fourteen profile/channel combinations still await a disposable Hibiscus deployment. [Case inventory](../matrix/features-and-cases.md#complete-registered-test-inventory) · [Live results](../live-runs.md)

Controller `308c8b246fca168e4a687704badaf40335e8cf40`. The archive contains the upstream exercises and their receipts, the earlier missing-grant failure, the native traffic reproduction inputs and both channels' full native observations. Archive SHA-256 `ce342e4401d3255e0291f39c445156bbe5423a0d08f32afd6cbfd898f63d0c6e`. [Qualification archive](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-308c8b246fca168e4a687704badaf40335e8cf40/hibiscus-operator-https-308c8b246fca.tar.gz) · [Development release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-308c8b246fca168e4a687704badaf40335e8cf40)
