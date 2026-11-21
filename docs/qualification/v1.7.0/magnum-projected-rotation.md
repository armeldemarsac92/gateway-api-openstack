# Magnum workload credential and CA rotation

## Scope and inputs

This local qualification ran on 2026-11-21 with the published `linux/amd64` controller at source revision `63ec0796cb57cd7ac0f698879e6b635d6069019b`, OCI manifest `sha256:0e4d2cef2350926304edb098f58c73c216f9cb8550b282e2717911d1a57a072b`. The manifest and its image configuration were checked against the release OCI archive.

The driver ran from magnum-capi-helm 1.4.0 at `a8b82c2480a286a10b60ecc1d94c6cd9f1d01823` with the integration patch. The existing driver test fixture rendered the checksum-pinned Azimuth `openstack-cluster` 0.10.1 package with Helm 3.19.4. The cluster-api-addon-provider 0.6.1 `Manifests` model and template loader built workload chart `openstack-gw` `0.1.0+7496f5e1` from the rendered source Secret via an in-memory management API read. The workload ConfigMap selected `experimental` to match the local Gateway API bundle. The driver rendered the actual image digest, cluster identity and dedicated Cloud Secret mount. [Driver patch](../../../integrations/magnum-capi-helm/1.4.0.patch)

The workload cluster ran on kind 0.33.0 with two Kubernetes 1.36.4 nodes and containerd 2.3.4. It used the original Gateway API v1.7.0 Experimental bundle and the fork's separate OpenStackGatewayClassConfig CRD. The controller Pods connected directly to the synthetic TLS endpoint on the private guest bridge.

The fixture extends the stock client rotation test with the read-only responses needed to validate a private Amphora GatewayClass with one shared VIP/member subnet. Keystone issues separate synthetic tokens for the initial and replacement credentials in the same test project and region. The HTTP counters identify the credential generation and response code. They contain no request bodies, credential IDs, tokens or TLS material. [Stock client rotation test](../../../internal/cloud/openstack/clients/rotation_test.go)

## Observed workload behavior

The Helm release installed five objects. Both initial controller Pods ran the expected image and were Ready without container restarts. The GatewayClass reached `Accepted=True` after the controller authenticated and read Octavia and Neutron through the fixture.

A full update to `openstack-gateway-system/openstack-clouds` retained the Secret UID, advanced its resource version and removed the old CA key. The Deployment generation stayed unchanged until rollout restart, which retained the Deployment UID. Helm's Deployment mounted the complete Secret read-only at `/etc/openstack`. The fixture switched to the replacement TLS chain for the restart, while the initial credential stayed valid until both replacement Pods were Ready. The original Pods terminated. The two replacement Pods were Ready with new UIDs, zero container restarts and the same pinned controller image.

| Fixture response from controller Pods | Initial input | Replacement input |
| --- | ---: | ---: |
| Keystone token creation, HTTP 201 | 2 | 2 |
| Octavia version discovery and validation, HTTP 200 | 4 | 3 |
| Octavia provider list, HTTP 200 | 2 | 1 |
| Neutron version discovery, HTTP 200 | 2 | 2 |
| Neutron subnet read, HTTP 200 | 2 | 1 |
| Neutron network read, HTTP 200 | 2 | 1 |

The replacement Pods performed the dependency reads with the new token. After the original Pods terminated, the fixture retired the initial credential and token. A fresh instance of the stock client with the replacement credential and only the retired CA failed with `x509.UnknownAuthorityError` and sent zero Keystone HTTP requests. The retired credential with the replacement CA reached Keystone and received exactly one HTTP 401. These two controls ran against the same fixture after the rollout. The fixture recorded zero other HTTP failure responses.

Helm uninstall removed the Deployment, ConfigMap, ServiceAccount, ClusterRole and ClusterRoleBinding it owned. The namespace, dedicated Cloud Secret, GatewayClass, OpenStackGatewayClassConfig and control ClusterRole each kept their UID. The 15 shared CRD UIDs survived uninstall and the subsequent named cleanup. The fixture exited. The qualification namespace, control objects and guest staging files were removed.

## Remaining boundary

This run verifies that real kubelet-managed controller Pods consume the rotated dedicated Secret after restart and use it for authentication and read-only GatewayClass validation. Real Magnum management reconciliation, deployed OpenStack identity and provider behavior, Gateway graph continuity, and traffic through a live VIP remain unqualified by this exercise. [Management-plane rehearsal](magnum-driver.md#local-lifecycle-rehearsal)

[Sanitized runtime counters](magnum-projected-rotation-results.txt) · [Download the exact fixture, counters and reproduction commands](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-455798411c13fd42f5c1568357f2c35cec2b2169/hibiscus-magnum-projected-rotation-63ec0796cb57.tar.gz)
