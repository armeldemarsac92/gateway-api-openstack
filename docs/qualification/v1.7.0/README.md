# Gateway API v1.7.0 qualification on Hibiscus

The fork provides a development controller and an opt-in Magnum driver patch. The first qualification target is an HTTP request sent through a private Amphora VIP to a known response on a worker NodePort. A disposable Hibiscus deployment has yet to be designated. Both channels have zero live profile runs.

## Revisions and installation

| Component | Development input | Deployed version |
|---|---|---|
| Controller base | Upstream `main` matched the audited base on November 21, 2026. [Commit 0d41b7c](https://github.com/gateway-api-openstack/gateway-api-openstack/tree/0d41b7ce39125c012af8bd1bfba3a9c90f0d1252) | Unobserved |
| Fork controller image | [Source revision, image and reproduction procedure](image-build.md) | Unobserved |
| Gateway API and conformance suite | [v1.7.0, e26d183](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152) | Unobserved |
| Go | [1.27.4](https://go.dev/dl/#go1.27.4) | Build input |
| Magnum and driver | [Magnum 23.0.0, magnum-capi-helm 1.4.0, chart 0.10.1 and exact revisions](magnum-driver.md) | Unobserved |
| Cloud and provider | [Octavia 19.0.0, Amphora, Neutron, Keystone and Barbican source revisions](provider/provider-feasibility.md) | Unobserved |
| Kubernetes and Amphora image | Kubernetes 1.36.2 for local envtest. Record the real management/workload versions, provider and Amphora image before a live run. | Unobserved |

The [Magnum operator guide](magnum-operator.md) covers how to build the patch, select an operator-owned template, supply the credential, rotate inputs, verify a drain and remove the addon. The [portable installation](../../../README.md#getting-started) uses the same controller. Install the official Standard bundle by default. Experimental requires the complete official Experimental bundle on a separate cluster and a controller configured for that channel. In both bundles the safe-upgrades policy and binding use the Standard annotation. [Release assets](https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.7.0)

## Profile records

An **Unrun** entry means no complete upstream profile execution, no pass/fail/skip counts, and no suite-generated report for this fork and channel. The Experimental suite may exercise additional fields on the shared Gateway API kinds. Each applicable Core and Extended case still needs its own result. The [matrix](matrix/README.md) relates features, cases, both schemas and unflagged API requirements.

| Upstream profile | Standard | Experimental | Required traffic observation |
|---|---|---|---|
| GATEWAY-HTTP | Unrun | Unrun | HTTP and terminated HTTPS, route matching, mutation, weighted and invalid backend responses |
| GATEWAY-GRPC | Unrun | Unrun | HTTP/2 method and metadata routing, backend protocol and gRPC status/trailers |
| GATEWAY-TLS | Unrun | Unrun | ClientHello SNI selects the backend while the client negotiates TLS with that backend |
| GATEWAY-TCP | Unrun | Unrun | An external client exchanges stream bytes with the chosen Service |
| GATEWAY-UDP | Unrun | Unrun | UDP requests and replies reach the chosen Service |
| MESH-HTTP | Unrun | Unrun | An enrolled workload uses the ordinary Service address and observes a Service-parent HTTPRoute |
| MESH-GRPC | Unrun | Unrun | An enrolled client calls the ordinary Service and observes a Service-parent GRPCRoute |

The Experimental qualification also requires behavioral checks for XBackend, XBackendTrafficPolicy, XMesh and XTelemetryPolicy. The registry inventory identifies named features without upstream cases. [Additional contracts](matrix/unflagged-contracts.md) · [Source study of AWS, Envoy Gateway and Istio](reference-implementations.md)

## Reproduce the local checks

Use Go 1.27.4 and the linter version reported by `make lint-version`. The following commands use a fresh local API server and etcd for each envtest run.

```sh
make envtest-assets
make ci
make test-envtest GATEWAY_API_CHANNEL=experimental
```

Local verification passed the pinned linter, Go race tests, tagged E2E helper tests, HTTP Core inventory, Standard envtest, binary and image builds. A separate Experimental envtest also passed. The qualification preflight accepted the two published release assets after server-side installation on separate Kubernetes 1.36.2 API servers. Its raw reads returned 10 and 14 established CRDs respectively, with one safe-upgrades policy and binding per channel.

| Local evidence | Observation |
|---|---|
| [Upstream status cases](../../../internal/controller/envtest_upstream_status_test.go) | The controller watches run five v1.7.0 cases with their upstream assertions. They cover `GatewayClassObservedGenerationBump`, `GatewayInvalidParametersRef`, `GatewayInvalidRouteKind`, `GatewayListenerUnsupportedProtocol` and `HTTPRouteInvalidCrossNamespaceParentRef`. The last case requires both a denied parent and resolved backend references. |
| [Automatic IP address request](matrix/features-and-cases.md#complete-registered-test-inventory) | A separate test applies the tagged address manifest directly, reads back the empty IP request and checks the address reported by the controller. The cloud provider is simulated. |
| [Stock Amphora rendering and HTTP traffic](cloud-contract.md#http-traffic-from-the-adapters-own-writes) | A real HAProxy sent a matching request to the echo backend before and after recreation of the Octavia adapter. Both responses were HTTP 200 with the original Host. A route miss returned HTTP 503 where v1.7.0 requires 404. |
| [Gateway and route hostname intersection](../../providers/hibiscus-exact-hostname.md) | An exact name and a wildcard can narrow one another. Nested matching subdomains reach the backend. The wildcard excludes its apex. |
| [Provider cleanup](cloud-contract.md) | After the adapter loses the response to a disable request, a fresh instance waits for the owned load balancer to be disabled and ACTIVE. It then removes the graph and leaves the two fixture control load balancers intact. |
| [Shared HTTP Gateway](cloud-contract.md#two-routes-on-a-shared-listener) | Two tenant namespaces attach same-named routes to the shared listener. Removing one namespace label drops its binding and changes the listener count from two to one. The independent Octavia loopback test checks policy order, selective deletion and continued sibling traffic. [Kubernetes watch test](../../../internal/controller/envtest_references_test.go) |
| [Stock Amphora UDP forwarding](provider/stock-udp.md) | The same VIP selected backend A, then B after a default-pool update. A detached listener left an empty IPVS table and returned `ECONNREFUSED` while both backends still served direct packets. The API and a disposable Linux guest supplied this evidence. Kubernetes UDPRoute reconciliation remains open. |
| [Stock Amphora gRPC routing](provider/stock-grpc.md) | On a single HTTP/2 client connection over TLS, two exact methods selected different backends and returned their gRPC trailers. The stock worker renderer produced the HAProxy configuration. The upstream HTTP/h2c case and Kubernetes GRPCRoute reconciliation remain open. |
| [Three upstream TCPRoute status cases on kind](kind-tcp.md) | Standard v1.7.0 with 23 Ready workload pods. The scoped base omitted one unsupported HTTPS Gateway. A direct worker NodePort handshake identified a Ready upstream backend pod. |
| [TCPRoute attachment and revocation](provider/stock-tcp.md#controller-and-provider-checks) | Two Kubernetes TCPRoutes attach and the oldest supplies the only endpoint. Grant removal withdraws traffic intent. Namespace label removal promotes the remaining route. The provider adapter also exercises restart, binary forwarding and detachment on loopback. |
| [Stock Amphora TCP forwarding](provider/stock-tcp.md) | Attaching a TCP pool through the public Octavia API let a loopback client exchange all 256 byte values with the member through HAProxy. Clearing the attachment stopped the traffic while the member still existed. The test uses the upstream API fixture and simulates worker completion. |
| [ReferenceGrant](service-grants.md) | The controller observes authorization and revocation for a cross-namespace Service on a local Kubernetes API server. |
| [Magnum lifecycle](magnum-driver.md#local-lifecycle-rehearsal) | Helm installed, upgraded, rolled back and removed the workload addon. Uninstall removed all five chart objects and preserved the sampled external resources and updated Cloud Secret. A separate TLS check exercised client startup with the replacement credential and CA. |

The [provider study](provider/provider-feasibility.md) describes the proposed Octavia policy extension for request headers and an opt-in waypoint for Service traffic. Both proposals require implementation and on-cloud qualification.

## First live run

1. Record management and workload Kubernetes versions, actual Magnum and driver builds, parent chart digest, Gateway bundle, controller image digest, cloud services, negotiated microversion and Amphora image. Confirm a private VIP and reachable worker NodePorts. Keep credentials and environment identifiers in private run artifacts.
2. Record both control load balancer identities and ownership before creation. Use the existing [foundation scenarios](../../testing-openstack-e2e.md#foundation-checks) in a designated project. Check the backend response through the private VIP, cold-restart the controller, send the same request again, and compare the IDs and owned graph.
3. Remove the test graph, wait for finalizers and audit Octavia and Neutron. Verify both original control load balancers and the absence of all objects tagged for this run. Exercise provider failures, permission revocation and credential and certificate rotation while the workload remains reachable.
4. Qualify Magnum install, upgrade, rollback, disable, removal and cluster teardown. Follow the management and workload steps in the operator guide. Record the result of each lifecycle transition independently of the traffic suite.
5. Follow the [manual qualification runbook](live-runs.md) to run the unmodified profile suite against each channel’s own disposable cluster. Retain the original report, logs and environment metadata. Record setup failures and unrun cases explicitly. A trace of the live wire behavior is also needed for requirements without an upstream test.
