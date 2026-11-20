# Gateway API v1.7.0 qualification on Hibiscus

The fork provides a development controller and an opt-in Magnum driver patch. The first qualification target is an HTTP request sent through a private Amphora VIP to a known response on a worker NodePort. A disposable Hibiscus deployment has yet to be designated. Both channels have zero live profile runs.

## Revisions and installation

| Component | Development input | Deployed version |
|---|---|---|
| Controller base | [Audited upstream commit 0d41b7c](https://github.com/gateway-api-openstack/gateway-api-openstack/tree/0d41b7ce39125c012af8bd1bfba3a9c90f0d1252) | Unobserved |
| Gateway API and conformance suite | [v1.7.0, e26d183](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152) | Unobserved |
| Go | [1.27.4](https://go.dev/dl/#go1.27.4) | Build input |
| Magnum and driver | [Magnum 23.0.0, magnum-capi-helm 1.4.0, chart 0.10.1 and exact revisions](magnum-driver.md) | Unobserved |
| Cloud and provider | [Octavia 19.0.0, Amphora, Neutron, Keystone and Barbican source revisions](provider/provider-feasibility.md) | Unobserved |
| Kubernetes and Amphora image | Kubernetes 1.36.2 for local envtest. Record the real management/workload versions, provider and Amphora image before a live run. | Unobserved |

The [Magnum operator guide](magnum-operator.md) covers how to build the patch, select an operator-owned template, supply the credential, rotate inputs, verify a drain and remove the addon. The [portable installation](../../../README.md#getting-started) uses the same controller. Install the official Standard bundle by default. Experimental requires the complete official Experimental bundle on a separate cluster and a controller configured for that channel. In both bundles the safe-upgrades policy and binding use the Standard annotation. [Release assets](https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.7.0)

## Profile records

An **Unrun** entry means no upstream suite execution, no pass/fail/skip counts, and no suite-generated report for this fork and channel. The Experimental suite may exercise additional fields on the shared Gateway API kinds. Each applicable Core and Extended case still needs its own result. The [matrix](matrix/README.md) relates features, cases, both schemas and unflagged API requirements.

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

The [Hibiscus adapter test](cloud-contract.md) injects a lost response after Octavia applies an L7-rule DELETE. A fresh adapter discovers the remaining graph and the test checks the final inventory, including the Magnum API server and OCCM Service controls. The [hostname check](../../providers/hibiscus-exact-hostname.md) sent HTTP requests through standalone HAProxy using the stock Octavia L7 rule template. The [ReferenceGrant check](service-grants.md) exercised authorization and revocation on a local API server. The [Magnum test](magnum-operator.md#offline-evidence-and-limits) renders the pinned parent chart through the patched driver. The [provider cases](provider/provider-feasibility.md) identify what the stock public API can express and the design changes required for additional behavior.

## First live run

1. Record management and workload Kubernetes versions, actual Magnum and driver builds, parent chart digest, Gateway bundle, controller image digest, cloud services, negotiated microversion and Amphora image. Confirm a private VIP and reachable worker NodePorts. Keep credentials and environment identifiers in private run artifacts.
2. Record both control load balancer identities and ownership before creation. Use the existing [foundation scenarios](../../testing-openstack-e2e.md#foundation-checks) in a designated project. Check the backend response through the private VIP, cold-restart the controller, send the same request again, and compare the IDs and owned graph.
3. Remove the test graph, wait for finalizers and audit Octavia and Neutron. Verify both original control load balancers and the absence of all objects tagged for this run. Exercise provider failures, permission revocation and credential and certificate rotation while the workload remains reachable.
4. Qualify Magnum install, upgrade, rollback, disable, removal and cluster teardown. Follow the management and workload steps in the operator guide. Record the result of each lifecycle transition independently of the traffic suite.
5. Follow the [manual qualification runbook](live-runs.md) to run the unmodified profile suite against each channel’s own disposable cluster. Retain the original report, logs and environment metadata. Record setup failures and unrun cases explicitly. A trace of the live wire behavior is also needed for requirements without an upstream test.
