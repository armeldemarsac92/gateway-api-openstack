# Gateway API v1.7.0 live qualification

All fourteen profile and channel combinations are **UNRUN**. A designated disposable Hibiscus environment and runners with private network access are still required. This ledger covers Core and applicable Extended cases for each profile.

| Profile | Standard | Experimental |
| --- | --- | --- |
| GATEWAY-HTTP | UNRUN | UNRUN |
| GATEWAY-TLS | UNRUN | UNRUN |
| GATEWAY-TCP | UNRUN | UNRUN |
| GATEWAY-UDP | UNRUN | UNRUN |
| GATEWAY-GRPC | UNRUN | UNRUN |
| MESH-HTTP | UNRUN | UNRUN |
| MESH-GRPC | UNRUN | UNRUN |

The scoped TCP cases ran in a local kind fixture using Octavia’s public API, the stock worker renderer and HAProxy. The full local GATEWAY-TCP Core attempts stopped during the upstream shared HTTPS setup. [Local TCP reports](provider/tcp-upstream.md)

Both mesh Core profiles also ran on ordinary local Kubernetes Services without an enrolled dataplane. The upstream reports record the assertions that failed and the traffic that reached the echo pods. [Local mesh reports](provider/stock-mesh.md)

The complete local selections used all channel-applicable profile features. Each Gateway run reached the common HTTPS setup. The mesh profile reports and seven supplemental cases are available for both Standard and Experimental. [Local profile reports](complete-local-profiles.md) · [Registered mesh supplement](provider/mesh-registered-supplement.md)

## Prepare the two targets

Designate separate disposable control planes and cloud scopes for Standard and Experimental. Install the respective v1.7.0 release bundles before qualification. Set the controller to the same channel as the bundle. Provide the selected GatewayClass and an enrolled mesh dataplane for the Service-parent cases. The upstream Gateway fixtures use multiple listeners, HTTPS and Kubernetes Service backends. Allow the runner to reach the private VIPs, the provider to reach backend endpoints, and enrolled clients to reach the mesh Services. Check the cloud resource budget and provide real usable and unusable addresses for the static address tests. See the [release bundles](https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.7.0) and [controller installation guide](../../getting-started.md).

Apply each published manifest with server-side apply against its designated context. The Experimental HTTPRoute CRD exceeds the annotation size limit used by client-side apply. On an empty disposable control plane the release installs with `kubectl --context "$DISPOSABLE_CONTEXT" apply --server-side -f "$RELEASE_FILE"`. The preflight compares each persisted CRD and admission spec with the published asset after API-server defaulting.

Use a dedicated Linux runner for each channel. Provide Git, Go 1.27.4, `curl`, `kubectl`, Skopeo with read access to the immutable images, Python 3 with PyYAML, and access to the Kubernetes API and the pinned upstream source, release files, Go dependencies and test images. Keep the runner directory on private persistent storage outside the checkout. Provide these files under that directory.

| File | Contents |
| --- | --- |
| `kubeconfig` | One selected context for this disposable control plane. Include the API server CA and keep TLS verification enabled. |
| `inventory.yaml` | Operator-verified deployment and environment inventory for this run. |
| `options.yaml` | Upstream conformance options for the selected GatewayClass, mesh enrollment, implementation details, address fixtures and any environment-specific timeouts. |

Record the current `kube-system` Namespace UID and the image digest observed on the running controller workload. Confirm its source revision from the build record before entering `controllerRevision`. Keep mesh, Kubernetes, Magnum, OpenStack, Octavia provider and network topology versions or identifiers under `environment`, along with the observed `GatewayClass.status.supportedFeatures` and the approved test scope. For a dev provider or mesh build, record its exact deployed image and source revision in that inventory. The Kubernetes server version is also read during preflight.

```yaml
# inventory.yaml structure, with placeholders to replace before a run
channel: standard
clusterUID: <kube-system namespace UID>
controllerRevision: <full fork commit SHA from the deployed image build>
controllerImage: <running repository@sha256:digest>
gatewayWorkload:
  controllerName: gateway-api-openstack.github.io/magnum-gateway
  namespace: openstack-gateway-system
  deployment: magnum-openstack-gateway-controller
  container: controller
  pod: <ready Pod from this Deployment>
# Supply the mesh implementation and its build record when the mode is installed.
meshWorkload:
  controllerName: <controller name of the selected XMesh, if present>
  namespace: <mesh namespace>
  deployment: <mesh controller Deployment>
  container: <controller container>
  pod: <ready Pod from this Deployment>
  image: <mesh controller repository@sha256:digest>
  revision: <full mesh source commit>
  implementation:
    organization: <mesh implementation organization>
    project: <mesh implementation name>
    url: <mesh project URL>
    contact: [<mesh project contact>]
environment:
  cloud: <Hibiscus cloud, Magnum, Octavia and provider version inventory>
  mesh: <workload enrollment setup and dataplane versions>
  topology: <private runner, VIP, worker and cloud scope details>
```

Set `gatewayClassName` and provide `implementation.organization`, `implementation.project`, a valid `implementation.url`, and at least one `implementation.contact` in the options file. Each pass verifies the deployed Pod through its owning ReplicaSet and rolled-out Deployment, checks its reported image ID against the published manifest or config digest, and records those identities privately. The Gateway pass verifies the selected class and the fork image’s source label. The mesh pass records its own implementation, image and revision and checks an explicitly selected XMesh. An absent mesh workload leaves that pass unrun. For a third-party mesh image without a source label, keep the provider’s build record tying its digest to the stated revision. Use `namespaceLabels` and `namespaceAnnotations` as required by the mesh, and `usableNetworkAddresses` and `unusableNetworkAddresses` for the static IP cases. The pinned [upstream option definitions](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/suite/suite.go) describe the accepted fields.

For GitHub Actions, protect the `gateway-qualification-standard` and `gateway-qualification-experimental` environments for the default branch and assign each matching label to its dedicated runner. Set the environment secret `GATEWAY_QUALIFICATION_ROOT` to that runner's absolute private directory. Dispatch **Gateway API v1.7.0 qualification** on the fork's default branch with the full controller source SHA. For a runner with these prerequisites, use the same command locally from the reviewed fork checkout.

```bash
git clone --depth 1 --branch v1.7.0 https://github.com/kubernetes-sigs/gateway-api.git /path/to/upstream-v1.7.0
GATEWAY_QUALIFICATION_ROOT=/private/standard \
  bash hack/qualify-gateway-api.sh standard <controller-sha> /path/to/upstream-v1.7.0
GATEWAY_QUALIFICATION_ROOT=/private/experimental \
  bash hack/qualify-gateway-api.sh experimental <controller-sha> /path/to/upstream-v1.7.0
```

## Run and interpret qualification

Before the suite makes any changes, preflight reads the target UID, Kubernetes version and Gateway API inventory. It compares the live CRDs and admission objects with the checksummed release manifest, including their annotations and specs. The release policy and its binding are annotated `standard` in both bundles. Preflight also rejects an existing conformance namespace or an inventory revision that differs from the requested SHA.

Each channel runs the upstream `TestConformance` entry point twice so failure during Gateway setup still allows the mesh cases to run. The Gateway pass selects the five Gateway profiles and exempts `Mesh`. The mesh pass selects both mesh profiles and exempts `Gateway`. All 170 registered cases at the pinned commit require exactly one of these two feature flags. Both passes enable `--all-features` so all applicable Extended and provisional cases are attempted, including cases the upstream profile report may omit. The Standard passes additionally exempt `ClusterTrustBundle`, `GatewayAddressRoutability`, `GatewayAddressRoutabilityCluster`, `GatewayListenerFilters` and `HTTPRouteDestinationPortMatching`, the Experimental features in the pinned registry. The Experimental passes include them. The requested selection tests behavior and does not change controller feature declarations. See the [upstream runner](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/conformance_test.go), [profile definitions](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/suite/profiles.go) and [feature registry](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152/pkg/features).

Each channel writes a private run directory with the copied inventory and options, observed Kubernetes inventory, exact suite and implementation revisions, feature selections, Go test event stream and exit codes, and the unmodified upstream reports when generated. `gateway-cases.tsv` and `mesh-cases.tsv` list `PASS`, `FAIL`, `SKIP`, `INCOMPLETE`, `UNRUN`, or `UNRUN_CHANNEL` per case. Use event output for the reason behind a failure or skip. An upstream setup failure can produce an empty report. Cases that never started and profiles without a result remain `UNRUN`. After cancellation, reconcile the case list with any events retained before interruption. Review cleanup on the disposable target before starting another run.

The GitHub summary includes the requested SHA, upstream revision and each profile's reported Core and Extended result. The runner keeps raw evidence and does not upload it as a GitHub artifact. Review the original records and remove environment identifiers before any authorized publication. Exercise normative contracts that lack a tagged case through separate checks. See the [evidence publication policy](../../../CONTRIBUTING.md#test-evidence-and-publication).

## OpenStack lifecycle E2E

Dispatch **OpenStack E2E** on the fork's default branch with repository variable `OPENSTACK_E2E_ENABLED=true`. Configure the protected `openstack-e2e` environment, its `OPENSTACK_E2E_CONFIG` secret containing the absolute private configuration path, and a dedicated runner carrying the `openstack-e2e` label. The job runs Cluster and Local traffic-policy checks through the existing `make test-e2e-recurring` entry point. The persistent pending marker blocks a new run after failure until the owned test resources and installation are confirmed absent. See the [E2E configuration and cleanup procedure](../../testing-openstack-e2e.md#recurring-dedicated-validation).

A separate Standard pilot reached a real kind worker NodePort through a local private VIP and retained forwarding while the controller was stopped. The management addon also completed three workload revisions on kind. [Cold restart](provider/controller-cold-http.md) · [Management lifecycle](magnum-management-addon.md)

The live pilot remains **UNRUN**. In its nominated project, record the exact deployed digest and verified inventory. Use a client on the private network to identify the backend response, restart the controller, and verify the same response and owned Gateway graph. Track the API-server and OCCM Service load balancers before the run and after Gateway finalization. Confirm their identities and test traffic where available. Record Magnum install, upgrade, rollback, credential and certificate rotations, uninstall and teardown as separate results.
