# Magnum management addon reconciliation

On 2026-11-21 the published cluster-api-addon-provider ran its stock Kopf handlers against two separate Kubernetes 1.36.2 envtest APIs in the first rehearsal. The management API held the `Manifests` and source Secret rendered by the patched magnum-capi-helm 1.4.0 driver from `openstack-cluster` 0.10.1. The CAPI Cluster and OpenStackCluster records came from the same render. The local fixture supplied CAPI's `ControlPlaneInitialized` condition, a dummy infrastructure identity and the workload API kubeconfig. It served the CAPO 0.10.4 `v1alpha7` schema as the OpenStackCluster storage version, without a conversion webhook. The API-only run set an 18-second Helm timeout in the addon spec. [Driver render and chart checksum](magnum-driver.md#real-chart-and-render-path)

The management chart 0.6.1 defaults to image tag `771b287`, the short form of source revision `771b28756e78c5271e40bd8f3b163e1d31f649f7`. Both that tag and `0.6.1` resolve to the tested Linux amd64 manifest `sha256:fcdd796c2dab2d2ce2597c262000731846292e0c5e2387a05d5c6919c77225af`. The image bundled Kopf 1.37.2, pyhelm3 0.4.0 and Helm 3.15.4. [Pinned management dependencies](https://github.com/azimuth-cloud/capi-helm-charts/blob/856f8fb7372615ac0d3173d9b5d91fe605c9aeac/dependencies.json) · [Published addon chart](https://azimuth-cloud.github.io/cluster-api-addon-provider/cluster-api-addon-provider-0.6.1.tgz) · [Operator entrypoint](https://github.com/azimuth-cloud/cluster-api-addon-provider/blob/771b28756e78c5271e40bd8f3b163e1d31f649f7/Dockerfile)

## API-only rehearsal

The stock handler wrote `Pending` before the CAPI bootstrap condition was supplied. It then read the real management source Secret and used the CAPI kubeconfig to reach the workload API. It set the CAPI Cluster as owner of the `Manifests` and held the addon with its finalizer. The workload API recorded Helm creating the Deployment. The Deployment, ServiceAccount, ClusterRole, ClusterRoleBinding and ConfigMap carried the `openstack-gw` release annotations. [Stock addon update handler](https://github.com/azimuth-cloud/cluster-api-addon-provider/blob/771b28756e78c5271e40bd8f3b163e1d31f649f7/capi_addons/operator.py#L187)

Helm ran with readiness waiting enabled. Envtest had no deployment controller, scheduler or kubelet, so the Deployment produced no Pods. Helm recorded revision 1 as `failed` after the deadline. The operator logged the exception for retry, while its last recorded phase was `Installing`. [Stock Helm invocation and status writes](https://github.com/azimuth-cloud/cluster-api-addon-provider/blob/771b28756e78c5271e40bd8f3b163e1d31f649f7/capi_addons/models/v1alpha1/base.py#L602)

Updating the source Secret to a second fixture image digest drove the upgrade path. The operator wrote `Upgrading`, and Helm patched the existing Deployment to the new image while preserving its UID. Revision 2 failed at the same readiness deadline and the management phase stayed `Upgrading`. Neither failed revision populated `status.revision`. [Stock source watch](https://github.com/azimuth-cloud/cluster-api-addon-provider/blob/771b28756e78c5271e40bd8f3b163e1d31f649f7/capi_addons/operator.py#L447)

Deleting the `Manifests` with the CAPI Cluster still present produced `Uninstalling`. Stock Helm removed the Deployment, the other four release objects and both history Secrets. Kopf cleared the finalizer and the management resource disappeared. The management source Secret remained, and the workload namespace, Cloud Secret, GatewayClass and two CRDs retained their UIDs. [Stock deletion handler](https://github.com/azimuth-cloud/cluster-api-addon-provider/blob/771b28756e78c5271e40bd8f3b163e1d31f649f7/capi_addons/operator.py#L331)

## Rehearsal with kubelets

The same published operator used a Kubernetes 1.36.2 management API and the two-node Kubernetes 1.36.4 kind workload cluster through an SSH tunnel using the kind CA and client certificate. The workload used the official Gateway API v1.7.0 Standard bundle and the fork's OpenStackGatewayClassConfig CRD. The patched driver rendered the pinned chart twice with Helm 3.19.4. Both renders used the Standard workload artifact with SHA-256 `b8523298642fe46b1b4d3fbadef88c23f2d1b2af7a493900b3b4d7b1793cd1cb`. The Gateway `Manifests` was identical across the renders. The source Secret differed by the image digest only. The management fixture used the same CAPI bootstrap condition with a 100-second Helm timeout.

| Action | Helm revision | Published image source | Management phase and Pods |
| --- | --- | --- | --- |
| Create the rendered addon | 1 | `63ec0796cb57cd7ac0f698879e6b635d6069019b` | `Deployed`, 2 Ready |
| Apply the second rendered source Secret | 2 | `ce6f51797fb7c31a63c1f63577cb6938ecaff618` | `Deployed`, 2 replacement Pods Ready |
| Restore the initial rendered source Secret | 3 | `63ec0796cb57cd7ac0f698879e6b635d6069019b` | `Deployed`, 2 replacement Pods Ready |

The Linux amd64 manifest digests were `sha256:0e4d2cef2350926304edb098f58c73c216f9cb8550b282e2717911d1a57a072b` and `sha256:0326e0e3b911b78ab0427f652f9344029ac155d42f4aca2d63c91954a555c23f`, in that order. Both images were preloaded and verified on both nodes. The live Deployment kept its UID through both updates. Container image IDs matched the expected images and every sampled Pod had zero restarts. The management audit recorded `Pending`, `Installing`, `Deployed`, `Upgrading`, `Deployed`, `Upgrading`, `Deployed` and `Uninstalling`. Helm marked the preceding revisions `superseded` on each update.

The controller used the projected Cloud Secret against a synthetic TLS Keystone endpoint and read-only catalog services on the private kind bridge. The fixture recorded six successful Keystone authentications, ten successful Octavia version reads, four Octavia provider reads and fourteen Neutron reads across versions, subnet and network. It recorded zero HTTP failure responses. The GatewayClass reached `Accepted=True`.

Deleting the `Manifests` with the Cluster record and workload API present removed the five Helm objects and all release revisions. The source Secret remained in the management API. The workload namespace, Cloud Secret, GatewayClass, class config and control ClusterRole kept their UIDs through uninstall. Scoped cleanup then removed the disposable workload fixtures and preserved all eleven shared CRD UIDs. Live Magnum orchestration, OpenStack provider writes and traffic through an Octavia VIP remain open. [Live acceptance criteria](magnum-driver.md#live-acceptance-and-missing-inputs)

The compact receipt contains the Pod count, image and complete Helm revision history from each successful management reconciliation. [Three workload revisions](magnum-management-addon-results.json)

## Inspect a rehearsal

Use explicit disposable contexts for both APIs. Preload both published digests on each workload node. Register the CAPI fixtures and create the workload namespace, TLS Cloud Secret and class fixtures. Apply the initial rendered source Secret and `Manifests` on the management API. Then apply the second rendered Secret and restore the initial one, checking the status and Pod image IDs after each change.

```sh
kubectl --context "$MANAGEMENT" -n "$PROJECT_NAMESPACE" \
  get manifests "$ADDON" -o jsonpath='{.status.phase}{" "}{.status.revision}{"\n"}'
helm --kube-context "$WORKLOAD" -n openstack-gateway-system history openstack-gw
kubectl --context "$WORKLOAD" -n openstack-gateway-system \
  rollout status deployment/magnum-openstack-gateway-controller --timeout=100s
kubectl --context "$WORKLOAD" -n openstack-gateway-system \
  get pods -l app.kubernetes.io/name=openstack-gateway-controller \
  -o jsonpath='{range .items[*]}{.status.containerStatuses[0].imageID}{"\n"}{end}'
```

Run deletion while the retained CAPI Cluster and workload API are available, then check that the release and its resources are gone and the separately managed resources retain their UIDs. [Operator workflow and ownership prerequisites](magnum-operator.md#create-and-qualify-a-workload-cluster)
