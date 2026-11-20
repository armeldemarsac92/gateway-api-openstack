# Magnum opt-in for the OpenStack Gateway controller

This integration targets Magnum 23.0.0 and the magnum-capi-helm 1.4.0 source at `a8b82c2480a286a10b60ecc1d94c6cd9f1d01823`. A new cluster on an operator-selected template can run the controller alongside the stock OpenStack cloud controller. The addon manages five workload objects. The platform supplies the shared Gateway API CRDs, the OpenStackGatewayClassConfig CRD, the GatewayClass and its parameters, the workload namespace, and a dedicated Cloud Secret.

## Build and configure

1. Apply [the driver patch](../../../integrations/magnum-capi-helm/1.4.0.patch) to the exact upstream driver revision using `git apply --check` and `git apply`. Build the driver package using the normal Magnum operator packaging process. Deploy the same patched build and configuration to all conductors serving an opted-in template.
2. Mirror the exact Azimuth chart package to a read-only conductor directory as `openstack-cluster-0.10.1.tgz`. Verify SHA-256 `fa3c30070e6f9f6684bfb10641917089b5d8453d91bc566230544029a0380d9d`. Chart source revision `856f8fb` lists cluster-api-addon-provider 0.6.1 among its management dependencies. Verify that the management plane serves the `addons.stackhpc.com/v1alpha1` resource and can reach workload APIs. [Azimuth chart package](https://azimuth-cloud.github.io/capi-helm-charts/openstack-cluster-0.10.1.tgz), [management dependencies](https://github.com/azimuth-cloud/capi-helm-charts/blob/856f8fb7372615ac0d3173d9b5d91fe605c9aeac/dependencies.json)
3. From the reviewed controller revision, build the image and record its registry digest. Render the matching workload artifact with `kubectl kustomize integrations/magnum-capi-helm/workload > gateway-controller.yaml`. Place that file alongside the parent chart and record its SHA-256. The driver verifies both artifacts before handing the checked bytes to Helm. It substitutes the configured image digest and Magnum cluster UUID only.
4. Register a separate operator-owned cluster template with driver `k8s_capi_helm_v1`. Use `capi_helm_chart_version=0.10.1` on that template. Add its UUID to the conductor configuration. Magnum cluster labels and arbitrary template labels have no opt-in effect. The allowlist applies to new clusters. Changing the allowlist has no effect on an existing cluster's recorded addon state.

```ini
[capi_helm]
gateway_api_cluster_templates = <operator-template-uuid>
gateway_api_artifacts_dir = /srv/magnum/gateway-addon
gateway_api_manifest_sha256 = <gateway-controller-yaml-sha256>
gateway_api_controller_image = registry.example.test/gateway/controller@sha256:<image-sha256>
```

## Create and qualify a workload cluster

1. Create a workload cluster from the selected template. The driver keeps the stock Helm release name. It first records a management marker Secret named `gateway-addon-<cluster-uuid>` in the stock Magnum project namespace. The immutable marker data contains the cluster UUID and the expected workload credential reference. Restrict marker writes and deletes in the management namespace to the conductor and authorised operators. Preserve the marker through driver upgrades and backups.
2. Access the new workload API using the management-plane CAPI kubeconfig procedure as soon as it comes up. Create or verify the reserved workload namespace `openstack-gateway-system`. Install the Gateway API bundle supported by the chosen image and the controller's own CRD through the platform's usual owner. Set their versions from that exact controller revision. Keep both CRD owners independent of the addon. The addon's install waits for a runnable controller and can time out while these prerequisites are missing.
3. Provision an application credential dedicated to this workload cluster in its OpenStack project. Scope it to the Octavia and Neutron operations required by the chosen class. Use the existing site credential delivery mechanism to create `openstack-gateway-system/openstack-clouds` in the workload cluster. Supply the `clouds.yaml` key with an `openstack` entry, the correct region and validated TLS trust. The application credential used by CAPI and OCCM stays separate. The manifest places only the workload Secret name in Helm values and mounts the whole Secret volume without `subPath`. Protect workload Secret and controller Pod access.
4. Check that the management `Manifests` addon `<release-name>-openstack-gw` reports `Deployed` and the workload Deployment `magnum-openstack-gateway-controller` is ready. The addon targets `openstack-gateway-system` with Helm release `openstack-gw`. Its payload contains a ServiceAccount, a ClusterRole, a ClusterRoleBinding, a ConfigMap, and a Deployment with the `magnum-` prefix. Reserve those object names for this release.
5. Create a platform-owned GatewayClass with controller name `gateway-api-openstack.github.io/magnum-gateway`. Configure the first class for a private Amphora VIP, its member subnet and a NodePort backend. Verify current-generation GatewayClass, Gateway and HTTPRoute status and a known response through the VIP. Confirm that the API server and OCCM Service LBs keep their own identities. The Magnum status includes addon deployment, while traffic and provider ownership need independent checks.

Before use with active Gateways, qualify an image update and recovery with the same controller name and cluster UUID. For an application credential or TLS trust replacement, retain the old input while applying the replacement to the workload Secret. Roll the Deployment, verify the replacement is consumed and working against the original project and endpoints, and then retire the old input. Magnum credential rotation acts on CAPI's separate credential. Upgrade, rollback and both rotations require site validation.

## Drain before disable or delete

Active markers cause Magnum delete and final cleanup to stop before Helm or credential removal. An upgrade onto an unselected template also stops while the addon is active. Retain the management addon, workload API, controller, marker and dedicated credential when any drain step fails.

1. Fence allocation at the workload API. Keep the existing GatewayClasses with controller name `gateway-api-openstack.github.io/magnum-gateway` while their Gateways finalize. Block creation of new classes with that controller name or changes which would make another class select it. Use the site's admission control to deny new Gateways selecting the affected class names and deny changes by tenants or automation to those Gateways and to HTTPRoute attachments targeting them. Preserve DELETE access and the addon ServiceAccount's reconciliation and finalizer updates. Pause provisioning automation and verify with rejected workload API writes. Keep the fence through addon removal or workload API teardown.
2. Record the affected Gateway and HTTPRoute bindings and their immutable UIDs, then delete the affected Gateways and let the controller finish finalization. Remove only dedicated tenant objects you own. Keep shared Gateway API objects and shared HTTPRoutes in place. Inspect stuck finalizers and the original cloud project or region using the [controller recovery guide](../../operator-recovery.md).
3. Under the verified fence, run the recovery guide's read-only `openstack-gateway-audit` with controller name `gateway-api-openstack.github.io/magnum-gateway`, the Magnum cluster UUID, the original project and region, and Octavia microversion 2.5. Verify a complete result with no remaining cluster-owned resources or unresolved bindings. Check the audit's discovery limits against the recorded UIDs and provider inventory, including pending Octavia operations and Neutron resources. Investigate any unexplained absence or ownership conflict. With a lost workload API, preserve the credential and establish an equivalent maintenance fence and identity-scoped OpenStack inventory with the management operator. Keep teardown blocked whenever ownership, absence or the fence cannot be established.
4. Record a private teardown report identifying the cluster UUID, controller, project, region, credential reference, verified fence and cloud audit evidence. Annotate the exact cluster marker in the management project namespace with the digest of that report. This is an operator acknowledgement that the fence is in force and the owned inventory is empty. The driver validates marker ownership and the digest format. Site operators verify the evidence itself.

```sh
kubectl --context <management-context> -n <magnum-project-namespace> annotate \
  secret gateway-addon-<magnum-cluster-uuid> \
  gateway-api-openstack.github.io/cleanup-evidence=sha256:<teardown-report-sha256>
```

5. Run normal Magnum delete, or let the next planned Magnum resize or compatible template upgrade re-render without the addon. Confirm management addon removal and the workload release's absence if retaining the cluster. Retire the dedicated credential and its Secret after these observations. For a deleted cluster, retain the marker until Magnum reports `DELETE_COMPLETE`, then remove it separately. A retained cluster keeps its drained marker and stays disabled through later updates. Direct Helm rollback, marker manipulation, or management-resource deletion bypasses this workflow.

## Offline evidence and limits

Use the standard upstream test module in the patched Magnum driver checkout with the two artifacts above and Helm 3.19.4.

```sh
MAGNUM_TEST_ARTIFACTS=/path/to/artifacts MAGNUM_TEST_HELM=/path/to/helm \
  python -m unittest magnum_capi_helm.tests.test_gateway_api
```

The render test captures the pinned driver's real Helm arguments and JSON values, renders the published parent archive with the addon off and on, and reads the controller objects from the resulting `Manifests` source. Tests also cover template selection, artifact validation, and the teardown hold and acknowledgement. The offline check stops before management or workload controllers reconcile. Live install, traffic, provider isolation, rotation, interrupted operations, rollback and teardown evidence remain pending.
