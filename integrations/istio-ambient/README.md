# Istio ambient Service waypoints for Magnum workload clusters

This module installs Istio 1.31.0 on a selected workload cluster. The platform owns the Gateway API v1.7.0 bundle. Istio runs the Service waypoint through the `istio.io/mesh-controller` GatewayClass and writes Service-parent Route status as `istio.io/gateway-controller`. The [Magnum Gateway addon](../../docs/qualification/v1.7.0/magnum-operator.md) continues to handle Octavia ingress independently. Install this module from the workload API after cluster creation. Magnum template selection alone leaves workloads outside the mesh.

## Install the mesh components

Use single-stack IPv4 Linux amd64 nodes and a Kubernetes version supported by Istio 1.31.0. The module configures the CNI and ztunnel for IPv4 workload traffic. Review the Istio ambient requirements for the cluster's primary CNI, kubelet CNI paths, Pod Security Admission, and network policies before enabling it. Reserve an operator-managed `istio-system` namespace on a cluster without another Istio control plane or Istio CNI installation. Keep Gateway API CRDs under their existing owner's management. Use the same Gateway API channel throughout the workload cluster. [Istio installation requirements](https://istio.io/v1.31/docs/ambient/install/platform-prerequisites/) · [Pinned release inputs](../../docs/qualification/v1.7.0/provider/istio-waypoint-inputs.md)

Run from this directory with Bash, GNU sha256sum, Helm 3 and kubectl. Populate `chart_dir` with the original archives listed in `charts.sha256` from the [official Istio chart repository](https://blob.istio.io/istio-release/charts/). Set `kubeconfig` and `context` to the selected workload API and `channel` to `standard` or `experimental`.

```sh
rtk ./install.sh render "$chart_dir" > ambient-rendered.yaml
rtk kubectl --kubeconfig "$kubeconfig" --context "$context" create namespace istio-system
rtk ./install.sh install "$chart_dir" "$kubeconfig" "$context" "$channel"
rtk kubectl --kubeconfig "$kubeconfig" --context "$context" create -f waypoint-class.yaml
rtk kubectl --kubeconfig "$kubeconfig" --context "$context" wait \
  --for=condition=Accepted gatewayclass/istio-waypoint --timeout=120s
```

Use the namespace and class creation commands once. If either name already exists, verify ownership and configuration before proceeding. The installer verifies the chart checksums and reads the installed Gateway API version and channel before installing its four named releases. The releases add Istio's own RBAC, API and workloads. The values disable automatic creation of Istio GatewayClasses. The explicit class above selects the Service waypoint template, which provisions a ClusterIP Service. Gateway API CRDs are read-only inputs for this installer.

## Enable HTTPRoutes owned by a client namespace

Append `--consumer-routes` to the render or install command to select the Hibiscus build of Istio 1.31.0. This changes the istiod image. The workload nodes need access to the pinned linux/amd64 image. [Pilot image](consumer-routes.yaml) · [Source patch and image archive](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-istio-953e8af36734) · [Qualification](../../docs/qualification/v1.7.0/provider/ambient-waypoint.md#httproutes-owned-by-a-client-namespace)

A client namespace on an enrolled, single-network mesh can set literal response headers for calls to a Kubernetes Service. Give the HTTPRoute a parent in the destination Service namespace, one `ResponseHeaderModifier` per rule and one backend that references the same Service and port. A parent without a port uses the backend port for that rule. The waypoint selects the route using the calling workload’s authenticated namespace. Other callers continue to receive the producer’s response. Foreign rerouting, GRPCRoute consumer attachments and Services configured for ingress forwarding through a waypoint report `Accepted=False/UnsupportedValue`.

To return to the standard pilot, remove the consumer routes and verify the producer response from both namespaces. Rerun the install command with its four original arguments.

## Report mesh capabilities on Experimental

On a cluster with the Gateway API v1.7.0 Experimental bundle, use `config/istio-ambient-xmesh` as the base of the controller installation overlay. Pin an image built from a controller revision that includes `--istio-ambient-xmesh`. Use the existing cluster ID and OpenStack settings. An installation already based on Experimental can add the `config/components/istio-ambient-xmesh` component to its overlay. The component passes the flag and adds the API permissions for reporting.

For Magnum, render the dedicated workload overlay from the repository root. Record the exact rendered file's SHA-256 in `gateway_api_manifest_sha256` and the compatible controller image digest in `gateway_api_controller_image`. [Magnum addon configuration](../../docs/qualification/v1.7.0/magnum-operator.md#build-and-configure)

```sh
rtk kubectl kustomize integrations/magnum-capi-helm/workload-istio-ambient-xmesh > gateway-controller.yaml
rtk sha256sum gateway-controller.yaml
```

With kubectl v1.33.13 and Kustomize v5.6.0, the supplied overlay renders to SHA-256 `80eae0ad28037162cb853fe1a64486ee13c9655756fff66788439bf506f0a739`. Recompute it for locally modified artifacts.

The controller looks for `XMesh/hibiscus-istio-ambient` at startup and creates it if the name is free. To choose another name, add `--istio-ambient-xmesh-name=<name>` to the controller Deployment arguments. An existing XMesh keeps its spec. A resource using another identity is left to its controller, including at the configured name.

```sh
rtk kubectl --kubeconfig "$kubeconfig" --context "$context" get xmesh hibiscus-istio-ambient -o yaml
```

The XMesh controller identity is `gateway-api-openstack.github.io/istio-ambient`. Every XMesh selecting it is reconciled. `Accepted=True` at the XMesh's generation means the reporting configuration was accepted. This integration has no XMesh parameters, so setting `spec.parametersRef` produces `Accepted=False` with reason `InvalidParameters`.

`Programmed=True` and the 16 sorted `supportedFeatures` require current-generation acceptance of `GatewayClass/istio-waypoint` under `istio.io/mesh-controller`. The controller also checks the pinned modules and current rollouts of `istio-system/istiod`, `istio-cni-node` and `ztunnel`, with the expected ambient, waypoint, Gateway API and IPv4 settings. It rechecks these workloads every ten seconds and withdraws the capabilities with `Programmed=Unknown` if a required observation is missing. The two pinned pilot images advertise the same set. The consumer pilot extension supports the limited cross-namespace response header behavior described above.

The module sets `AMBIENT_ENABLED=true` and `AMBIENT_IPV6=false` directly in the CNI Pod template. Existing Pods keep their startup environment when a ConfigMap changes. For an installation that still reads these two values from a ConfigMap, rerun the module's install command with the same options. XMesh waits for the updated DaemonSet to finish rolling out before advertising features.

`Programmed` reports the observed cluster prerequisites. Verify enrollment, Service-parent Route conditions and traffic separately for each Service. [Observed XMesh conditions during istiod shutdown and recovery](../../docs/qualification/v1.7.0/provider/experimental-policies.md#xmesh-readiness-on-istio-ambient)

On Standard, use the existing controller installation and workload enrollment instructions. To turn reporting off on Experimental, remove the component or switch the base back to `config/experimental`. After the controller rollout finishes, you can delete the XMesh if it belongs to this installation. Istio continues to manage its waypoints and Service-parent Route status separately.

## Enroll workloads and bind one Service

Get consent from the owners of each client and destination namespace. Ambient mode enrolls eligible pods throughout a labeled namespace. Use namespaces without sidecar injection, existing ambient enrollment or a namespace-level waypoint binding. Set `destination_namespace`, `client_namespace` and `service` to the selected resources. Use a cluster-internal Service and declare HTTP using `appProtocol: http` and cleartext gRPC using `appProtocol: kubernetes.io/h2c` on the relevant Service ports.

```sh
kube() { rtk kubectl --kubeconfig "$kubeconfig" --context "$context" "$@"; }
kube label namespace "$destination_namespace" "$client_namespace" istio.io/dataplane-mode=ambient
kube -n "$destination_namespace" create -f waypoint.yaml
kube -n "$destination_namespace" wait \
  --for=condition=Programmed gateway/service-waypoint --timeout=120s
kube -n "$destination_namespace" rollout status deployment/service-waypoint --timeout=120s
kube -n "$destination_namespace" label service "$service" istio.io/use-waypoint=service-waypoint
```

List the namespace once if clients share the destination namespace. The Service label applies only to the selected destination. Use one waypoint per destination namespace and record the bindings before enrolling more services. Create a `gateway.networking.k8s.io/v1` HTTPRoute or GRPCRoute in the Service's namespace. In `spec.parentRefs`, set `group: ""`, `kind: Service`, `name` to the selected Service and `port` to its relevant Service port. Use the existing Service DNS name from an enrolled client. There is no external address for that traffic.

Check for `Accepted=True`, `ResolvedRefs=True`, and `ResolvedWaypoints=True` at the observed Route generation in the Service parent's status for `istio.io/gateway-controller`. Exercise a known upstream response from each enrolled namespace and a neighboring unlabeled Service before widening enrollment. Route status and an available waypoint Deployment establish control-plane readiness. The Service request establishes data-plane behavior.

## Withdraw and remove

1. Delete the owned producer Routes or remove their Service parent from shared Routes. Wait for the original backend behavior to return using the same Service DNS from an enrolled client.
2. Remove the Service binding with a JSON Patch that tests its current value. Repeat for every Service assigned to this waypoint. Check traffic again before deleting the waypoint Gateway. The test protects any concurrent reassignment of the label.
3. After the waypoint bindings and applications have drained, set `enrolled_namespace` to the recorded name of a namespace still managed by this deployment. Run the namespace patch once per enrolled namespace. The patch checks that the live mode is still `ambient` before removing the label. Verify that the affected workloads leave ambient and retain connectivity.

```sh
kube -n "$destination_namespace" patch service "$service" --type=json -p \
  '[{"op":"test","path":"/metadata/labels/istio.io~1use-waypoint","value":"service-waypoint"},{"op":"remove","path":"/metadata/labels/istio.io~1use-waypoint"}]'
kube -n "$destination_namespace" delete gateway service-waypoint --wait=true
kube patch namespace "$enrolled_namespace" --type=json -p \
  '[{"op":"test","path":"/metadata/labels/istio.io~1dataplane-mode","value":"ambient"},{"op":"remove","path":"/metadata/labels/istio.io~1dataplane-mode"}]'
```

An exclusive installation may now uninstall the three runtime releases after verifying there are no remaining Istio clients, waypoints or workloads depending on them. Retain the base release and Istio CRDs with the platform, as removing the base chart can delete API data. Retain `istio-system` and the shared Gateway API bundle. Remove `istio-waypoint` only if this deployment created the class and no Gateway references it.

```sh
rtk helm uninstall hibiscus-ambient-ztunnel hibiscus-ambient-cni hibiscus-ambient-istiod \
  --kubeconfig "$kubeconfig" --kube-context "$context" --namespace istio-system --wait --timeout 5m
```

The operator must qualify the site's CNI, workload policies and provider topology before using this module on deployed clusters. Istio 1.31.0's published mesh report uses Gateway API v1.6.0. [Local v1.7.0 qualification](../../docs/qualification/v1.7.0/provider/ambient-waypoint.md) · [Reference implementations](../../docs/qualification/v1.7.0/reference-implementations.md)

## Mesh ServiceImport opt-in

Append `--service-import-parents` to the render or install command to select the Hibiscus pilot with ServiceImport parent support. This image also includes consumer HTTPRoutes. The option enables `PILOT_ENABLE_AMBIENT_MCS_HTTPROUTE` and disables Istio’s classic MCS host registration. [Pinned pilot image](service-import-parents.yaml) · [Signed source and build inputs](https://github.com/armeldemarsac92/gateway-api-openstack/blob/82213f2b4cf5996d628272ed99cedf90914eaa5f/istio-serviceimport-source.json)

Include the `config/components/istio-ambient-service-imports` Kustomize component in your deployment overlay, with either `config/default` or `config/experimental` as the base. The component adds `--istio-ambient-service-imports` and the corresponding Kubernetes permissions. The controller discovers the optional CRDs at runtime. They can be installed after startup. The MCS ingress backend and XMesh flags are independent.

Use an MCS provider that publishes authoritative IPv4 EndpointSlices and exposes `status.endpointSliceObjects: Present` on its ServiceImport. Keep Istio's classic `ENABLE_MCS_HOST` disabled when using this projector so that a single Istio registry publishes the ClusterSetIP host. Ensure the ClusterSetIP differs from any core or provider-derived Service VIP and the endpoint addresses are directly reachable from the waypoint in the same mesh network and region. Set the ServiceImport annotation `gateway-api-openstack.github.io/mcs-endpoint-slice-managed-by` to the exact `endpointslice.kubernetes.io/managed-by` value of the trusted provider after checking those conditions. Manage enrollment through your MCS provider configuration if it rewrites import metadata.

An enrolled import must declare a single IPv4 `ClusterSetIP` and a single HTTP port over TCP. Its `internalTrafficPolicy` must be omitted or set to `Cluster` because a shared waypoint cannot enforce node-local forwarding. Declare `appProtocol: http` or use an `http` or `http-*` port name with an empty appProtocol. Set `istio.io/use-waypoint` on the ServiceImport to a waypoint that serves service traffic. For a waypoint in another namespace, also set `istio.io/use-waypoint-namespace` and allow the import's namespace in the waypoint listener. Istio resolves the binding and reports its status. The projector copies the explicit waypoint labels and records the enrolled slice provider in the ServiceEntry annotation `gateway-api-openstack.github.io/mcs-endpoint-slice-managed-by`.

Imported slices must carry the MCS service name, an exporting cluster listed in the ServiceImport status, and a controller owner reference to the import's current UID. A slice can also be labeled for a provider-derived core Service. The controller uses the matched TCP port's address and target port from the slice. Losing the last eligible endpoint leaves an empty static ServiceEntry. Losing the authoritative inventory, removing enrollment, deleting the import, or finding an overlapping host or VIP withdraws the projection. During provider slice replacement, duplicate records for the same cluster, address and port keep the endpoint available while either slice reports it ready. If the controller cannot list source objects needed to determine safe membership, it withdraws its own projections and retries. Once the inventories are readable, eligible imports are projected again. Rejection reasons for enrolled imports appear in the controller's verbose logs.

Create the producer `HTTPRoute` in the same namespace as the import. Set its parent’s `group` to `multicluster.x-k8s.io`, `kind` to `ServiceImport`, and `name` to the import. An exact path can forward to another Service. For unmatched paths, add a rule whose backend is the same ServiceImport and port. A same-named core Service retains its separate VIP and routing. For a shared waypoint, the route must be visible in its namespace. Inspect the route’s `status.parents` entry from `istio.io/gateway-controller` for attachment and reference resolution.

Each projection is a `networking.istio.io/v1` ServiceEntry with host `<name>.<namespace>.svc.clusterset.local` and resolution `STATIC` in `MESH_INTERNAL`. Its namespace equals the ServiceImport's, and its object name starts with `hibiscus-mcs-` followed by the first 32 lowercase hexadecimal characters of SHA-256 of the import name. Its `gateway-api-openstack.github.io/managed-by` label is `hibiscus-mcs-projector`. Its sole owner reference has `controller: true` and identifies the ServiceImport by its name, UID and served MCS API version. The projected port retains the imported name and uses `http` for an unnamed import port. Reserve this ownership marker and these generated names for the projector.

To turn off the integration, remove the enrollment annotation from every enrolled ServiceImport and keep the controller and its permissions in place until the owned ServiceEntries have been withdrawn. To verify the withdrawal, list ServiceEntries with `kubectl get serviceentries.networking.istio.io -A -l gateway-api-openstack.github.io/managed-by=hibiscus-mcs-projector` and check the owner references for any remaining objects. Remove the component or flag and its associated RBAC after the withdrawal. If the flag was removed early, restore the same controller configuration long enough to finish withdrawal. Redeploy the waypoint configuration only after Istio has observed the removals.

The controller writes Kubernetes ServiceEntries. End-to-end mesh traffic still requires the compatible Hibiscus Istio driver, waypoint and data plane, reachable endpoints and an authentication policy compatible with the provider's endpoints.
