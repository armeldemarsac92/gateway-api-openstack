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
