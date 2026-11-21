# Opt-in Istio ambient mesh qualification

The [workload integration](../../../../integrations/istio-ambient/README.md) installs the pinned Istio 1.31.0 ambient profile and a Service waypoint on an operator-selected Kubernetes cluster. The platform supplies the Gateway API CRDs. Workload owners select the client and destination namespaces for ambient enrollment, then select individual Services for waypoint routing. Istio writes the Service-parent status. The independent Octavia reconcilers retain their Gateway-parent scope. [Istio release inputs](istio-waypoint-inputs.md) · [Plain kind negative control](stock-mesh.md)

## Run the upstream Core cases

Use a fresh Linux amd64 kind cluster for each channel and the exact v1.7.0 [upstream release bundle](https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.7.0). The local runner also needs the fork's class-configuration CRD and the upstream fixture's pinned `echo-advanced:v0.1.0` image. Mirror the published linux/amd64 manifest using the [image procedure in the stock control](stock-mesh.md#reproduce). Preload the pinned ambient component images or provide registry access to the kind nodes.

For an offline node, use the component repository and digest from `integrations/istio-ambient/*.yaml`. Copy the amd64 manifest and blobs anonymously, import the archive on each node, and add the exact digest reference used by the pod specification. Here `repository` is the full image repository, `digest` includes the `sha256:` prefix, and `node` is the disposable kind node.

```sh
rtk skopeo copy --src-no-creds --preserve-digests "docker://$repository@$digest" \
  "oci-archive:component.oci.tar:$repository:1.31.0-distroless"
rtk sudo podman exec -i "$node" ctr -n k8s.io images import --digests - < component.oci.tar
rtk sudo podman exec "$node" ctr -n k8s.io images tag \
  "$repository:1.31.0-distroless" "$repository@$digest"
```

Install the mesh components and waypoint class using the operator procedure. Leave the two upstream mesh fixture namespaces absent before the test so the suite can apply and remove its fixtures. The runner enrolls both upstream fixture namespaces in ambient mode. It places the operator's Service waypoint in `gateway-conformance-mesh` and assigns only `Service/echo` to it. The remaining fixture Services take the ordinary Kubernetes path without that waypoint.

Build and run from the repository root. Set `channel` to `standard` or `experimental`, `cluster` to the disposable kind name, and `kubeconfig` to the file containing context `kind-$cluster`.

```sh
rtk env CGO_ENABLED=0 go test -c -tags=envtest,kindtest -o ./ambient-mesh.test ./internal/controller
rtk env GATEWAY_API_CHANNEL="$channel" KIND_CLUSTER_NAME="$cluster" HIBISCUS_TEST_KUBECONFIG="$kubeconfig" \
  HIBISCUS_AMBIENT_WAYPOINT_MANIFEST="$PWD/integrations/istio-ambient/waypoint.yaml" \
  ./ambient-mesh.test -test.run '^TestKindAmbientMesh$' -test.v -test.timeout=12m
```

The tagged test uses the unmodified v1.7.0 test registry with the Core features of `MESH-HTTP` and `MESH-GRPC`. It starts the real fork reconcilers with a recording adapter and requires zero cloud graph operations for the mesh fixtures. The verbose log shows the upstream report per profile and channel. MeshConsumerRoute, MeshClusterIPMatching and Experimental XMesh are outside these Core selections. Report consumer-namespace isolation, direct ClusterIP behavior and XMesh separately if exercised.
