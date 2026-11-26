# Use multicluster ServiceImport backends

A Gateway can route to the IPv4 VIP of a `ClusterSetIP` import or directly to the IPv4 endpoints of a `Headless` import. This opt-in supports primary `backendRefs` on `HTTPRoute`, `GRPCRoute` and `TLSRoute` with Gateway parents in the Standard and Experimental Gateway API v1.7.0 bundles.

## Prerequisites and configuration

- Use a GatewayClass with IPv4 `Endpoint` member mode and unmanaged backend security. Amphora must reach the provider’s VIP or each eligible Headless endpoint and receive return traffic. Octavia uses routed members without the Neutron subnet binding for local Pods. Verify reachability from the Amphora network before enabling the feature.
- Install an MCS provider that publishes `multicluster.x-k8s.io/v1beta1` ServiceImports. For Headless imports, the provider must publish `discovery.k8s.io/v1` EndpointSlices in the import’s namespace with `multicluster.kubernetes.io/service-name`, `multicluster.kubernetes.io/source-cluster` and the provider’s `endpointslice.kubernetes.io/managed-by` label. Restrict writes to ServiceImports and provider slices to trusted actors because these objects determine where Amphora sends traffic. A current negative or unknown MCS `Ready` condition withdraws the destinations. Providers may omit the condition. [Multi-Cluster Services API](https://github.com/kubernetes/enhancements/tree/master/keps/sig-multicluster/1645-multi-cluster-services-api#using-endpointslice-objects-to-track-endpoints)
- Use `config/multicluster-service-imports` for Standard, or add `config/components/multicluster-service-imports` to the Experimental overlay. The component enables `--multicluster-service-imports`, the existing HTTP fixed-response and weighted-target capabilities, and read-only `get`, `list`, `watch` RBAC for ServiceImports. If configuring flags directly, enable `--octavia-fixed-http-responses`, and enable `--octavia-weighted-http-targets` for rules with multiple backends. The selected Octavia region must advertise the capabilities used.
- For gRPC, enable `--octavia-backend-h2c` and use a region that advertises h2c support. The imported TCP port must declare `appProtocol: kubernetes.io/h2c`. For HTTP, this setting also selects h2c on ports that advertise it. When enabled, the GatewayClass HTTP and gRPC health monitor probes the VIP for ClusterSetIP imports and each selected member for Headless imports. The MCS provider controls endpoint selection and affinity behind the ClusterSetIP VIP.

## Reference an imported port

Replace the name and port with values from the provider’s ServiceImport. The backend namespace defaults to the Route’s namespace.

```yaml
backendRefs:
  - group: multicluster.x-k8s.io
    kind: ServiceImport
    name: catalog
    port: 8080
```

A cross-namespace import requires a ReferenceGrant in the import’s namespace. Set `from.group` to `gateway.networking.k8s.io`, specify the source namespace and its exact route kind, and set `to.group` and `to.kind` to `multicluster.x-k8s.io` and `ServiceImport`. Set `to.name` to limit authorization to one import. For TLSRoute use `from.kind: TLSRoute`. Experimental inline rule session persistence requires all backendRefs on that rule to be core Services.

For Headless imports the Route selects the ServiceImport port number. Each slice matches that port by name and TCP protocol and supplies the actual destination port. Only eligible unicast IPv4 addresses from ready, serving, non-terminating endpoints receive traffic. A homonymous core Service retains its cluster-local endpoints. An explicit `status.endpointSliceObjects: Absent` leaves a Headless backend without members. When this status field is omitted, the controller checks for provider slices. ClusterSetIP imports use their VIP even when the provider publishes slices.

An HTTP rule can mix Service and ServiceImport references with separate weights. An invalid target retains its share and returns HTTP 500, or `UNAVAILABLE` for gRPC. Check the Route’s `ResolvedRefs` condition for a missing port, missing import, unsupported transport or missing grant. When the MCS API is absent or the feature is disabled, core Service routing can still run.

A valid Headless reference with no eligible members keeps `ResolvedRefs=True`. Its share uses the existing unavailable backend behavior, including HTTP 503 or gRPC `UNAVAILABLE`. TLSRoute rejects that share under the same regional capability requirements as an invalid backend. IPv6 endpoint forwarding and MCS providers that expose Headless endpoints through another mechanism are outside this profile.

## Forward TLSRoute connections

Enable the [passthrough](operator-octavia-tls-sni-passthrough.md) or [termination](qualification/v1.7.0/provider/tlsroute-termination.md) profile for the listener. Both sections of an eligible mixed listener can use ServiceImports. Passthrough sends the client TLS stream to the imported TCP port. Termination sends the decrypted stream to the imported TCP port. Service BackendTLSPolicies apply only to core Service references.

A TLSRoute can mix Services and ServiceImports in the same weighted rule. An invalid import or a revoked grant preserves the backend’s connection share as rejection through the regional TCP connection rejection capability, or through independent weighted SNI pools if the region supports them for the selected listener mode. Enable `--octavia-tcp-connection-rejection` to use the single-pool rejection capability. When the regional provider offers neither mechanism, forwarding for that SNI hostname is withdrawn. Separate hostnames continue to use their own backend ports.
