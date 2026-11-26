# Use multicluster ServiceImport backends

A Gateway can route to the IPv4 VIP of a `ClusterSetIP` import or directly to the IPv4 endpoints of a `Headless` import. This opt-in supports primary `backendRefs` on `HTTPRoute`, `GRPCRoute`, `TLSRoute`, `TCPRoute` and `UDPRoute` with Gateway parents in the Standard and Experimental Gateway API v1.7.0 bundles.

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

A cross-namespace import requires a ReferenceGrant in the import’s namespace. Set `from.group` to `gateway.networking.k8s.io`, specify the source namespace and its exact route kind, and set `to.group` and `to.kind` to `multicluster.x-k8s.io` and `ServiceImport`. Set `to.name` to limit authorization to one import. For transport routes use `from.kind: TLSRoute`, `TCPRoute` or `UDPRoute` as appropriate. Experimental inline rule session persistence requires all backendRefs on that rule to be core Services.

For Headless imports the Route selects the ServiceImport port number. Each slice matches that port by name and protocol and supplies the actual destination port. UDPRoute requires UDP on both the imported port and the matching slice port. The other routes use TCP. Only eligible unicast IPv4 addresses from ready, serving, non-terminating endpoints receive traffic. A homonymous core Service retains its cluster-local endpoints. An explicit `status.endpointSliceObjects: Absent` leaves a Headless backend without members. When this status field is omitted, the controller checks for provider slices. ClusterSetIP imports use their VIP even when the provider publishes slices.

An HTTP rule can mix Service and ServiceImport references with separate weights. An invalid target retains its share and returns HTTP 500, or `UNAVAILABLE` for gRPC. Check the Route’s `ResolvedRefs` condition for a missing port, missing import, unsupported transport or missing grant. When the MCS API is absent or the feature is disabled, core Service routing can still run.

A valid Headless reference with no eligible members keeps `ResolvedRefs=True`. Its share uses the existing unavailable backend behavior, including HTTP 503 or gRPC `UNAVAILABLE`. TLSRoute rejects that share under the same regional capability requirements as an invalid backend. IPv6 endpoint forwarding and MCS providers that expose Headless endpoints through another mechanism are outside this profile.

On both local release channels, removing and restoring the HTTP import grant preserved the weights and the independently authorized gRPC route. [Route conditions and traffic by phase](qualification/v1.7.0/provider/multicluster-service-imports/README.md)

## Mirror HTTPRoute and GRPCRoute requests

An ownerless EndpointSlice retaining the same MCS import name and source cluster stays outside the homonymous core Service when its manager label disappears. The local grant cycle recorded one primary reply per request and the copies received at each remote endpoint. [HTTP and gRPC counts and conditions](qualification/v1.7.0/provider/service-import-request-mirror/README.md)

Enable `--octavia-http-request-mirror` alongside `--multicluster-service-imports`. A mirror in a `backendRefs[].filters` list also requires `--octavia-weighted-http-targets`. Use a region that advertises the [HTTP request mirroring capabilities](qualification/v1.7.0/provider/http-request-mirror.md) or [gRPC request mirroring capabilities](getting-started.md#grpc-request-mirroring) for the route protocol. The same IPv4 Endpoint and unmanaged backend security prerequisites apply.

Add a `RequestMirror` filter under a rule or under a selected backendRef. This example sends a copy of 10% of the matching requests to the import’s TCP port.

```yaml
filters:
  - type: RequestMirror
    requestMirror:
      percent: 10
      backendRef:
        group: multicluster.x-k8s.io
        kind: ServiceImport
        name: catalog-shadow
        port: 8080
```

Each selected filter occurrence sends one copy. With a Headless import, Octavia selects one eligible endpoint from the pool. With a ClusterSetIP import, Octavia sends the copy to the VIP and the MCS provider selects the endpoint. A GRPCRoute mirror requires an h2c import port and regional h2c support. An HTTPRoute mirror uses cleartext HTTP/1.1 or h2c according to the imported port’s appProtocol. The ServiceImport profile uses cleartext transport. BackendTLSPolicy applies to core Service destinations.

A cross-namespace mirror needs a grant for the import and the exact route kind. A single ReferenceGrant can authorize an import and a homonymous core Service with two `spec.to` entries, one for each target kind. A missing or invalid import or a revoked grant removes the affected copies while the primary continues. Check `ResolvedRefs` for the reference error. An eligible Headless import without available endpoints, a zero sampling ratio or a zero-weight parent backendRef produces no copies. The controller still checks these references and reports their resolution status.

## Forward TLSRoute connections

Enable the [passthrough](operator-octavia-tls-sni-passthrough.md) or [termination](qualification/v1.7.0/provider/tlsroute-termination.md) profile for the listener. Both sections of an eligible mixed listener can use ServiceImports. Passthrough sends the client TLS stream to the imported TCP port. Termination sends the decrypted stream to the imported TCP port. Service BackendTLSPolicies apply only to core Service references.

A TLSRoute can mix Services and ServiceImports in the same weighted rule. An invalid import or a revoked grant preserves the backend’s connection share as rejection through the regional TCP connection rejection capability, or through independent weighted SNI pools if the region supports them for the selected listener mode. Enable `--octavia-tcp-connection-rejection` to use the single-pool rejection capability. When the regional provider offers neither mechanism, forwarding for that SNI hostname is withdrawn. Separate hostnames continue to use their own backend ports.

The local passthrough Gateway kept both target weights through a TLSRoute ReferenceGrant withdrawal and restoration. [SNI pool state, closed connections and origin counts](qualification/v1.7.0/provider/tlsroute-service-imports/README.md)

The local trials recorded HTTP traffic to distinct published ports, an imported gRPC origin, and TLS connection weights after the final imported endpoint disappeared. [Headless ServiceImport traffic](qualification/v1.7.0/provider/headless-service-imports/README.md)

## Forward TCPRoute connections and UDPRoute packets

A local trial exercised a 3:1 rule with a same-named Service and Headless ServiceImport. Removing the import grant left the separate control route reachable through both protocols. [TCP connections and UDP packet counts](qualification/v1.7.0/provider/transport-service-imports/README.md)

Use the same `backendRefs` example under a TCPRoute or UDPRoute rule on a listener with the matching protocol. The import and a homonymous core Service can appear in the same rule with separate weights. For example, an import at weight 3 and a Service at weight 1 give the import three quarters of the traffic.

Enable `--octavia-tcp-connection-rejection` for TCPRoute or `--octavia-udp-packet-drop` for UDPRoute in a region that advertises the corresponding proportional rejection capability. UDP packet discard requires a dedicated IPv4 Endpoint listener and uses EndpointSlice readiness for Headless imports. A missing grant, invalid import or loss of the last eligible Headless endpoint rejects the TCP share or discards the UDP share. When the applicable capability is unavailable, the controller withdraws forwarding for the affected route until it can honor those weights. A zero-weight import is still validated and contributes no traffic.

## Use a ServiceImport as a mesh route parent

For an enrolled IPv4 ClusterSetIP with one HTTP port, the producer can attach an HTTPRoute in the import’s namespace. Enable the controller projection and install the matching Hibiscus Istio pilot. The waypoint selects the producer route using the import VIP. A consumer can attach an HTTPRoute from an application namespace to that same import and port. Its response header applies to calls from that namespace. The import must be visible there in Istio. [Configuration and prerequisites](../integrations/istio-ambient/README.md#mesh-serviceimport-opt-in) · [Local responses](qualification/v1.7.0/provider/ambient-service-import-parents/README.md)
