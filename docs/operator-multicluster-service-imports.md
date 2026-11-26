# Use multicluster ServiceImport backends

A Gateway can route HTTP, gRPC or TLSRoute traffic to an MCS provider’s IPv4 ClusterSetIP. This opt-in supports primary `backendRefs` on `HTTPRoute`, `GRPCRoute` and `TLSRoute` with the Standard or Experimental Gateway API v1.7.0 bundle. It applies to Gateway parents and ClusterSetIP imports.

## Prerequisites and configuration

- Use a GatewayClass with IPv4 `Endpoint` member mode and unmanaged backend security. Amphora must be able to reach the imported VIP and port, and the MCS provider’s dataplane must accept the traffic and return it to Amphora. The controller supplies the VIP directly to Octavia without a Neutron member subnet binding. Verify network reachability from the Amphora network before enabling the feature.
- Install an MCS provider that publishes `multicluster.x-k8s.io/v1beta1` ServiceImports. Restrict creation and modification of these objects to trusted MCS actors, since the controller uses the advertised IP and port as a destination. The provider selects the remote endpoints behind the VIP. A current negative or unknown MCS Ready condition withdraws the VIP until the provider reports readiness. Providers may omit the condition.
- Use `config/multicluster-service-imports` for Standard, or add `config/components/multicluster-service-imports` to the Experimental overlay. The component enables `--multicluster-service-imports`, the existing HTTP fixed-response and weighted-target capabilities, and read-only `get`, `list`, `watch` RBAC for ServiceImports. If configuring flags directly, enable `--octavia-fixed-http-responses`, and enable `--octavia-weighted-http-targets` for rules with multiple backends. The selected Octavia region must advertise the capabilities used.
- For gRPC, enable `--octavia-backend-h2c` and use a region that advertises h2c support. The imported TCP port must declare `appProtocol: kubernetes.io/h2c`. For HTTP, this setting also selects h2c on ports that advertise it. For HTTP and gRPC, the GatewayClass health monitor probes the VIP. The MCS provider controls endpoint selection and affinity behind it.

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

An HTTP rule can mix Service and ServiceImport references with separate weights. An invalid target retains its share and returns HTTP 500, or `UNAVAILABLE` for gRPC. Check the Route’s `ResolvedRefs` condition for a missing port, missing import, unsupported transport or missing grant. When the MCS API is absent or the feature is disabled, core Service routing can still run.

## Forward TLSRoute connections

Enable the [passthrough](operator-octavia-tls-sni-passthrough.md) or [termination](qualification/v1.7.0/provider/tlsroute-termination.md) profile for the listener. Both sections of an eligible mixed listener can use ServiceImports. Passthrough sends the client TLS stream to the imported TCP port. Termination sends the decrypted stream to the imported TCP port. Service BackendTLSPolicies apply only to core Service references.

A TLSRoute can mix Services and ServiceImports in the same weighted rule. An invalid import or a revoked grant preserves the backend’s connection share as rejection through the regional TCP connection rejection capability, or through independent weighted SNI pools if the region supports them for the selected listener mode. Enable `--octavia-tcp-connection-rejection` to use the single-pool rejection capability. When the regional provider offers neither mechanism, forwarding for that SNI hostname is withdrawn. Separate hostnames continue to use their own backend ports.
