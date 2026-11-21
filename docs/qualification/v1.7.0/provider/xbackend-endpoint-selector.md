# Experimental XBackend EndpointSelector

The v1.7.0 Experimental API admitted an `XBackend` and an `HTTPRoute` referencing it. At controller revision `2e7c32c10e406a7b5f1603dbf83ffb9341b58d31`, the Gateway accepted the Route attachment and the Route reported `ResolvedRefs=False/InvalidKind`. The recording provider received zero graph writes from the Route reconcile. These observations came from a local Kubernetes 1.36.2 API server and etcd, including the Experimental CRDs and the release's safe-upgrades admission policy and binding. No HTTP request was sent to an Octavia listener.

## Release manifests and observed reconciliation

The published `experimental-install.yaml` contains 14 CRDs, including the namespaced `xbackends.gateway.networking.x-k8s.io` serving `v1alpha1`. The 10 CRDs in `standard-install.yaml` exclude XBackend. The published Experimental XBackend and HTTPRoute CRDs matched the module copies installed in envtest. In the Experimental HTTPRoute CRD, a CEL rule requires the Route to omit `backendRef.port` for XBackend. The Backend supplies its own port in `spec.port.number`. [Experimental manifest][experimental] · [Standard manifest][standard] · [BackendObjectReference][references]

The fixture used an `Endpoint` member-mode GatewayClass and an HTTP listener on `controller-envtest/edge`. The Gateway reached `Programmed=True` through the recording provider before the Route was created. The API server returned these results after the existing `httproute.Reconciler.Reconcile` entry point ran.

| Object or output | Observed value |
| --- | --- |
| Pod `controller-envtest/xbackend-pod` | Matched `app=xbackend-echo`. The observer set `Ready=True` and the test Pod IP `192.0.2.20` through the status subresource. |
| XBackend `controller-envtest/xbackend-echo` | Admitted with `type: EndpointSelector`, `endpointSelector.matchLabels: {app: xbackend-echo}`, `port.number: 8080`, and defaulted `tls.mode: None`. The raw `status` field was absent after reconciliation. |
| HTTPRoute `controller-envtest/xbackend-route` | Admitted with `group: gateway.networking.x-k8s.io`, `kind: XBackend`, `name: xbackend-echo`, and defaulted `weight: 1`. The reference omitted both namespace and port. |
| HTTPRoute parent status | One entry for `gateway.networking.k8s.io/Gateway edge` owned by `example.com/gateway-api-openstack`. At `observedGeneration: 1`, it reported `Accepted=True/Accepted`, `ResolvedRefs=False/InvalidKind`, and controller condition `example.com/Programmed=False/Invalid`. The latter two conditions had the message `backendRef group "gateway.networking.x-k8s.io" is unsupported; use a core Service`. |
| Provider and binding | One Gateway graph write before adding the Route. Zero further graph writes, no Route pool/member/forwarding spec, and no Route-to-Gateway binding annotation. |

`validateRouteStructure` rejects the non-core backend group before Service lookup or endpoint selection. The pinned conformance module returned no matches for `xbackend` or `endpoint.?selector` across case sources, manifests, and feature declarations. The upstream `HTTPRouteInvalidBackendRefUnknownKind` case uses `unknownkind.example.com/NonExistent` and checks both `InvalidKind` and HTTP 500. The local XBackend observation establishes admission and controller status only. HTTP 500 for matching requests to an invalid backend remains unverified here. [HTTPRoute model](../../../../internal/controller/httproute/httproute_model.go) · [Upstream invalid-kind case][invalid-kind] · [HTTPRoute contract][http-route]

## Public Octavia mapping

- `EndpointSelector` supplies a nonempty AND of `matchLabels` over Pods in the Backend namespace. A reachable selected Pod IP and literal port can be expressed as an Octavia pool member with `address` and `protocol_port`, using the configured member subnet where needed. An Amphora HTTP L7 policy can forward matching requests to the pool. Pod readiness, address-family compatibility, membership changes and deletion still require reconciliation. A reachable endpoint network is a separate prerequisite. [XBackend types][types] · [Octavia member API][member-api] · [Provider and topology evidence](provider-feasibility.md)
- Generic `BackendObjectReference` rules default to the local namespace and require a target-namespace `ReferenceGrant` when crossing namespaces. The XBackend proposal additionally specifies same-namespace Route attachment. A future selector implementation must scope Pod selection and own any generated resolution resources. The pinned types describe an optional generated-name headless Service with an ownership reference as a way to obtain EndpointSlices. A managing controller must maintain its `status.ancestors` entries with an ancestor reference and controller name. `Accepted` is the defined ancestor condition. Backend deletion, selector changes and withdrawing an ancestor require separate lifecycle checks. [Reference rules][references] · [XBackend attachment proposal][proposal] · [XBackend status][types]
- `ExternalHostname` requires an external FQDN. Octavia's member API accepts an IP address, so a controller using those members would need to manage trusted DNS resolution, refresh and withdrawal as addresses change. Protocol overrides and negotiation require provider support for the requested wire protocol. TLS verification and mTLS also require hostname/trust and client-certificate reference, rotation and ownership semantics. Selector-only session persistence has its own contract. The stock gRPC study measured HTTP/1.1 upstream when configured with non-TLS pool ALPN and separately verified an HTTP/2 TLS pool. The local selector observation used the default cleartext configuration. [XBackend types][types] · [DNS and security model][proposal] · [Stock gRPC study](stock-grpc.md) · [Octavia pool API][pool-api]

## Reproduce the observation

The [TCP provider study](stock-tcp.md#reproduce-the-weighted-member-probe) links the source archive containing the observer patch and exact envtest command. Apply it in an isolated checkout at the tested revision and select the Experimental channel. The observer creates a Ready Pod status on the API server, reconciles the HTTPRoute and compares the provider graph write count with the baseline. Both envtest subtests passed. They recorded the controller's rejection and zero forwarding writes.

For the independent source search, obtain the release's separately versioned conformance module with `go list -mod=readonly -m -f '{{.Dir}}' sigs.k8s.io/gateway-api/conformance`. A case-insensitive search for `xbackends?|endpoint.?selector` returned no matches in the v1.7.0 module.

[experimental]: https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.7.0/experimental-install.yaml
[standard]: https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.7.0/standard-install.yaml
[types]: https://github.com/kubernetes-sigs/gateway-api/blob/v1.7.0/apisx/v1alpha1/xbackend_types.go
[references]: https://github.com/kubernetes-sigs/gateway-api/blob/v1.7.0/apis/v1/object_reference_types.go
[http-route]: https://github.com/kubernetes-sigs/gateway-api/blob/v1.7.0/apis/v1/httproute_types.go
[proposal]: https://github.com/kubernetes-sigs/gateway-api/blob/v1.7.0/geps/gep-4894/index.md
[invalid-kind]: https://github.com/kubernetes-sigs/gateway-api/blob/v1.7.0/conformance/tests/httproute-invalid-backendref-unknown-kind.go
[member-api]: https://docs.openstack.org/api-ref/load-balancer/v2/#create-member
[pool-api]: https://docs.openstack.org/api-ref/load-balancer/v2/#create-pool
