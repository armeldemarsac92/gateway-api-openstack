# TLS passthrough by SNI on Amphora

A dedicated Gateway can serve two TLS applications on the same port. Amphora reads the DNS name in the ClientHello and forwards the TCP stream to the matching Service. Operators can enable exact names on their own or add wildcard names for the same listener. Each application terminates TLS and presents its own certificate.

## Enable the capability

1. Use an Amphora GatewayClass with `spec.memberMode: Endpoint` on its OpenStackGatewayClassConfig. Start with a private VIP. Follow the direct endpoint network requirements so Amphora can reach the IPv4 members and their replies return to Amphora. Install Gateway API v1.7.0 in the workload cluster. Both release bundles serve `gateway.networking.k8s.io/v1` for TLSRoute and ReferenceGrant. When enabled, the controller requires the TLSRoute API at startup. [Direct endpoint network requirements](reference/class-configuration.md#direct-endpoint-members)
2. Install Octavia 19.0.1.dev4 from `0e37f9873147c3c0468edd4e3da11531d8614e6c`. Verify the wheel SHA-256 `8c0e50358ba98d44d5afa88a0c013ab22cc95d463b380b97eaa0f5d246ee324a` and apply the Octavia database migrations. Roll out matching API and worker processes across the selected region before admitting SNI rules. Use the existing Hibiscus octavia-lib build at `bb1a993ac95477b0bdc429f8aaf6d1620f8e7669`. [Octavia build](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-tls-sni-0e37f9873147) · [octavia-lib build](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-backend-h2c-bb1a993ac954)
3. Set `enabled_tls_sni_providers = amphora` in the `[api_settings]` section of the region's Octavia API configuration. With the controller's project credentials and regional load-balancer endpoint, check that `GET /v2/lbaas/providers/amphora/gateway_capabilities` returns `tls_sni_passthrough_v1` in `gateway_capabilities`.
4. For standalone, pin the matching controller image by digest in an operator kustomization based on `config/octavia-tls-sni-passthrough`. For Magnum, render and inspect the artifact below and pin its SHA-256 and the matching controller image in the conductor configuration. Select the Endpoint class for the workload. [Magnum operator setup](qualification/v1.7.0/magnum-operator.md#build-and-configure)

   The original single-backend qualification used `ghcr.io/armeldemarsac92/gateway-api-openstack@sha256:523db1c10771c4c520c2d1b8cd9e2c616b2987bf9d3cc470a7e6108c3901e0a4`. For a rule that distributes connections among Services, use the controller build in the qualification ledger. [Controller builds](qualification/v1.7.0/image-build.md) · [Exact-name qualification](qualification/v1.7.0/provider/tls-hostname-intersection.md) · [Wildcard qualification](qualification/v1.7.0/provider/tls-sni-suffix.md)

```sh
kubectl kustomize config/octavia-tls-sni-passthrough
kubectl kustomize integrations/magnum-capi-helm/workload-tls-sni-passthrough \
  > gateway-controller.yaml
sha256sum gateway-controller.yaml
```

Both overlays add `--octavia-tls-sni-passthrough`. The base manifests leave the flag off. Both bases and overlays grant the controller `get`, `list` and `watch` on TLSRoute and `get`, `update` and `patch` on its status subresource. The overlays also grant read access to BackendTLSPolicy so the controller can reject a Service port that requires backend TLS origination. Wait for the matching controller rollout and verify its arguments before admitting TLS Gateways.

## Route two names on one port

Prepare two TLS applications listening on pod port 8443. Use labels `app: tls-alpha` in `tls-demo` and `app: tls-beta` in `tls-backends`. Give each application the certificate for its own name and a distinct response body. Set `gatewayClassName` to your enrolled Endpoint class if it differs from `openstack-tls`. The [example manifest](../examples/tls-sni-passthrough.yaml) creates the namespaces, Services, Gateway, routes and the backend owner's grant. Deploy the applications into those namespaces and verify both Services have ready IPv4 EndpointSlice members.

```sh
kubectl apply -f examples/tls-sni-passthrough.yaml
kubectl -n tls-demo get gateway edge -o yaml
kubectl -n tls-demo get tlsroute alpha beta -o yaml
```

A Gateway in this mode has one `TLS` listener in `Passthrough` mode, without certificate references or TLS options. Its hostname may be omitted, an exact DNS name, or a leading wildcard such as `*.example.test`. Each TLSRoute has one Gateway parent and one rule. The rule can select up to 16 IPv4 Endpoint Services.

The controller forwards the exact names shared by the listener and the route. For example, a listener with `*.example.test` and a route with `alpha.example.test`, `alias.example.test` and `elsewhere.example.org` sends the first two names to that Service. Its backend certificate must cover both. An exact listener can also narrow a wildcard route to that listener's name. When the names are disjoint, the route reports `Accepted=False/NoMatchingListenerHostname`. To serve an intersection such as `*.shop.example.test`, enable wildcard routing in the region and on the controller.

When two routes claim an effective name, the more specific original route hostname takes precedence. Equally specific claims use creation time, then namespace and name. The chosen route must have an authorized backend before that name can serve traffic. If the winning route loses its only authorized backend, its names stay closed even when a lower-priority route also claims them. Each accepted route counts once in `attachedRoutes`.

## Distribute connections among Services

Set `spec.rules[0].backendRefs[].weight` to split new connections. For example, weights 3 and 1 give the Services three quarters and one quarter of the connections. Each Service divides its share among its ready endpoints. Each backend used in passthrough mode must present a certificate for the name the client requested. A zero weight excludes a backend from traffic while its reference still contributes to `ResolvedRefs`. The resulting per-endpoint ratio must fit Octavia's member weights of 1 through 256.

To preserve the split when a Service loses its grant or its last ready endpoint, enable `enabled_tcp_connection_rejection_providers = amphora` in Octavia's `[api_settings]` and `--octavia-tcp-connection-rejection` on the controller. The region must advertise `tcp_connection_rejection_v1` in addition to the SNI capability. With that support, losing the backend of weight 1 rejects roughly one quarter of new connections. The other Service continues serving its share. Restoring the grant or the endpoints restores the split. Without proportional rejection, the controller withdraws forwarding for the affected name. [Regional Octavia build](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-xbackend-cookie-permanent-0a0c55cbc7c3) · [Connection rejection](operator-octavia-tcp-connection-rejection.md)

On a terminating listener, multiple Services can receive plaintext TCP from Amphora. Backend re-encryption with a `BackendTLSPolicy` currently requires a rule with a single backend reference.

## Serve a wildcard TLS name

Install Octavia 19.0.1.dev5 on the API and workers in the selected region and qualify its Amphora fleet. It uses the exact-SNI database schema and the same octavia-lib wheel as the exact-name release. [Wildcard Octavia release and wheel](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-tls-sni-suffix-83653eade75a)

Keep `enabled_tls_sni_providers = amphora` and add `enabled_tls_sni_suffix_providers = amphora` in Octavia's `[api_settings]`. With the controller's project credentials, read the regional capability endpoint from step 3. Expect both `tls_sni_passthrough_v1` and `tls_sni_suffix_v1`. Then add `--octavia-tls-sni-suffix` to the controller container arguments in the standalone or Magnum workload manifest that already enables `--octavia-tls-sni-passthrough`. Both flags default to disabled. Render and pin the resulting manifest and image before rollout.

On a dedicated wildcard-enabled listener, `vip.shop.example.test` wins over `*.shop.example.test`, which wins over `*.example.test`. The name `shop.example.test` matches only the broader suffix in this example. When the narrower route loses its cross-namespace ReferenceGrant, its names close on fresh connections. The more specific exact name and the broader route's other names can still serve. Restoring the grant makes the narrower names eligible to serve again after reconciliation.

Before disabling wildcard routing or downgrading a worker, withdraw wildcard routes with the matching controller, API, workers and both capabilities still active. Verify their policies have been removed through Octavia, check that fresh connections reach only the remaining intended backends, then remove the controller suffix flag and the Octavia suffix enrollment. The region can continue to serve exact names with its exact-SNI capability.

## Upgrade an existing exact-name Gateway

The first SNI release (`6c74e8eaa940`) closed an exact name claimed by two routes. This release can start serving that name on upgrade. Check overlapping route declarations and confirm the winning route and Service before rolling out the new controller. Each effective name uses one Octavia pool, one policy, one rule and one member per resolved endpoint. Allow for those resources when adding aliases.

Before rolling back to `6c74e8eaa940`, drain Gateways that use the new hostname shapes, or restore one exact name per route and remove the listener hostname and duplicate claims. With the newer controller still running, verify the resulting parent status, pools, policies and fresh TLS connections. Then roll out the earlier image.

## Check the routes

Check the current-generation Gateway `Programmed=True` and both routes' managed parent conditions `Accepted=True`, `ResolvedRefs=True` and `gateway-api-openstack.github.io/Programmed=True` (using the configured controller's domain). The listener should report `supportedKinds` including TLSRoute and `attachedRoutes: 2`. Use a CA file that trusts both backend certificates.

```sh
vip=$(kubectl -n tls-demo get gateway edge -o jsonpath='{.status.addresses[0].value}')
for name in alpha beta; do
  curl --noproxy '*' --cacert ca.pem \
    --resolve "$name.example.test:443:$vip" "https://$name.example.test/"
done
```

Each fresh connection must validate the certificate for its requested name and return its application's identity. The listener has no default backend. An unknown name and a connection without SNI should both fail to complete a handshake.

```sh
openssl s_client -connect "$vip:443" -servername unknown.example.test </dev/null
openssl s_client -connect "$vip:443" -noservername </dev/null
```

| Change | What to check after reconciliation |
|---|---|
| Delete `tls-backends/allow-tls-demo-beta` | `beta` stays attached with `Accepted=True`, reports `ResolvedRefs=False` with reason `RefNotPermitted` and reports `Programmed=False` on the managed parent. Fresh connections for `beta` fail and `alpha` still serves. |
| Reapply the grant | `beta` returns to `ResolvedRefs=True` and `Programmed=True`, and its certificate and response return on fresh connections. |
| Delete one TLSRoute | Its SNI policy, rule and backend pool are withdrawn, the listener reports one attachment, and the surviving route still reports `Programmed=True` and serves its name. |
| Delete the Gateway and leave the surviving route | After finalization its managed parent status entry is removed and its owned Octavia resources disappear. Other controllers' parent status entries are preserved. |

This case checks exact SNI forwarding, application TLS identity, grant revocation and restoration, route withdrawal, and Gateway cleanup. Local results for this shape cover a subset of the Gateway API TLS profile. The stock Amphora baseline is recorded separately. [Stock SNI traffic](qualification/v1.7.0/provider/stock-tls-sni.md)

## Disable or recover the mode

1. Fence new TLS Gateways and TLSRoute attachments and pause changes to existing ones. Keep the SNI controller flag, CRDs, original project credentials, Octavia API and workers, and regional capability available while draining.
2. Remove the affected routes and Gateways with the controller running. Wait for Gateway finalization, verify TLSRoute parent entries for deleted Gateways are gone, and check the original regional Octavia and Neutron inventory for remaining owned resources or pending operations. Use the [ownership audit and its discovery limits](operator-recovery.md#run-the-ownership-audit). For Magnum addon removal, keep the cluster-wide fence and follow the [Magnum drain procedure](qualification/v1.7.0/magnum-operator.md#drain-before-disable-or-delete).
3. After cleanup, return to the matching base workload manifest and roll out without the flag. For an enrolled Magnum cluster that keeps the addon, pin the base artifact and image digest and re-render through the supported resize or compatible upgrade. Once every consumer is drained, remove the regional Octavia admission setting. Coordinate any package or database rollback under the cloud's upgrade procedure.

If the region stops advertising the capability, the controller attempts to close the affected listener through the standard Octavia API when it can safely identify the owned listener. This interrupts new connections on that port. Keep the SNI-aware controller and workload API available. Restore the matching Octavia API and workers in the original region and confirm capability discovery again with the same project credentials. The controller reconciles outstanding changes before it reopens the listener. Check listener administrative state, current-generation route and Gateway status, and fresh connections for both allowed and withdrawn names. If finalization is stuck, restore access to the original workload API and cloud scope and resolve inventory or ownership errors before retrying.
