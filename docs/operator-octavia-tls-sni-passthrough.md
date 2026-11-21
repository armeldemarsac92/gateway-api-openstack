# TLS passthrough by SNI on Amphora

A dedicated Gateway can serve two TLS applications on the same port. Amphora reads the exact DNS name in the ClientHello and forwards the TCP stream to the matching Service. Each application terminates TLS and presents its own certificate.

## Enable the capability

1. Use an Amphora GatewayClass with `spec.memberMode: Endpoint` on its OpenStackGatewayClassConfig. Start with a private VIP. Follow the direct endpoint network requirements so Amphora can reach the IPv4 members and their replies return to Amphora. Install Gateway API v1.7.0 in the workload cluster. Both release bundles serve `gateway.networking.k8s.io/v1` for TLSRoute and ReferenceGrant. When enabled, the controller requires the TLSRoute API at startup. [Direct endpoint network requirements](reference/class-configuration.md#direct-endpoint-members)
2. Install Octavia 19.0.1.dev4 from `0e37f9873147c3c0468edd4e3da11531d8614e6c`. Verify the wheel SHA-256 `8c0e50358ba98d44d5afa88a0c013ab22cc95d463b380b97eaa0f5d246ee324a` and apply the Octavia database migrations. Roll out matching API and worker processes across the selected region before admitting SNI rules. Use the existing Hibiscus octavia-lib build at `bb1a993ac95477b0bdc429f8aaf6d1620f8e7669`. [Octavia build](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-tls-sni-0e37f9873147) · [octavia-lib build](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-backend-h2c-bb1a993ac954)
3. Set `enabled_tls_sni_providers = amphora` in the `[api_settings]` section of the region's Octavia API configuration. With the controller's project credentials and regional load-balancer endpoint, check that `GET /v2/lbaas/providers/amphora/gateway_capabilities` returns `tls_sni_passthrough_v1` in `gateway_capabilities`.
4. For standalone, pin the matching controller image by digest in an operator kustomization based on `config/octavia-tls-sni-passthrough`. For Magnum, render and inspect the artifact below and pin its SHA-256 and the matching controller image in the conductor configuration. Select the Endpoint class for the workload. [Magnum operator setup](qualification/v1.7.0/magnum-operator.md#build-and-configure)

   Pin `ghcr.io/armeldemarsac92/gateway-api-openstack@sha256:c7bbf6bde39c2ed6fa757f909e81f160f2be1ba54430403f9f2d05e31167016e`. [Controller release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-6c74e8eaa94054527823c5c7576da96e84d5f6f1) · [TLSRoute qualification](qualification/v1.7.0/provider/tlsroute-sni.md)

```sh
kubectl kustomize config/octavia-tls-sni-passthrough
kubectl kustomize integrations/magnum-capi-helm/workload-tls-sni-passthrough \
  > gateway-controller.yaml
sha256sum gateway-controller.yaml
```

Both overlays add `--octavia-tls-sni-passthrough`. The base manifests leave the flag off. Both bases and overlays grant the controller `get`, `list` and `watch` on TLSRoute and `get`, `update` and `patch` on its status subresource. Wait for the matching controller rollout and verify its arguments before admitting TLS Gateways.

## Route two names on one port

Prepare two TLS applications listening on pod port 8443. Use labels `app: tls-alpha` in `tls-demo` and `app: tls-beta` in `tls-backends`. Give each application the certificate for its own name and a distinct response body. Set `gatewayClassName` to your enrolled Endpoint class if it differs from `openstack-tls`. The [example manifest](../examples/tls-sni-passthrough.yaml) creates the namespaces, Services, Gateway, routes and the backend owner's grant. Deploy the applications into those namespaces and verify both Services have ready IPv4 EndpointSlice members.

```sh
kubectl apply -f examples/tls-sni-passthrough.yaml
kubectl -n tls-demo get gateway edge -o yaml
kubectl -n tls-demo get tlsroute alpha beta -o yaml
```

A Gateway in this mode has one `TLS` listener in `Passthrough` mode, without certificate references or TLS options. Its hostname may be omitted, an exact DNS name, or a leading wildcard such as `*.example.test`. Each TLSRoute has one Gateway parent and one rule selecting one IPv4 Endpoint Service at weight 1.

The controller forwards the exact names shared by the listener and the route. For example, a listener with `*.example.test` and a route with `alpha.example.test`, `alias.example.test` and `elsewhere.example.org` sends the first two names to that Service. Its backend certificate must cover both. An exact listener can also narrow a wildcard route to that listener's name. When the names are disjoint, the route reports `Accepted=False/NoMatchingListenerHostname`. A remaining wildcard forwarding target requires a provider capability beyond this Octavia release.

When two routes claim an effective name, the more specific original route hostname takes precedence. Equally specific claims use creation time, then namespace and name. The chosen route must have an authorized backend before that name can serve traffic. A revoked grant closes its names even when a lower-priority route also claims them. Each accepted route counts once in `attachedRoutes`.

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
