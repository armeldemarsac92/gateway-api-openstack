# Exact HTTPRoute header matching on Amphora

The exact-header profile programs Gateway API v1.7.0 header matches on a patched Hibiscus Amphora provider. A matching request reaches the selected backend. An unmatched request receives HTTP 404 from the listener.

## Enable the profile

1. Apply the Octavia migrations and update the API and Amphora workers to builds that support `HTTP_HEADER_EXACT` and fixed HTTP responses. Enable `enabled_http_header_exact_providers = amphora` and `enabled_gateway_action_providers = amphora` in the Octavia `[api_settings]` section.
2. Using the controller's project credentials and regional Octavia endpoint, read `GET /v2/lbaas/providers/amphora/gateway_capabilities`. The response must advertise both `http_header_exact_v1` and `fixed_http_responses_v1`. The fixed response supplies the HTTP 404 required when every rule misses. [Fixed response setup](operator-octavia-http-actions.md)
3. A standalone installation uses `config/octavia-http-header-exact`. For Magnum, render `kubectl kustomize integrations/magnum-capi-helm/workload-octavia-http-header-exact` as `gateway-controller.yaml` and pin its checksum and the controller image in the conductor configuration. Both manifests pass `--octavia-http-header-exact` and `--octavia-fixed-http-responses`. [Magnum operator procedure](qualification/v1.7.0/magnum-operator.md)

Pin the qualified controller image to `ghcr.io/armeldemarsac92/gateway-api-openstack@sha256:358069a4210a218196078fab457a717ea92eedff4d5428db3d9b3fedd5fad5af`. Use Octavia `19.0.1.dev6` from the provider release and apply migration `e9c83aa47e21` before enrolling the region. [Controller and replay](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-db720031f03f5c344be3decad81a1563dc734dde) · [Octavia wheel](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-http-header-exact-5896c054cf70) · [Both channel results](qualification/v1.7.0/provider/http-header-exact.md)

## Match limits

Amphora's HAProxy HTTP parser supports field names up to 255 bytes. The controller reports `Accepted=False` with reason `UnsupportedValue` for a 256-byte name. Exact configured values support up to 4096 characters without surrounding spaces or tabs.

Amphora runs redirects before forwarding. When a higher-priority forwarding rule intersects a redirect and adds an exact header condition that the redirect does not require, the controller leaves out that redirect alternative and reports `Accepted=False` with reason `UnsupportedValue` on its HTTPRoute. Forwarding rules stay in place. To use both actions, give them disjoint hosts or paths, or make the redirect require the same header conditions where the path guard can preserve precedence.

## Withdraw the profile

A temporary error reading provider capabilities keeps the last verified policies serving and blocks new unverified policies. When Octavia explicitly withdraws the exact-header capability, the controller closes the affected listener before removing a guarded policy. After capability recovery, the controller verifies the affected policies before reopening the listener. Withdraw the routes and verify cleanup while patched Octavia workers and the controller flags are available before rolling back the packages.
