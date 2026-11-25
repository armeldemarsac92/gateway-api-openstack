# Exact request header matching on Amphora

The exact-header profile programs Gateway API v1.7.0 header matches on a patched Hibiscus Amphora provider. A matching request reaches the selected backend. An unmatched request receives HTTP 404 from the listener.

## Enable the profile

1. Apply the Octavia migrations and update the API and Amphora workers to builds that support `HTTP_HEADER_EXACT` and fixed HTTP responses. Enable `enabled_http_header_exact_providers = amphora` and `enabled_gateway_action_providers = amphora` in the Octavia `[api_settings]` section.
2. Using the controller's project credentials and regional Octavia endpoint, read `GET /v2/lbaas/providers/amphora/gateway_capabilities`. The response must advertise both `http_header_exact_v1` and `fixed_http_responses_v1`. The fixed response supplies the HTTP 404 required when every rule misses. [Fixed response setup](operator-octavia-http-actions.md)
3. A standalone installation uses `config/octavia-http-header-exact`. For Magnum, render `kubectl kustomize integrations/magnum-capi-helm/workload-octavia-http-header-exact` as `gateway-controller.yaml` and pin its checksum and the controller image in the conductor configuration. Both manifests pass `--octavia-http-header-exact` and `--octavia-fixed-http-responses`. [Magnum operator procedure](qualification/v1.7.0/magnum-operator.md)

The earlier exact-header qualification used `ghcr.io/armeldemarsac92/gateway-api-openstack@sha256:358069a4210a218196078fab457a717ea92eedff4d5428db3d9b3fedd5fad5af`. That run used Octavia `19.0.1.dev6` with migration `e9c83aa47e21`. [Earlier controller release and replay](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-db720031f03f5c344be3decad81a1563dc734dde) · [Earlier Octavia wheel](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-http-header-exact-5896c054cf70) · [Both channel results](qualification/v1.7.0/provider/http-header-exact.md)

## Match limits

The default exact-header profile accepts ASCII field names up to 255 bytes. Exact configured values support up to 4096 characters without surrounding spaces or tabs.

To combine a redirect with a higher-priority forwarding rule that adds an exact header condition, enable `--octavia-http-request-redirect` in a region advertising `http_request_redirect_v1`. A request with the matching field value reaches the forwarding backend. Other values can still receive the redirect. With this option disabled, an overlap that needs ordered evaluation gives the redirect route `Accepted=False` with reason `UnsupportedValue`. [Regional configuration](operator-octavia-http-actions.md#enable-request-derived-redirects)

The overlap qualification used `ghcr.io/armeldemarsac92/gateway-api-openstack@sha256:f3a14ec1d7a5003f9746e3fc94ea7bb3730920d152f1a0fc2f0eaeb57a908f07` and Octavia `19.0.1.dev91` (`31a05857f3b3`). Add `--octavia-http-request-redirect` to the controller arguments when enabling this combination. [Controller release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-76d28f67c8420eb281ea846670db4b50b2177d7b) · [Octavia wheel and HAProxy](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-grpc-request-mirror-31a05857f3b3) · [Standard and Experimental overlap receipts](qualification/v1.7.0/provider/http-redirect-precedence-evidence/receipt.json)

## Enable exact matches on 256-byte field names

1. Upgrade the regional Octavia API, workers and every affected Amphora to builds that support parsing and matching 256-byte HTTP field names. Apply the Octavia schema migrations before enrollment.
2. Enable `enabled_http_header_name_256_providers = amphora` alongside `enabled_http_header_exact_providers = amphora` in Octavia's `[api_settings]`. Check that the regional capability endpoint advertises both `http_header_exact_v1` and `http_header_name_256_v1`.
3. Add `--octavia-http-header-name-256` to a controller that already runs with `--octavia-http-header-exact`. The new flag defaults to off. The existing GRPCRoute h2c requirements apply to gRPC metadata matching. [gRPC setup](qualification/v1.7.0/provider/grpc-h2c-controller.md)

With both flags enabled, HTTPRoute and GRPCRoute accept exact matches on valid ASCII field names of up to 256 bytes. Without the additional flag, a 256-byte name gets `Accepted=False` with reason `UnsupportedValue`. The published header case used the original profile.

## Withdraw the profile

For names up to 255 bytes, a temporary error reading provider capabilities keeps the last verified policies serving and blocks new unverified policies. When Octavia explicitly withdraws the exact-header capability, the controller closes the affected listener before removing a guarded policy. After capability recovery, the controller verifies the affected policies before reopening the listener.

A port with an installed 256-byte rule stays closed if either capability is missing, regional discovery fails, or the opt-in is switched off. The controller can remove owned rules after verifying the listener is closed. Withdraw the routes and verify cleanup while patched Octavia workers and the controller flags are available before rolling back the packages.
