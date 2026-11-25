# Weighted backends and fixed HTTP responses on Amphora

An HTTPRoute can keep sending traffic to each Service at its declared weight when an operator revokes or restores a cross-namespace ReferenceGrant. The Gateway controller uses two capabilities added to the Hibiscus forks of Octavia and octavia-lib. The platform operator must enable them on the cloud and on the controller.

The local qualification records the original Gateway API cases and the settled 3:1 grant lifecycle on both release bundles. [Opt-in provider results](qualification/v1.7.0/provider/opt-in-http-actions.md) · [Independent request-header capability and weighted regression](qualification/v1.7.0/provider/opt-in-request-header-modifier.md)

## Enable the capabilities

1. Install matching builds of octavia-lib and Octavia and apply the Octavia database migrations. Upgrade the API processes and the workers that render Amphora configuration across the fleet before enabling admission. Keep the previous workloads serving while completing the rollout. [Octavia fork](https://github.com/armeldemarsac92/octavia) · [octavia-lib fork](https://github.com/armeldemarsac92/octavia-lib)
2. Set `enabled_gateway_action_providers = amphora` in the `[api_settings]` section of the Octavia configuration for the selected region. Apply this configuration to the API processes. Using the controller's project credentials, read `GET /v2/lbaas/providers/amphora/gateway_capabilities` from its regional load-balancer endpoint. The `gateway_capabilities` list must include `fixed_http_responses_v1` and `weighted_http_targets_v1`.
3. For a standalone installation, select the verified controller image in an operator kustomization using `config/octavia-http-actions` as its base. For Magnum, render `kubectl kustomize integrations/magnum-capi-helm/workload-octavia-http-actions` as `gateway-controller.yaml` and pin its SHA-256 and the matching controller image in the conductor configuration. The deployment passes `--octavia-fixed-http-responses` and `--octavia-weighted-http-targets` to the controller. [Magnum operator procedure](qualification/v1.7.0/magnum-operator.md)

The standard workload and Magnum manifests continue to use the stock Octavia API. The HTTP actions component changes two controller arguments and can be composed with the operator HTTPS component in an operator-owned kustomization. The HTTPS component has its own certificate and listener requirements. [Operator HTTPS](operator-https.md)

## Enable forwarding URL rewrites

Use `--octavia-request-url-rewrite` when the effective Amphora driver on the regional endpoint advertises `request_url_rewrite_v1`. The rule matches the original request, then forwards it with the new hostname or path and the original query string. A prefix replacement uses the rule's single `PathPrefix` match. An empty `urlRewrite: {}` leaves the request unchanged.

A combined `RequestHeaderModifier` requires its own controller opt-in and regional capability. Amphora applies request header operations before the URL rewrite. Place a modifier that changes `Host` before a hostname rewrite. A later `Host` operation is supported when it sets the hostname to the same value. Other later `Host` operations produce `IncompatibleFilters`.

## Enable request filters on HTTP backend references

With weighted targets enabled, add `--octavia-http-backend-request-filters` to the controller arguments. Set `enabled_weighted_target_request_filter_providers = amphora` alongside `enabled_gateway_action_providers` in the Octavia API settings. The effective provider on the regional endpoint must advertise `weighted_http_target_request_filters_v1` along with the fixed-response and weighted-target capabilities. Rule-level request filters require their own flags and capabilities.

A `backendRef` can use `RequestHeaderModifier` and `URLRewrite`. The route matches the original request. Octavia selects the target by weight, then applies rule headers, rule URL, target headers and target URL in that order. Separate references to the same Service port can carry separate filters. Weight-zero references receive no requests. An unresolved backend or backend `ExtensionRef` keeps its weighted share as a local HTTP 500 without target transformations.

A rule and a backend reference cannot both rewrite the request path or both rewrite the hostname. A hostname rewrite and a `Host` operation cannot be combined on a filtered target. An incompatible rule is rejected while valid sibling rules can still be programmed with `PartiallyInvalid=True`.

The affected physical listener stays closed while Octavia's regional capability or the applied target filters cannot be verified. Remove or replace the filters and verify recovery before rolling back the provider.

## Enable response header modifiers on backend references

Enable `--octavia-response-header-modifier` and `--octavia-weighted-http-targets` on the controller. Enroll Amphora in `enabled_weighted_target_response_header_providers` alongside the response-header and weighted-target provider settings in Octavia's `[api_settings]`. Verify `weighted_http_target_response_headers_v1` on the effective regional provider. Response modifiers on their own require no backend request filter opt-in.

Add `type: ResponseHeaderModifier` under the selected HTTPRoute or GRPCRoute `backendRef.filters`. A single filtered backend also uses a weighted policy. Octavia applies the selected backend's operations before the rule's response operations. References to the same Service can carry different modifiers. A weight-zero or unresolved reference contributes no target response operations. CORS-owned response headers and gRPC transport and status metadata remain reserved. For comma-separated binary gRPC response metadata, the native gRPC Content-Type predicate and `grpc_binary_response_metadata_v1` are required.

The controller keeps the affected physical listener closed while it cannot verify the regional capability or the applied target operations. Check the route's `Programmed` condition and the policy's `targets[].response_header_operations` before retiring the extension.

## Enable rule-level CORS

Set `enabled_request_cors_providers = amphora` in Octavia's `[api_settings]` after installing the CORS migration, API, octavia-lib, workers and Amphora images with HAProxy 2.2 or newer. Verify that the effective provider advertises `request_cors_v1` on the controller's regional endpoint. Add `--octavia-request-cors` to the controller arguments. The flag defaults to off.

Add `type: CORS` and `cors` under an HTTPRoute forwarding rule's `filters`. Amphora applies the winning policy's origin, credentials, methods, allowed and exposed headers, and preflight max age to that rule's selected targets. Preflights use the original OPTIONS request for route matching. `cors: {}` denies all origins. Weighted rules still require their own operator opt-ins and capabilities. The provider can answer a preflight before returning an unavailable weighted target's HTTP 500 for ordinary requests.

This mode supports CORS on HTTP and terminated HTTPS forwarding policies. CORS and RequestRedirect on the same rule, backendRef CORS, and header modifiers that touch CORS request fields or overwrite CORS response fields or Vary are unsupported. The provider requires a forwarding action, so the controller rejects CORS rules without backendRefs. Enable weighted targets to handle invalid backend references with the fixed-response profile.

The controller closes the affected physical listener if discovery or policy readback cannot verify the active CORS settings. It opens the listener once the required regional capabilities, policies and rules are verified. Remove the HTTPRoute filter and confirm that Octavia no longer reports `request_cors` on the owned policies before retiring the provider extension.

## Enable HTTP request deadlines and retries

Upgrade the Octavia API, database, workers and Amphora fleet to builds that support `http_request_policy_v1`. After regional enrollment, confirm the capability in the effective provider's `GET /v2/lbaas/providers/{provider}/gateway_capabilities` response. Add `--octavia-http-request-policy` through `config/octavia-http-request-policy` or compose its component with the existing weighted target or HTTPS overlays. The option defaults to off.

An HTTPRoute forwarding rule can configure `timeouts.request`, `timeouts.backendRequest` and `retry`. A duration of `0s` disables its deadline. An omitted timeout retains the provider default. A present retry policy defaults to one additional attempt for eligible connection failures. It supports response codes 500, 502, 503 and 504. `attempts: 0` disables retries.

To configure the interval between eligible attempts, install the Amphora build supporting `http_retry_backoff_v1` on every serving peer and enable `enabled_http_retry_backoff_providers = amphora` in Octavia's `[api_settings]` alongside the request policy capability. Check the public capability response for both names. Set `retry.backoff` on the forwarding rule. For example, `220ms` waits 220 milliseconds between a failed attempt and its retry. The total request deadline includes this wait. Check `http_request_policy.retry.backoff_ms` on the active L7 policy. Zero requests an immediate retry and omission uses the provider default. [Timing observations](qualification/v1.7.0/provider/http-retry-backoff.md) · [Octavia packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-http-retry-backoff-bf5ab480c018)

Check the active Octavia policy's `http_request_policy` field and the HTTPRoute's current `Accepted` and `Programmed` conditions after writes. If the region stops advertising the capability, the controller withdraws the managed forwarding action for each affected rule. Unaffected rules stay eligible for programming. The settings apply only to the matching forwarding action. Retries after a response status or a backend deadline depend on HAProxy's request replay buffer. A request that exceeds the buffer may end without a retry. A body that fits can also be ineligible for replay if HAProxy starts sending it before receiving the full body. A deadline after the final response has started can terminate the stream.

The Experimental Service policy adds a shared retry allowance to eligible HTTP backend attempts. [Configure Service retry budgets](operator-octavia-backend-retry-budget.md)

An Experimental Gateway policy can export request spans to an OTLP collector Service. [Configure Gateway tracing](operator-octavia-telemetry.md)

## Enable request-derived redirects

On the regional load-balancer endpoint, check that the configured Amphora provider advertises `http_request_redirect_v1` in `gateway_capabilities`. Add `--octavia-http-request-redirect` to the controller arguments in the operator overlay. This option can be used independently of weighted backends and fixed HTTP responses.

An HTTPRoute `RequestRedirect` can then preserve the request hostname when `hostname` is omitted, replace the whole path, or replace the prefix of a `PathPrefix` match. Octavia preserves the query string. The selected listener supplies the scheme and port when neither is specified. An explicit scheme without a port uses its well-known port. All five redirect statuses are supported. Rules with a fixed hostname and no path modifier use `REDIRECT_PREFIX` when ordinary Octavia rules preserve precedence. If a higher-priority forwarding match adds a method or an exact header condition and needs ordered evaluation, the controller emits `REDIRECT_REQUEST` for that redirect alternative.

The controller closes a dependent physical listener while verifying new redirect policies, when removing a request-derived redirect, and when regional discovery omits the capability or times out. A full Gateway reconciliation restores that listener after the complete policy and rule inventory matches the desired configuration and any required regional discovery succeeds. Other physical listeners remain eligible to serve traffic. The ordered action uses the listener's Gateway API match precedence for forwarding and redirection. The limits on the number of positive predicates in a single Octavia policy still apply.

To withdraw the mode, remove or replace the HTTPRoute rules that need it and wait for the affected listener to reopen. Verify that its owned `REDIRECT_REQUEST` policies have disappeared before rolling back the provider API or worker packages. The controller can remove previously installed policies when the region stops advertising the capability, using the retained Octavia read, disable and delete operations.

The local kind runs cover request-derived status codes, path, scheme and port. [Case evidence and outstanding HTTPS setup](qualification/v1.7.0/provider/http-request-redirect.md)

## Create HTTP policies with their matching rules

On a region running the Hibiscus Octavia API and workers, set `[api_settings] enabled_l7policy_initial_rules_providers = amphora`. Check that the regional Amphora provider advertises `l7policy_initial_rules_v1`, then pass `--octavia-l7policy-initial-rules` to the controller. [Octavia API and worker release](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-target-request-filters-76828b5ac955)

For an HTTP or terminated HTTPS policy, the controller can create the disabled policy and its rules in one request when it has a single positive path match or at least two predicates. It reads the policy and each rule back before enabling traffic. If a create response is lost, the controller checks the listener inventory before submitting another create. Disabling the option returns new policies to individual rule creation. Policies already created with initial rules retain their readback checks.

## Activate prepared HTTP policies together

The controller can enable two to 64 forwarding policies on one HTTP listener in a single Amphora refresh. Each policy must have been created with its initial matching rules and have a ready HTTP backend and a positive path match. A hostname and exact header matches are optional. For exact headers, enable `--octavia-http-header-exact` and check that the region advertises `http_header_exact_v1`. The controller checks the listener's policy order, the predicates, the pools and their members, and the current Kubernetes references before it submits the group. A concurrent change causes a fresh read.

Apply Octavia migration `cc7a1218b24e`, upgrade the regional API and workers, and set `enabled_l7policy_batch_activation_providers = amphora` in `[api_settings]`. Workers must use the same database writer for the per-load-balancer render lock. When `GET /v2/lbaas/providers/amphora/gateway_capabilities` lists `l7policy_batch_activation_v1`, add `--octavia-l7policy-batch-activation` to the controller arguments.

HTTP 204 acknowledges submission. Follow the provisioning status of the listener and the selected policies to confirm activation. If delivery is interrupted, Octavia can resume on redelivery of the same RPC or an identical activation request. Wait for pending activations to finish before withdrawing the capability.

## Remove a retiring route policy and its rules together

Upgrade the regional Octavia API and Amphora workers to builds that support conditional L7 policy removal. Set `enabled_l7policy_conditional_removal_providers = amphora` in Octavia's `[api_settings]`. Once the effective provider advertises `l7policy_conditional_removal_v1` on the controller's regional load-balancer endpoint, add `--octavia-l7policy-conditional-removal` to the controller arguments. The option defaults to off and is independent of grouped creation and activation.

For a retiring route binding on an HTTP or terminated HTTPS listener, the controller first observes the policy disabled and settled. It compares the full policy and rule snapshot with the owned inventory and rechecks the Kubernetes binding before submitting that revision. Octavia accepts the request with HTTP 202 and applies the removal and quota release together. The controller confirms policy absence through the normal inventory on a later reconcile before completing cleanup. A conflict or an interrupted request is followed by fresh inventory reads.

The controller uses individual rule deletion with the option off or if the regional provider explicitly lacks the capability. Wait for in-flight removals to settle before rolling back the regional API or workers.

## Observe a grant change

For a route with a weight-3 Service in its own namespace and a weight-1 Service in another namespace, match requests to the same rule and distinguish replies by application identity.

| ReferenceGrant | Condition on the managed HTTPRoute parent | Responses after reconciliation |
|---|---|---|
| Present and both Services ready | `Accepted=True` and `ResolvedRefs=True` | About three quarters from the first Service and one quarter from the second |
| Removed | `Accepted=True` and `ResolvedRefs=False`, reason `RefNotPermitted` | About three quarters from the first Service and one quarter HTTP 500. The second Service receives zero requests |
| Restored | `Accepted=True` and `ResolvedRefs=True` | The second Service again handles about one quarter. The local 500 responses for the denied reference cease |

Check the same HTTPRoute UID, generation, declared weights and parent throughout. On the Octavia policy, inspect `action` and `targets`. The revoked target's pool reference becomes a fixed 500 at weight 1. On restoration a pool reference returns at weight 1. The listener's `default_http_response` supplies HTTP 404 on an unmatched request. A permitted Service that exists and has no ready endpoints remains a distinct case.

## Retire the mode

If the region stops advertising an action during a target withdrawal, the controller closes the affected listener through the standard Octavia API. This preserves route precedence and interrupts traffic on that port. When the capability returns, the controller applies the outstanding target changes before reopening the listener. An operator can follow the listener's administrative state and the Gateway's programmed condition during recovery.

Keep the patched workers, API and operator flags while withdrawing routes and Gateways that use the extension. Wait for their owned policies and listener defaults to clear and confirm cleanup against the public Octavia inventory before rolling back the workload manifest. The operator can then withdraw admission for the driver. Coordinate any database or package rollback separately under the cloud's upgrade procedure.

[Upstream deadline and retry results on both channels](qualification/v1.7.0/provider/http-request-policy.md)

## HTTP request mirroring

Enable the `octavia-http-request-mirror` controller component after the regional Amphora capability is available. [Enablement, packages and recorded request copies](qualification/v1.7.0/provider/http-request-mirror.md)

## HTTPRoute external authorization

Enable `--octavia-http-route-external-auth` with `--gateway-api-channel=experimental` and
`--octavia-fixed-http-responses`. The regional Amphora provider must advertise
`l7policy_http_external_auth_v1` to program the filter. This flag uses a
cleartext HTTP authorization Service and requires body forwarding to be disabled.
An authorization response of HTTP 200 allows the request to reach the origin.
The rule can specify an authorization path prefix and request and response header allowlists.

For an Envoy v3 gRPC authorization Service, enable
`--octavia-http-route-external-auth-grpc` with the same Experimental and
fixed-response flags. Amphora must advertise `l7policy_grpc_external_auth_v1`.
Cleartext authorizers require regional `backend_h2c_v1` support. To use an
authorizer with BackendTLSPolicy, enable `--octavia-backend-tls-policy` and use
Octavia API microversion 2.24 or later with regional hostname validation and
mandatory h2 ALPN. The rule uses the policy's trust and hostname, and the
Gateway backend client certificate when configured. An empty gRPC
request header allowlist sends all request headers. A populated list sends only
its entries. Omit `forwardBody` or set `maxSize` to zero.

Use ExternalAuth as the only filter on the rule, with unfiltered Service
backendRefs. More than one origin backend also requires
`--octavia-weighted-http-targets`.
Grant a cross-namespace authorization Service reference from the HTTPRoute's
namespace with `from.kind: HTTPRoute` in a ReferenceGrant in the Service namespace.

If that grant is withdrawn, `ResolvedRefs` becomes `False` with reason
`RefNotPermitted`. Requests matching the protected rule get a local error.
An authorization Service with no ready members also closes that rule. Unfiltered
sibling rules continue forwarding using their usual match precedence.

For multiple gRPC-protected rules on a shared HTTP listener, enable
`enabled_l7policy_grpc_auth_transition_providers` on the upgraded Octavia
API and workers. The provider advertises `l7policy_grpc_auth_transition_v1`.
When a grant is withdrawn, the controller checks the complete policy order,
ownership and matching rules. It can switch the affected rules to HTTP 500
together while the other paths keep serving. After the grant returns, it
checks the origin and authorization pools and their members before it
restores forwarding. A conflicting update prompts the controller to read the
listener again. If a worker stops during the update, recovery reinstates
the protective response. A fresh reconciliation checks current Kubernetes
references before it restores access. If the provider cannot confirm that
revoked access is blocked, the controller or worker can close the listener
until that restriction is in place.
