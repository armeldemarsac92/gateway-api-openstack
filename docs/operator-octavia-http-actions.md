# Weighted backends and fixed HTTP responses on Amphora

An HTTPRoute can keep sending traffic to each Service at its declared weight when an operator revokes or restores a cross-namespace ReferenceGrant. The Gateway controller uses two capabilities added to the Hibiscus forks of Octavia and octavia-lib. The platform operator must enable them on the cloud and on the controller.

## Enable the capabilities

1. Install matching builds of octavia-lib and Octavia and apply the Octavia database migrations. Upgrade the API processes and the workers that render Amphora configuration across the fleet before enabling admission. Keep the previous workloads serving while completing the rollout. [Octavia fork](https://github.com/armeldemarsac92/octavia) · [octavia-lib fork](https://github.com/armeldemarsac92/octavia-lib)
2. Set `enabled_gateway_action_providers = amphora` in the `[api_settings]` section of the Octavia configuration for the selected region. Apply this configuration to the API processes. Using the controller's project credentials, read `GET /v2/lbaas/providers/amphora/gateway_capabilities` from its regional load-balancer endpoint. The `gateway_capabilities` list must include `fixed_http_responses_v1` and `weighted_http_targets_v1`.
3. For a standalone installation, select the verified controller image in an operator kustomization using `config/octavia-http-actions` as its base. For Magnum, render `kubectl kustomize integrations/magnum-capi-helm/workload-octavia-http-actions` as `gateway-controller.yaml` and pin its SHA-256 and the matching controller image in the conductor configuration. The deployment passes `--octavia-fixed-http-responses` and `--octavia-weighted-http-targets` to the controller. [Magnum operator procedure](qualification/v1.7.0/magnum-operator.md)

The standard workload and Magnum manifests continue to use the stock Octavia API. The HTTP actions component changes two controller arguments and can be composed with the operator HTTPS component in an operator-owned kustomization. The HTTPS component has its own certificate and listener requirements. [Operator HTTPS](operator-https.md)

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
