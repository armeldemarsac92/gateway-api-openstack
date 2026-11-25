# HTTP ExternalAuth on Gateway listeners

Use an Experimental Gateway listener request filter to call an HTTP authorization Service before forwarding to an HTTPRoute backend. Each logical listener on a shared physical port can have its own authorization settings. Gateway API v1.7.0 exposes `spec.listeners[].filters.requests` in the Experimental CRD.

## Enable the Amphora capability

1. Install the matching Octavia and octavia-lib builds, apply Octavia migration `cadb700cba48`, and roll out the updated API, workers and Amphora image. [Octavia release](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-listener-request-filters-ef3b2204f472) · [octavia-lib release](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-listener-request-filters-0ca1468da861)
2. Set `enabled_listener_request_filter_providers = amphora` in Octavia's `[api_settings]`. Use the controller's regional endpoint and credentials to read `GET /v2/lbaas/providers/amphora/gateway_capabilities`. Verify that `gateway_capabilities` includes `listener_request_filters_v1`.
3. Install the Gateway API v1.7.0 Experimental CRDs. In the controller deployment, set `--gateway-api-channel=experimental` and add `--octavia-listener-request-filters`. The controller needs read access to Services, EndpointSlices, ReferenceGrants and BackendTLSPolicies. Configure the GatewayClass member mode and network so that the Amphora can reach the authorization Service.

## Configure an HTTP listener

This example goes under `spec.listeners` on a Gateway in the `apps` namespace. Attach an HTTPRoute using the `protected` section name. The authorization Service has a routable TCP port `8081` and accepts HTTP. With the `/authorize` prefix, a request to `/application` triggers an authorization request to `/authorize/application`. The selected response header can carry an identity to the application after authorization succeeds.

```yaml
- name: protected
  hostname: protected.example.com
  protocol: HTTP
  port: 80
  filters:
    requests:
      - type: ExternalAuth
        externalAuth:
          protocol: HTTP
          backendRef:
            namespace: identity
            name: identity
            port: 8081
          http:
            path: /authorize
            allowedResponseHeaders:
              - x-fixture-identity
```

For the cross-namespace reference, create the following ReferenceGrant in the Service's namespace. The grant authorizes the Gateway to reference this Service.

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: ReferenceGrant
metadata:
  name: gateway-identity
  namespace: identity
spec:
  from:
    - group: gateway.networking.k8s.io
      kind: Gateway
      namespace: apps
  to:
    - group: ""
      kind: Service
      name: identity
```

The local traffic qualification covers HTTP Gateway listeners, bodyless HTTP authorization to a core Service and HTTPRoute forwarding. It uses an IPv4 Endpoint member, one HTTP auth stage and two exact hostnames on a shared port.

## Forward the request body

On an Experimental `HTTP` listener, set `externalAuth.forwardBody.maxSize` to
the largest request body that the HTTP authorizer should receive, in bytes. For
example, `forwardBody: {maxSize: 32768}` allows up to 32 KiB. Omit the setting
or use zero for a bodyless check.

After qualifying the Amphora fleet for HTTP body forwarding, add
`enabled_listener_external_auth_forward_body_providers = amphora` to Octavia's
`[api_settings]`. Regional discovery must advertise
`listener_http_external_auth_forward_body_v1` and a
`l7policy_external_auth_max_body_bytes` value at least as large as the listener
setting. Octavia uses the same per-process buffer budget and published maximum
for listeners and route policies. The listener capability has its own regional
opt-in. The controller uses the existing Experimental listener filter flag.

An oversized request receives HTTP 413 before the affected check. If matching
listener and HTTPRoute filters both forward the body, the listener checks run
before route selection. Each check receives the complete body and successful
requests reach the origin once. If the region cannot honor a listener's
configured maximum, the controller programs a rejection for that hostname
and reports `Accepted=False/UnsupportedValue` for its listener section.

## Check a grant change

Read the Gateway's listener conditions and test both hostnames. The local fixture produced the following settled results.

| Grant | Protected listener | Protected hostname | Unfiltered hostname |
|---|---|---|---|
| Present | `ResolvedRefs=True`, `Programmed=True` | HTTP 200 with the fixture identity | HTTP 200 from its own Service |
| Removed | `ResolvedRefs=False` with `RefNotPermitted`, `Programmed=False` with `Invalid` | HTTP 403 | HTTP 200 from its own Service |
| Restored | `ResolvedRefs=True`, `Programmed=True` | HTTP 200 with the fixture identity | HTTP 200 from its own Service |

The HTTPRoute kept `Accepted=True`, `ResolvedRefs=True` and the controller’s `Programmed=True` condition throughout the replay. Use the Gateway’s listener conditions to check authorization admission and a traffic probe to confirm enforcement. For troubleshooting, verify the current ReferenceGrant, the authorization Service port and its ready members alongside the regional capability.

The qualification used disposable kind clusters, Octavia REST and native listener worker flows, Amphora rendering and live HAProxy. Cloud provisioning and Amphora agent transport were fixture stand-ins. The Standard control run used two ordinary HTTP listeners because the Standard Gateway CRD lacks the filters field. Qualification on a deployed Hibiscus region remains pending. [Traffic receipt](qualification/v1.7.0/provider/gateway-listener-filters-evidence/receipt.json)

If an unrelated backend policy cannot read its ClusterTrustBundle, the controller independently rechecks a listener authorization denial. It closes the affected physical port before updating the denied hostname. Other hostnames on that port retain their configuration and can resume serving when the complete graph can be reconciled. During simultaneous HTTPS trust withdrawal, `Programmed=False/Pending` records that closure is still being confirmed and `ResolvedRefs=False` records the authorization denial. The graph, provider and status regression tests cover this combination.

## Withdraw the opt-in

Remove traffic from protected HTTPRoutes or move authorization to another layer before removing their listener filters. With the provider capability still available, wait for the controller to reconcile and verify the listener and route statuses, the regional listener readback and the expected traffic. Remove the controller opt-in and retire the regional capability after the affected listener configuration has been reconciled.
