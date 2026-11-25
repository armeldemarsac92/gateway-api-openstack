# Configure Service retry budgets on Amphora

An Experimental Gateway API v1.7.0 `XBackendTrafficPolicy` can cap retries to a Service in the same namespace. The budget applies to eligible retries already allowed by the HTTP forwarding configuration. Once a retry would exceed both allowances, HAProxy returns HTTP 503 and skips the extra backend attempt. The first attempt still reaches the backend. A Service without a budget keeps its existing retry behavior.

## Enable the capability

1. Upgrade the Octavia API, octavia-lib and workers. Apply Octavia migration `6bf122d105e4` and roll out Amphora images with the `backend-retry-budget-v1` HAProxy build. [Octavia changes](https://github.com/armeldemarsac92/octavia/pull/28) · [Octavia release packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-backend-retry-budget-f1e0870b2f50) · [Driver model changes](https://github.com/armeldemarsac92/octavia-lib/pull/18) · [octavia-lib release packages](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-backend-retry-budget-ba8cc812fc69)
2. Set `enabled_backend_retry_budget_providers = amphora` under Octavia's `[api_settings]`. Verify that `GET /v2/lbaas/providers/{provider}/gateway_capabilities` advertises `backend_retry_budget_v1` on the controller's regional endpoint.
3. Install the v1.7.0 Experimental bundle, compose `config/components/experimental` into the controller overlay for policy permissions and channel selection, and add `--octavia-backend-retry-budget`. The flag defaults to off. For route-configured retry codes and attempt counts, also enable the regional request policy and controller option. The Standard bundle omits the Service policy CRD and still supports the separate HTTPRoute retry mode. [HTTPRoute deadlines and retries](operator-octavia-http-actions.md#enable-http-request-deadlines-and-retries)

## Apply and verify a budget

This example allows one retry per minute when the percentage allowance is zero. The Service and its policy share the namespace `apps`.

```yaml
apiVersion: gateway.networking.x-k8s.io/v1alpha1
kind: XBackendTrafficPolicy
metadata:
  name: checkout-retries
  namespace: apps
spec:
  targetRefs:
  - group: ""
    kind: Service
    name: checkout
  retryConstraint:
    budget:
      percent: 0
      interval: 10s
    minRetryRate:
      count: 1
      interval: 1m
```

The oldest policy targeting a Service wins, with alphabetical policy name breaking a timestamp tie. A present `retryConstraint` defaults to a 20% budget over 10 seconds and a minimum of 10 retries per second. An omitted `retryConstraint` leaves the Service without a budget. The same winning policy can also configure endpoint affinity. [Service session persistence](#configure-service-session-persistence)

Check `status.ancestors` on the policy for the managing controller and consuming Gateway. Expect `Accepted=True` for the policy's current `observedGeneration`. Check the HTTPRoute's current `Programmed` condition alongside the ACTIVE Octavia pool and its `backend_retry_budget` field to confirm programming. The field should identify the target Service UID and contain the configured allowances.

## Scope and removal

HAProxy keeps the rolling history per Service UID **within one process**. Listeners and pools in that process share it. Each replica or Amphora has its own history. A reload or failover starts a fresh history in the new process. The configured allowance therefore applies per process.

Delete the policy or remove its `retryConstraint` before disabling the capability. Verify that the pool clears `backend_retry_budget` and that the selected rule's ordinary retries resume. Keep the provider packages and controller option enabled through cleanup.

[Local Experimental lifecycle and Standard baseline receipt](qualification/v1.7.0/provider/backend-retry-budget-evidence/receipt.json)

## Configure Service session persistence

Install the v1.7.0 Experimental bundle, compose `config/components/experimental` into the controller overlay, and add `--octavia-xbackend-session-persistence`. The target must be a core Service in the policy's namespace. An HTTPRoute or GRPCRoute in another namespace also needs a ReferenceGrant for its Service reference. The Gateway must use Endpoint member mode so that an Octavia member identifies an endpoint directly.

Use `sessionPersistence` on the winning Service policy. It can coexist with `retryConstraint` in the same policy when the separate retry budget capability and controller flag are enabled. This field is deprecated in Gateway API v1.7.0.

```yaml
apiVersion: gateway.networking.x-k8s.io/v1alpha1
kind: XBackendTrafficPolicy
metadata:
  name: checkout-traffic
  namespace: apps
spec:
  targetRefs:
  - group: ""
    kind: Service
    name: checkout
  sessionPersistence:
    type: Header
    header:
      name: x-checkout-session
    absoluteTimeout: 30m
```

The effective regional Amphora provider must advertise the capabilities for the requested mode.

| Mode | Regional capabilities |
| --- | --- |
| Cookie | `http_cookie_name_v1` |
| Cookie with `absoluteTimeout` | `http_cookie_name_v1` and `http_cookie_absolute_timeout_v1` |
| Permanent cookie | The named and timed capabilities plus `http_cookie_permanent_v1` |
| Cookie with a custom path | The capabilities for the requested lifetime plus `http_cookie_path_v1` |
| Header | `http_header_persistence_v1` |

Cookie mode defaults to a session lifetime and path `/`. Omitting `cookie.name` derives a stable name from the Service UID. Give explicit cookie names distinct values across Services sharing a host. For gRPC, a client can replay the Cookie or the returned Header value as metadata on later calls. Header mode on a GRPCRoute requires a custom ASCII metadata key using letters, digits, underscores, hyphens or dots. Choose a name without the binary `-bin` suffix and outside the reserved protocol fields, such as `x-checkout-session`. A header token is scoped to its Octavia pool. Clients using more than one route or port may encounter different pools.

The policy applies to primary forwarding destinations with positive weight. The route selects a backend using its existing weights, then persistence selects an eligible endpoint in that backend's pool. Mirror delivery uses its own forwarding path without client affinity.

Check `status.ancestors` for the consuming Gateway and the current policy generation. A Gateway needs a slot in this list before the controller can program affinity for it. Check the route's current `Programmed` condition and the ACTIVE Octavia pool's `session_persistence` for programming. Verify client affinity by replaying the cookie or header against more than one eligible endpoint.

In the local Experimental qualification, a Service policy issued a signed header and pinned three HTTPRoute replays to the same Pod. After policy deletion, requests with and without the former header reached both Pods on the same pool. The witness route returned HTTP 200 throughout. The traffic run covered Header persistence for HTTPRoute in Endpoint mode. [Qualification receipt](qualification/v1.7.0/provider/service-session-persistence-evidence/receipt.json)

On an update or removal of the policy, the controller can temporarily drain the affected pool while it verifies the new configuration. Remove `sessionPersistence` and verify the pool has cleared it before disabling the controller flag or rolling back the Octavia capability. [Named cookies, timeout and provider setup](reference/class-configuration.md#keep-a-session-on-one-selected-endpoint)
