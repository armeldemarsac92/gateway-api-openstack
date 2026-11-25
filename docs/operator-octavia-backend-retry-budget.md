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

The oldest policy targeting a Service wins, with alphabetical policy name breaking a timestamp tie. A present `retryConstraint` defaults to a 20% budget over 10 seconds and a minimum of 10 retries per second. An omitted `retryConstraint` leaves the Service without a budget. Session persistence is unsupported by this controller.

Check `status.ancestors` on the policy for the managing controller and consuming Gateway. Expect `Accepted=True` for the policy's current `observedGeneration`. Check the HTTPRoute's current `Programmed` condition alongside the ACTIVE Octavia pool and its `backend_retry_budget` field to confirm programming. The field should identify the target Service UID and contain the configured allowances.

## Scope and removal

HAProxy keeps the rolling history per Service UID **within one process**. Listeners and pools in that process share it. Each replica or Amphora has its own history. A reload or failover starts a fresh history in the new process. The configured allowance therefore applies per process.

Delete the policy or remove its `retryConstraint` before disabling the capability. Verify that the pool clears `backend_retry_budget` and that the selected rule's ordinary retries resume. Keep the provider packages and controller option enabled through cleanup.

[Local Experimental lifecycle and Standard baseline receipt](qualification/v1.7.0/provider/backend-retry-budget-evidence/receipt.json)
