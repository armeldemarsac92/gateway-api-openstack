# Octavia API for per-route request header changes

The Hibiscus Octavia fork stores an ordered list of typed request header
operations on each forwarding L7 policy. Two policies can share a pool while
only one modifies a request. Stock Octavia 19 rejected the corresponding
route-level fields. [Stock observations](provider-feasibility.md#first-public-api-change-for-ingress)

## Public contract

| Operation | Behavior |
|---|---|
| Discover | GET /v2/lbaas/providers/{provider}/gateway_capabilities advertises request_header_operations_v1 only for an enabled driver whose fleet operator has opted in. |
| Create | The L7 policy POST and nested listener or load balancer POST accept request_header_operations for forwarding policies on HTTP and TERMINATED_HTTPS listeners. Omit the field to create a policy without operations. |
| Read | The policy GET, list and full graph response return the stored operations. Policies created without the field omit it. A cleared list reads back as an empty array. |
| Update | PUT retains an omitted field and replaces a supplied list atomically. An empty array clears it, including during withdrawal after fleet opt-out. Clear it explicitly in the same update when switching to a non-forwarding action. Existing project permissions apply. |

```json
{
  "l7policy": {
    "request_header_operations": [
      {"operation": "SET", "name": "X-Tenant", "value": "blue"},
      {"operation": "ADD", "name": "X-Label", "value": "preview"},
      {"operation": "REMOVE", "name": "X-Old"}
    ]
  }
}
```

SET replaces all values, ADD keeps existing values and appends the supplied
one, and REMOVE deletes every value. SET and ADD require a literal value.
Names compare case-insensitively and read back lowercase. The API rejects
invalid names and values, `null`, duplicate names regardless of case, and
operations with fields incompatible with their action.

Amphora selects the winning switching policy using the original headers, path
and hostname. Only the selected pool applies that policy's operations. Rejected
and redirected requests keep their existing listener behavior, and separate
requests may select another policy on the same pool. Operator-configured
listener header insertion still runs in the backend. Weighted forwarding
applies the operations on the pool selected by the draw and leaves local
response shares unchanged. [Gateway API header filter](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/httproute_types.go#L1044-L1134)

## Operator setup

1. Apply the Octavia schema migration and run builds of Octavia and octavia-lib
   that support the field. Check every eligible API and worker instance and
   Amphora image. The renderer requires HAProxy 2.2 or newer for active policies
   with header operations.
2. Add the eligible provider name to `[api_settings] enabled_request_header_operation_providers`.
   Verify that the regional Octavia endpoint advertises
   `request_header_operations_v1` for the selected provider.
3. Set `--octavia-request-header-modifier` on the controller. This flag is
   independent of local responses and weighted target support. The controller
   verifies the effective provider before writing nonempty operations or
   enabling a policy that carries them. A capability outage leaves the affected
   route pending and still permits independent sibling pool and member changes.
   An unfiltered single-match route keeps its scalar policy and pool. Adding
   the first operation or removing the last stages a replacement for that
   route. Edits on an already grouped match update its existing policy.

To withdraw, remove the affected modifiers or delete the routes while their
controller is running. Verify readback, provisioning status and traffic before
disabling the controller flag or downgrading Octavia. Removing the provider
from the allowlist stops new use. The compatible worker still enforces stored
operations until the policy is cleared or removed.

The controller lowers one RequestHeaderModifier filter per HTTPRoute rule on
the supported forwarding matches. Response headers, backendRef filters and
header-based matching have separate support requirements. The local WSGI and
HAProxy checks exercise persisted operations and shared-pool traffic.
Upstream conformance, a rolling production fleet and Amphora failover require
separate evidence.
