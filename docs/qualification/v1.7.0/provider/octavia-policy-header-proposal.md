# Proposed Octavia API for per-route request header changes

Two routes can forward to the same pool while only one sets `X-Tenant`. Stock Octavia 19 rejected that header on the listener and rejected the insertion field on an L7 policy. The proposal gives the selected forwarding policy its own typed operation list. Octavia, octavia-lib and the controller would require changes before an operator could enable it. [Stock API observations](provider-feasibility.md#first-public-api-change-for-ingress) · [Gateway API header contract](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/httproute_types.go#L1044-L1134)

## Public API contract under study

| Operation | Proposed behavior |
|---|---|
| Discover | A new provider capability endpoint would identify support for a versioned `request_header_operations` contract on forwarding L7 policies. A controller would require the capability for the selected provider, region and applicable Amphora image fleet. Stock provider discovery lists providers and exposes flavor and availability-zone capabilities. [Existing discovery routes](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/controllers/provider.py#L39-L71) |
| Create and read | Policy POST and nested policy creation would accept an optional ordered list of typed `SET`, `ADD` and `REMOVE` operations. GET and the full graph response would return the persisted list. An omitted list on create would retain today's forwarding behavior. [Current API types](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/types/l7policy.py#L28-L151) |
| Update and clear | An omitted field on PUT would retain the list. A supplied list would replace it atomically. An empty list would clear it. Validate the complete requested action and enforce the parent project's existing write permissions before persisting a change. The storage and octavia-lib driver models would carry the distinction between omission and an empty list. [Current policy update](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/controllers/l7policy.py#L207-L299) · [Driver model](https://github.com/openstack/octavia-lib/blob/bc03e1856b60517e1b75db4fdf9b3edf55f3a019/octavia_lib/api/drivers/data_models.py#L291-L322) |

This proposed field would set the header on requests matched by one forwarding policy.

```json
{"request_header_operations":[{"operation":"SET","name":"X-Tenant","value":"blue"}]}
```

The provider would match against the original request and determine the winning policy before applying its operations. It would then forward to that policy's pool. Otherwise a mutation could change the outcome of a later predicate, or a shared pool could apply the mutation to both paths. Names compare case-insensitively. SET replaces the field's existing values, ADD preserves the values and appends, and REMOVE deletes the field. Unsupported actions, invalid header names or values, and conflicting actions on the same name must be rejected at admission. Request and response operations require separate contracts. [Stock Amphora policy rendering](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/jinja/haproxy/combined_listeners/templates/macros.j2#L144-L178)

## Acceptance and rollout

Apply two overlapping policies pointing to the same echo pool, one for an exact path and one for a prefix. Give only the exact policy the operation. Check both responses with a caller-supplied header, then clear the field and repeat. Verify API readback and fresh requests after controller restart and Amphora failover. An unsupported driver must decline the capability. A rolling upgrade must withhold the capability until each worker and image eligible for that load balancer can enforce it. An existing policy with active operations needs a verified migration or withdrawal before downgrading its provider. [Upstream policy driver entry points](https://github.com/openstack/octavia-lib/blob/bc03e1856b60517e1b75db4fdf9b3edf55f3a019/octavia_lib/api/drivers/provider_base.py#L377-L421)

The measured stock API still lacks a route-level terminal 404 or 500 and a weighted target that can return an error for only the invalid share. Those outcomes, gRPC trailers, SNI passthrough and Service-parent routing have separate dataplane requirements. [Other proposed primitives and evidence](provider-feasibility.md#architecture-decisions-available-for-qualification)
