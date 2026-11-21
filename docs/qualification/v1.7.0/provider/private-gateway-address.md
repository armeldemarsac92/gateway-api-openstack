# Fixed private IPv4 addresses for Gateways

A tenant can request one fixed IPv4 address on a Gateway whose GatewayClass
uses a private VIP subnet and sets `floatingIP.mode` to `None`. The address
uses default routability. The example assumes the class selects a subnet that
contains the example address.

```yaml
spec:
  gatewayClassName: private-amphora
  addresses:
    - type: IPAddress
      value: 192.0.2.30
```

The controller checks the frozen, project-owned VIP subnet and sends both
`vip_subnet_id` and `vip_address` on the Octavia load balancer create request.
Gateway `status.addresses` comes from the observed load balancer. An omitted
address or an IPAddress entry with an empty value requests the existing
dynamic behavior, including Floating IP allocation when enabled by the class.
[Octavia create API](https://docs.openstack.org/api-ref/load-balancer/v2/#create-load-balancer)

A requested address outside the class subnet or a VIP conflict reported by
Octavia produces `Accepted=True` and `Programmed=False` with reason
`AddressNotUsable`. The condition names the problem so the tenant can choose an
available IPv4 address on the class subnet. A fixed address requires private
VIP mode. The profile supports a single IPv4 request and default routability.
Unsupported address types produce `Accepted=False` with reason
`UnsupportedAddress`. [Gateway API v1.7.0 conditions](https://github.com/kubernetes-sigs/gateway-api/blob/v1.7.0/apis/v1/gateway_types.go#L1364)

Octavia treats the VIP as immutable. Requesting a different fixed address on an
existing Gateway preserves the owned load balancer and routes that are still
valid. The controller reports the observed VIP and `Programmed=False` with
`AddressNotUsable` for the current generation. Restore the observed address in
`spec.addresses` to clear the condition, or use a separate Gateway for the new
address. [Octavia update request type](https://github.com/openstack/octavia/blob/19.0.0/octavia/api/v2/types/load_balancer.py)

## Qualification

- Focused controller and graph tests exercise the typed Octavia create payload,
  out-of-subnet rejection, Octavia's VIP-specific HTTP 409, observed status,
  preservation of a ready route and recovery after restoring the existing VIP.
- The upstream v1.7.0 `GatewayStaticAddresses` case passes in envtest with the
  Standard and Experimental release CRDs and a deterministic allocation fixture.
  It exercises live Kubernetes API-server admission, reconciliation and status.
  [Upstream case](https://github.com/kubernetes-sigs/gateway-api/blob/v1.7.0/conformance/tests/gateway-static-addresses.go)
- The separate offline Octavia 19.0.0 probe exercised the stock API, Amphora
  provider and Neutron driver with synthetic IPAM. Live Neutron allocation,
  Amphora convergence and traffic remain unverified for this change.
