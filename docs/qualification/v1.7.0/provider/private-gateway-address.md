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

A requested address outside the class subnet, a subnet address reserved by
Neutron, or a duplicate of the requested VIP produces `Accepted=True` and
`Programmed=False` with reason `AddressNotUsable`. The condition helps the
tenant choose an available IPv4 address on the class subnet. A fixed address
requires private VIP mode. Requests containing only fixed addresses with
default routability keep the single-IPv4 admission rules on both channels.
Unsupported address types produce `Accepted=False` with reason
`UnsupportedAddress`. [Gateway API v1.7.0 conditions](https://github.com/kubernetes-sigs/gateway-api/blob/v1.7.0/apis/v1/gateway_types.go#L1364)

Octavia treats the VIP as immutable. Requesting a different fixed address on an
existing Gateway preserves the owned load balancer and routes that are still
valid. The controller reports the observed VIP and `Programmed=False` with
`AddressNotUsable` for the current generation. Restore the observed address in
`spec.addresses` to clear the condition, or use a separate Gateway for the new
address. [Octavia update request type](https://github.com/openstack/octavia/blob/19.0.0/octavia/api/v2/types/load_balancer.py)

On a bound Gateway, listener admission and route reconciliation are evaluated
independently of address assignment. A GatewayClass configured to allocate a
Floating IP publishes only its observed Floating IP as the client address.

## Experimental address routability

On the Experimental CRD channel an accepted GatewayClass advertises
`GatewayAddressRoutability`. The controller evaluates address requests
against the observed client address, the private VIP or the class's Floating
IP. It explicitly reports the empty default scope or the requested conformance
sentinel `testing.x-k8s.io/sentinel`. The sentinel carries no network
reachability guarantee. Each physical IP can satisfy a single request.
A compatible fixed request takes priority when several entries compete for
the same address. [Gateway API address semantics](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/geps/gep-5093/index.md#spec-semantics)

Once address assignment is observed, the Gateway's `AddressesAssigned`
condition gives the outcome. The success reason is `AddressesAssigned`,
the value in the published v1.7.0 Go API. `PartiallyAssigned` identifies
the entries still unsatisfied. `NotAssigned` identifies a request for which
no address could be assigned. While a previous address is still serving, the
controller keeps it in status and removes it after observing that it is no
longer active. The `Cluster` scope requires a Kubernetes Service ClusterIP
and remains unsupported. [Upstream condition reasons](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/gateway_types.go#L1289)

The supplemental envtest exercises the experimental assignment lifecycle
using a Kubernetes API server and a recording provider. It also checks that
Standard prunes the experimental field. The registered optional-address and
static-address cases run on both channels. Gateway API v1.7.0 registers no
case for this named routability feature. [Supplemental test](../../../../internal/controller/envtest_address_routability_test.go) · [Registered case entry points](../../../../internal/controller/envtest_upstream_status_test.go)

The executable extracted from image `a3d84b12b57d` also ran against fresh
Kubernetes v1.36.2 API servers with the two release bundles. Experimental
reported the requested sentinel, retained the active VIP when its new scope
was unsatisfiable and restored the default scope on the same VIP. Standard
pruned the sentinel and programmed the fixed IP. Both runs used the
controller's normal cloud client against local HTTP fixtures for Keystone,
Neutron and Octavia. [OCI image, channel receipts and replay](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-a3d84b12b57d610b14fc8c08c5c85710355c1251)

## Qualification

- Focused controller and graph tests exercise the typed Octavia create payload,
  Neutron subnet rules, duplicate and quota faults, VIP and Floating IP
  observation, and preservation of eligible routes during a rejected address edit.
- The upstream v1.7.0 `GatewayStaticAddresses` case passes in envtest with the
  Standard and Experimental release CRDs and a deterministic allocation fixture.
  It exercises live Kubernetes API-server admission, reconciliation and status.
  [Upstream case](https://github.com/kubernetes-sigs/gateway-api/blob/v1.7.0/conformance/tests/gateway-static-addresses.go)
- The offline Octavia 19.0.0 probe exercised the stock API, Amphora provider and
  Neutron driver with synthetic SDK responses. Separate Neutron 29.0.0 upstream
  tests exercised fixed IP allocation and release using the stock database IPAM
  driver. Live Neutron allocation, Amphora convergence and traffic require
  separate qualification.

The previous controller and this controller read the same saved Gateway in the local rollback check. The previous revision classified the explicit fixed request as invalid. After the operator cleared the request with the newer controller, the previous revision kept its actual address and unchanged binding without issuing a Gateway delete. [Operator rollback](../../../operator-recovery.md#roll-back-a-fixed-private-gateway-address) · [Test sources and receipts](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-455798411c13fd42f5c1568357f2c35cec2b2169/hibiscus-private-ipv4-455798411c13.tar.gz)
