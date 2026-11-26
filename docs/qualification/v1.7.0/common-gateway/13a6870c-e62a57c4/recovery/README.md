# Follow-up runs for the Gateway TLS and UDP profiles

The original campaign stopped during setup for these five selections. The follow-ups used the same Gateway API v1.7.0 bundles, controller, Octavia builds, HAProxy and upstream conformance executable. For UDP, the fixture removed an unused expression that read an undefined variable before invoking the executable. The TLS fixture inputs were identical. [Original attempts](../README.md)

| Profile | Channel | Subset | Upstream pass / fail / skip | Upstream exit | Fixture exit | Upstream failures |
| --- | --- | --- | --- | ---: | ---: | --- |
| [TLS](gateway-tls-experimental-extended-default.yaml) | Experimental | Extended | 16 / 1 / 1 | 1 | 1 | `BackendTLSPolicyTLSRouteTerminate` |
| [UDP](gateway-udp-standard-core-default.yaml) | Standard | Core | 20 / 1 / 0 | 1 | 1 | `UDPRouteParentRefAttachAllListeners` |
| [UDP](gateway-udp-standard-extended-default.yaml) | Standard | Extended | 11 / 0 / 0 | 0 | 1 | None |
| [UDP](gateway-udp-experimental-core-default.yaml) | Experimental | Core | 21 / 0 / 0 | 0 | 0 | None |
| [UDP](gateway-udp-experimental-extended-default.yaml) | Experimental | Extended | 11 / 0 / 0 | 0 | 1 | None |

The static-address case passed upstream in TLS Experimental Extended, UDP Standard Extended, UDP Experimental Extended. The fixture recorded allocation of the usable VIP and no allocation attempt for the occupied VIP. The Gateway starts with three addresses. It reports `Accepted=False/UnsupportedAddress` for the unsupported address type. After the test removes that address, the controller built from the signed Git revision reports `Accepted=True` and `Programmed=False/AddressNotUsable` for the occupied and usable pair because its IPv4 profile accepts one fixed address. After the test removes the occupied address, the Gateway reports `Programmed=True` and the fixture records allocation of the usable address. [Upstream address sequence](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/gateway-static-addresses.go#L83) · [Controller validation](https://github.com/armeldemarsac92/gateway-api-openstack/blob/13a6870cb63abafa131527b7b1b67048d962cd57/internal/controller/gateway/gateway_validation.go#L282)

The suite exit records the upstream executable. The fixture exit also includes its additional observations. The JSON keeps original fixture verdicts and notes extra static-address observations wherever present. The five rows describe new runs. The original reports and setup failures retain their own dates and outcomes.

TLS used native HAProxy and UDP used a local Keepalived and IPVS director. No Hibiscus region was available. [Input pins, case outcomes and original attempt mapping](results.json)
