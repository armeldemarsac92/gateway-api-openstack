# Local Gateway qualification for gRPC, TCP, TLS and UDP

The campaign pairs controller `597d921fd083bb71f9ad3fc024f5f6bf0891e0ba` and Octavia `300b2638c072c10f9f1e59403eedb792b9cd98a5`.
They use the Gateway API v1.7.0 upstream suite from `e26d183dbbc1bb782b78addbd26faba03955c152`, with default Go test parallelism and the original case deadlines.
Staging verified the Git source signatures for the controller and the Octavia wheels. The OCI archive was checked by digest.
[Inputs, selected cases and report digests](protocol-receipt.json) · [HTTP results and fixture context](README.md)

| Selection | Channel | Subset and variant | Eligible | Pass | Fail | Skip in selection | Suite / fixture exit | Report |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| 05-grpc-c | Standard | Core · default | 17 | 17 | 0 | 0 | 0 / 0 | [YAML](protocol-yaml/gateway-grpc-standard-core-default.yaml) |
| 06-grpc-e | Standard | Extended · default | 13 | 13 | 0 | 0 | 0 / 1 | [YAML](protocol-yaml/gateway-grpc-standard-extended-default.yaml) |
| 07-grpc-c | Experimental | Core · default | 17 | 17 | 0 | 0 | 0 / 0 | [YAML](protocol-yaml/gateway-grpc-experimental-core-default.yaml) |
| 08-grpc-e | Experimental | Extended · default | 13 | 13 | 0 | 0 | 0 / 1 | [YAML](protocol-yaml/gateway-grpc-experimental-extended-default.yaml) |
| 09-tcp-c | Standard | Core · default | 20 | 20 | 0 | 0 | 0 / 0 | [YAML](protocol-yaml/gateway-tcp-standard-core-default.yaml) |
| 10-tcp-e | Standard | Extended · default | 11 | 11 | 0 | 0 | 0 / 1 | [YAML](protocol-yaml/gateway-tcp-standard-extended-default.yaml) |
| 11-tcp-c | Experimental | Core · default | 20 | 20 | 0 | 0 | 0 / 0 | [YAML](protocol-yaml/gateway-tcp-experimental-core-default.yaml) |
| 12-tcp-e | Experimental | Extended · default | 11 | 11 | 0 | 0 | 0 / 1 | [YAML](protocol-yaml/gateway-tcp-experimental-extended-default.yaml) |
| 13-tls-c | Standard | Core · default | 21 | 21 | 0 | 0 | 0 / 0 | [YAML](protocol-yaml/gateway-tls-standard-core-default.yaml) |
| 14-tls-e | Standard | Extended · default | 17 | 17 | 0 | 0 | 0 / 1 | [YAML](protocol-yaml/gateway-tls-standard-extended-default.yaml) |
| 15-tls-t | Standard | Extended · terminate-only | 1 | 1 | 0 | 0 | 0 / 0 | [YAML](protocol-yaml/gateway-tls-standard-extended-terminate-only.yaml) |
| 16-tls-c | Experimental | Core · default | 21 | 21 | 0 | 0 | 0 / 0 | [YAML](protocol-yaml/gateway-tls-experimental-core-default.yaml) |
| 17-tls-e | Experimental | Extended · default | 17 | 17 | 0 | 0 | 0 / 1 | [YAML](protocol-yaml/gateway-tls-experimental-extended-default.yaml) |
| 18-tls-t | Experimental | Extended · terminate-only | 1 | 1 | 0 | 0 | 0 / 0 | [YAML](protocol-yaml/gateway-tls-experimental-extended-terminate-only.yaml) |
| 19-udp-c | Standard | Core · default | 21 | 21 | 0 | 0 | 0 / 0 | [YAML](protocol-yaml/gateway-udp-standard-core-default.yaml) |
| 20-udp-e | Standard | Extended · default | 11 | 11 | 0 | 0 | 0 / 1 | [YAML](protocol-yaml/gateway-udp-standard-extended-default.yaml) |
| 21-udp-c | Experimental | Core · default | 21 | 21 | 0 | 0 | 0 / 0 | [YAML](protocol-yaml/gateway-udp-experimental-core-default.yaml) |
| 22-udp-e | Experimental | Extended · default | 11 | 11 | 0 | 0 | 0 / 1 | [YAML](protocol-yaml/gateway-udp-experimental-extended-default.yaml) |

The 18 selections contain 264 case entries. Cases recur across channels and profiles. Each row counts only its eligible cases.
The receipt lists CLI exclusions and upstream report skips outside each selected set. Extended selections omit the Core cases.
The TLS Extended default excludes `TLSRouteListenerMixedTerminationNotSupported`. Each terminate-only row selects that single case with termination enabled and mixed mode disabled.
UDP Core declares TLSRoute for the listener rejection case and excludes nine TLSRoute cases from the selection.
Features without a selected case are listed in the receipt.

Across the four TLS Extended reports, the upstream Extended sections record six skips for cases outside the selected listener variant. Each of those reports labels its Extended result as `partial`.

During recovery, the host memory check stopped one Experimental TLS Core attempt and one Experimental UDP Extended attempt before either suite started. The first Standard UDP Core fixture also stopped before producing a usable upstream report. The UDP fixture was updated to supply the persisted Octavia load balancer project and pools to its renderer. The table records the completed attempts on the same controller and provider source revisions.

## Static address assertions

Each selection below passed the upstream `GatewayStaticAddresses` checks for the unsupported address type, the unusable address and the usable address.
The fixture additionally checks for the usable address allocation and an allocation attempt rejected with `VIPInUseException` for the unusable address.
An absent event produces `ASSERTION-NOT-OBSERVED`. The receipt preserves the upstream log digest, assertion line references and allocator entry indices.

After the upstream test removes the unsupported address type, the Gateway still requests two fixed IPv4 addresses with default routability. The controller accepts the Gateway and sets `Programmed=False/AddressNotUsable` before calling Octavia. Removing the unusable address leaves one request, and the fixture records its allocation in all eight selections. The separate controller test for a single occupied address sends the requested IP to Octavia and checks a simulated HTTP 409. A collision with a deployed allocator still requires a regional run. [Upstream sequence](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/gateway-static-addresses.go#L83) · [Controller admission at the tested revision](https://github.com/armeldemarsac92/gateway-api-openstack/blob/597d921fd083bb71f9ad3fc024f5f6bf0891e0ba/internal/controller/gateway/gateway_validation.go#L326) · [Single-address collision test](../../../../../internal/cloud/openstack/graph/gateway_address_test.go)

| Selection | Usable address allocated | Unusable address attempt observed | Suite / fixture exit |
| --- | --- | --- | --- |
| 06-grpc-e | Yes | No | 0 / 1 |
| 08-grpc-e | Yes | No | 0 / 1 |
| 10-tcp-e | Yes | No | 0 / 1 |
| 12-tcp-e | Yes | No | 0 / 1 |
| 14-tls-e | Yes | No | 0 / 1 |
| 17-tls-e | Yes | No | 0 / 1 |
| 20-udp-e | Yes | No | 0 / 1 |
| 22-udp-e | Yes | No | 0 / 1 |

The upstream YAML timestamps record 2026-11-26. The receipt preserves each timestamp and its offset.

The local kind fixture uses Octavia and Barbican. TCP, TLS and gRPC use native HAProxy, and UDP uses Keepalived/IPVS. Regional qualification is tracked separately.
[Matrix evidence boundary](../../matrix/README.md#evidence-boundary) · [Regional ledger](../../live-runs.md)
