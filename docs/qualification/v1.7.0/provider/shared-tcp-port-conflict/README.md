# Conflicting TCP and HTTP listeners on a shared port

A Gateway initially served a TCPRoute through one TCP listener on port 9444. Adding an HTTP listener on that port made the two listeners indistinguishable. Removing the HTTP listener allowed the same TCPRoute to serve its Service again. [Local observations and pinned versions](receipt.json)

| Gateway listeners | Gateway Accepted | TCP listener Conflicted | HTTP listener Conflicted | TCPRoute Accepted | New echoes through the VIP |
| --- | --- | --- | --- | --- | ---: |
| TCP | True/Accepted | False/NoConflicts | — | True/Accepted | 3/3 |
| TCP and HTTP | False/ListenersNotValid | True/ProtocolConflict | True/ProtocolConflict | False/UnsupportedValue | 0/3 |
| TCP restored | True/Accepted | False/NoConflicts | — | True/Accepted | 3/3 |

The same TCPRoute kept its UID, generation and spec throughout. During the conflict each listener reported zero attached routes. The direct Service probe succeeded in all three phases.

## Status and traffic timing

The runner sent one new connection immediately after it observed the Gateway and route conditions. It then waited until the active HAProxy frontend stopped forwarding to the Service and opened three more connections. Times are seconds from the start of the conflict phase in each local run.

| Controller | TCP and HTTP Conflicted | First connection after status | Time of first probe | Time withdrawal was observed | Subsequent connections reaching the Service |
| --- | --- | --- | ---: | ---: | ---: |
| Previous release | False/NoConflicts | echo | 0.204 | 3.963 | 0/3 |
| Updated release | True/ProtocolConflict | refused | 7.116 | 7.139 | 0/3 |

## Distinct listeners on other ports

The controller tests cover a Gateway with a second, usable port. A whole-Gateway HTTPRoute still matched the admitted HTTP listener. The scoped TCP Gateway reported its healthy port as programmed. A route accepted by the Gateway counts against each listener selected by its parent and allowed route rules, including a conflicted listener where it cannot forward traffic. [HTTPRoute compiler test](../../../../../internal/controller/httproute/reconciler_test.go) · [Scoped Gateway status and attachment counts](../../../../../internal/controller/gateway/gateway_graph_result_test.go)

## Test environment

The Standard Gateway API bundle ran in a local Kubernetes API server with etcd. The fixture used Octavia REST and SQLite, Amphora's configuration renderer, HAProxy, a synthetic EndpointSlice and TCP sockets. Local adapters supplied Keystone, Neutron, provisioning completion, Amphora transport and service control. The traffic sample covered newly opened connections on a single local Amphora. A deployed Hibiscus region still requires qualification.
