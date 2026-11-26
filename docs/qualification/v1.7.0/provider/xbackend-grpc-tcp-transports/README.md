# External gRPC and TCP origins and authenticated TCP connections

Eight local executions used the same controller image and Octavia packages with the Gateway API v1.7.0 bundles. Three checked ordinary Services with Standard. Three exercised the Experimental XBackend features and two checked disabled operator options. [Image and package digests, route conditions and traffic](receipt.json)

## gRPC over h2c and authenticated HTTP/2

Two GRPCRoute methods targeted external hostnames, one over h2c and one using TLS with an enrolled CA, an exact server name and required h2 ALPN. A third method targeted a Kubernetes Service. Each cell counts replies out of three calls.

| DNS answer | h2c origin | TLS origin | Control Service |
| --- | --- | --- | --- |
| Allowed address | 3/3 xbackend | 3/3 secure | 3/3 service |
| Address outside the allowed network | 3/3 UNAVAILABLE | 3/3 UNAVAILABLE | 3/3 service |
| Allowed address restored | 3/3 xbackend | 3/3 secure | 3/3 service |

With the outside answer both external pools had zero members. Their policy and pool IDs stayed stable through both transitions. The Control Service handled all 7 extra calls during the changes. The routes kept their accepted and resolved references. An unknown method returned UNIMPLEMENTED. Standard forwarded three calls to the Service and returned UNIMPLEMENTED for the unknown method. [GRPCRoute settings](../../../../reference/class-configuration.md#forward-a-grpcroute-to-an-xbackend)

## TCP to an external hostname

The TCPRoute assigned weight 3 to a Service and weight 1 to an XBackend. Each row counts 256 fresh connections. A second listener served a control Service.

| DNS answer | Service echoes | External echoes | Closed | Control echoes |
| --- | ---: | ---: | ---: | ---: |
| Allowed address | 192 | 64 | 0 | 16/16 |
| Address outside the allowed network | 189 | 0 | 67 | 16/16 |
| Allowed address restored | 192 | 64 | 0 | 16/16 |

Octavia assigned rejection a weight of 1 in 4 while DNS was outside the allowed network. The outside address received zero Gateway connections. The control Service handled all 12 extra connections during the changes. With the hostname option disabled, the external origin received zero connections in each phase. On Standard the control Service returned 16 of 16 echoes. [External TCP hostname setup](../../../../reference/class-configuration.md#route-tcp-to-an-experimental-xbackend-hostname)

## Authenticate the origin of a TCP connection

Each TCPRoute under test used one positive backend. One XBackend required server authentication and another required a client certificate as well. The listener accepted an ordinary TCP connection. Amphora established TLS to each protected origin. An independent Service listener served clear TCP. Each cell reports the result of four fresh connections.

| Change | Server authentication | Mutual TLS | Clear Service |
| --- | --- | --- | --- |
| Valid origin certificate | 4 echoes | 4 echoes | 4 echoes |
| Server-only origin presents the wrong DNS name under the trusted CA | 4 closed | 4 echoes | 4 echoes |
| Valid server name restored | 4 echoes | 4 echoes | 4 echoes |
| Client identity ReferenceGrant removed | 4 echoes | 4 closed | 4 echoes |
| Client identity ReferenceGrant restored | 4 echoes | 4 echoes | 4 echoes |

The server-name mismatch caused 16 TLS handshake errors and delivered zero application payloads. Removing the client identity grant detached the mutual TLS pool. Its TCPRoute kept Accepted=True, changed ResolvedRefs and Programmed to False with RefNotPermitted, and made zero origin connections in the four probes. Restoring the grant restored both conditions and the four echoes used the enrolled client certificate. The clear Service returned all 7 additional echoes during transitions.

With the TCP backend TLS option disabled, the Experimental protected route reported UnsupportedValue and closed all four connections. With Standard, the XBackend CRD was absent and the Service returned all four echoes. Routing multiple independently authenticated pools in one TCP rule requires separate provider support and qualification. [TCP origin authentication settings](../../../../operator-octavia-backend-tls-policy.md#authenticate-a-tcproute-xbackend)

## Local setup

The tests used a Kubernetes API server and etcd, installed Octavia packages, the Amphora configuration renderer, a running HAProxy and local application sockets. The hostname scenarios used a private DNS server. Adapters supplied Keystone, Neutron, provisioning completion and the transport to Amphora. Regional routing, Barbican and an actual Amphora pair still require a deployed Hibiscus environment. [Octavia release](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-tcp-default-pool-tls-30dc29021685) · [octavia-lib release](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-tcp-default-pool-tls-89991fd0d6de) · [Measurements and boundaries](receipt.json)
