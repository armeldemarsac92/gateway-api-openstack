# TCPRoute weights across a client identity grant change

One Experimental TCPRoute assigned weight 3 to a clear Service and weight 1 to an XBackend that required mutual TLS. The client opened a clear TCP connection to the Gateway. A second listener independently served the same Service. [Source, image and package digests, route conditions and observed traffic](receipt.json)

Each row counts 384 new connections to the mixed listener. The origin recorded the DNS name and the client certificate on each successful TLS handshake.

| Client identity grant | Service echoes | Mutual TLS echoes | Closed | Independent listener | ResolvedRefs |
| --- | ---: | ---: | ---: | ---: | --- |
| Client identity authorized | 295 | 89 | 0 | 8/8 echoes | True/ResolvedRefs |
| ReferenceGrant removed | 305 | 0 | 79 | 8/8 echoes | False/RefNotPermitted |
| ReferenceGrant restored | 283 | 101 | 0 | 8/8 echoes | True/ResolvedRefs |

Octavia reported the mixed listener's targets ACTIVE before each sample. The TCPRoute kept the same spec and generation. Accepted and the controller's Programmed condition stayed True in all three phases. Removing the ReferenceGrant in the certificate namespace replaced the protected pool with a weight-1 rejection. The origin received zero connection attempts in that phase. Restoring the grant restored the enrolled client identity and its share. Octavia kept the clear pool and the independent listener throughout.

The independent Service also returned all 24 echoes sent while configuration was changing. Each measured one-quarter share fell within the declared interval of 62 to 130 connections.

## Operator controls

With the weighted option disabled, the mixed listener closed all eight probe connections. The clear Service and a separate server-authenticated XBackend each returned eight echoes. The preceding signed release showed the same behavior. With the Standard CRDs, the Service returned eight echoes and the XBackend kind was absent. [Controller and regional settings](../../../../operator-octavia-backend-tls-policy.md#mix-authenticated-and-clear-backends-on-one-tcproute)

## Test environment

The local trial used a Kubernetes API server and etcd, the installed Octavia REST API with SQLite, Barbican over HTTPS, Amphora configuration rendering and a running HAProxy with real application sockets. Local adapters supplied Keystone, Neutron, provisioning completion, Amphora transport and service control. The measurements used one local Amphora instance. A deployed Hibiscus region and failover between two Amphora instances still require verification. [Released Octavia packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-tcp-weighted-default-pools-0e1cdf4b8599) · [Released provider library](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-tcp-weighted-default-pools-735564bb8701)
