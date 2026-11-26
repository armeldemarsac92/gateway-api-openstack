# Application protocols on Experimental XBackends

Four HTTPRoutes forwarded application requests to XBackends selected by Pod labels. The Gateway used a separately reachable origin port for each transport.

| XBackend | Connection to the origin | What the application observed | Pool monitor |
| --- | --- | --- | --- |
| GRPC with TLS None | h2c | Echo RPC returned OK with its original payload | TCP |
| GRPC with ServerOnly | Authenticated TLS with required ALPN `h2` | Echo RPC returned OK from the TLS origin | TCP |
| WSS with ServerOnly | Authenticated TLS with ALPN `http/1.1` | HTTP 101 and a WebSocket text frame echoed by the origin | TCP |
| MCP with TLS None | Streamable HTTP | Initialization returned 200, the initialized notification returned 202, and the tool list arrived as `text/event-stream` | TCP |

All four HTTPRoute parents reported `Accepted=True`, `ResolvedRefs=True` and `Programmed=True`. Their XBackend ancestors reported `Accepted=True`. The MCP server recognized the same application session during initialization and the tool-list exchange. Octavia reported each TCP monitor provisioned with status `ACTIVE`. A core Service returned HTTP 200 on Experimental and Standard. The Standard bundle omitted the XBackend CRD. The signed baseline controller `1052c31a` reported `UnsupportedProtocol` for each Experimental test backend.

The test used the Kubernetes API server and the manager extracted from the controller image. The local API server held the route, XBackend, trust and Pod objects. The Pod status supplied the address of application processes in an isolated network. Installed Octavia and Amphora code configured the HAProxy process that handled the application calls. The certificate authority and origin certificates were generated for the trial. Fixtures supplied cloud identity, Neutron, worker delivery, agent transport and service control.

These observations cover Gateway HTTPRoute with IPv4 EndpointSelector. DNS endpoints, mutual TLS with these protocol values, raw TCP forwarding and a deployed Hibiscus region have separate qualification boundaries. [Route conditions, pool settings and application replies](receipt.json) · [Operator configuration](../../../../reference/class-configuration.md#select-an-application-protocol-for-an-httproute)
