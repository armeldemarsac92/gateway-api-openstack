# HTTP/2 between Amphora and an Experimental XBackend

An HTTPRoute on an Experimental Endpoint-mode Gateway can select cleartext HTTP/2 with `protocol: H2C` and `tls.mode: None`. Select `protocol: HTTP2` with `ServerOnly` or `ClientAndServer` to require an authenticated TLS connection negotiating `h2`. Both forms work with ready Pods selected by labels and with a DNS name authorized by the operator. [Endpoint and DNS setup](../../../reference/class-configuration.md#select-pods-with-an-experimental-xbackend) · [Backend TLS setup](../../../operator-octavia-backend-tls-policy.md#protect-an-experimental-xbackend)

| XBackend | Origin selected | Configuration | VIP status | Application requests A / B |
| --- | --- | --- | ---: | ---: |
| ExternalHostname | A | H2C | 200 | 1 / 0 |
| EndpointSelector | A | HTTP2, mutual TLS | 200 | 1 / 0 |
| ExternalHostname | A | HTTP2, server TLS | 200 | 1 / 0 |
| ExternalHostname after DNS update | B | H2C | 200 | 0 / 1 |
| ExternalHostname after DNS update | B, HTTP/1.1 over TLS | HTTP2, server TLS | 503 | 0 / 0 |

The three routes and their XBackends retained their Kubernetes identities, generations and specs. For each observed configuration, the current Gateway parent reported `Accepted=True`, `ResolvedRefs=True` and the controller's `Programmed=True`. The XBackend ancestor reported `Accepted=True`. Both DNS routes retained their Octavia pool, policy, rule and listener through the address change. The Pod receiving mutual TLS identified the enrolled client certificate. A direct request to that Pod without the certificate failed the handshake. The other TLS origin served HTTP/1.1 with the expected certificate on a direct request. The route required HTTP/2 and the application recorded zero requests there. A sibling Service returned three 200 responses in every phase. On Standard, API discovery omitted XBackend and the same ordinary Service returned three 200 responses. [Transport, route and pool observations](xbackend-http2-evidence/receipt.json)

A separate application health check used the installed Amphora templates and the same HAProxy binary with a strict h2c origin. HAProxy sent `GET /healthz` over HTTP/2 with the monitor's HTTP version omitted. When the origin changed its health response from 200 to 500 and back to 200, the VIP served 200, 503 and 200. [Transport, route and pool observations](xbackend-http2-evidence/receipt.json)

The trial ran the controller image, Kubernetes API and Pods, Octavia REST, local Barbican REST, Amphora rendering and HAProxy locally. Fixtures supplied cloud identity, Neutron, worker scheduling, Amphora transport and provisioning completion. Regional qualification is pending. [Profile ledger](../live-runs.md) · [Octavia packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-http-cookie-path-3d73d50eb78d)
