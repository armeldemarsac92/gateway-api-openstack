# Envoy gRPC authorization on HTTPRoute rules

On an Experimental HTTPRoute, an operator can attach an Envoy v3 authorization Service to selected rules. The local test used an h2c authorizer, a TLS authorizer and a distinct origin Service for protected traffic.

| ReferenceGrant | Route condition | Protected requests | Public path during the change |
| --- | --- | --- | --- |
| Present | `Accepted=True` and `ResolvedRefs=True` | Allowed requests reached the origin. Refused requests returned HTTP 401 after one authorization call. | HTTP 200 |
| Removed | `Accepted=True` and `ResolvedRefs=False/RefNotPermitted` | Both protected rules returned HTTP 500 with zero authorization or origin calls. | 74/74 HTTP 200 |
| Restored | `Accepted=True` and `ResolvedRefs=True` | Both rules reached their recreated origin on allow. The h2c denial returned HTTP 401. | 76/76 HTTP 200 |

During restoration the fixture interrupted the worker's reload acknowledgement. The controller recovered the fixed HTTP 500 responses before it checked current references and restored forwarding. The table counts requests sampled on the public path during withdrawal and restoration. [Traffic, route conditions and worker transitions](grant.json)

With an operator-enrolled client certificate the TLS authorizer validated the client identity. After withdrawal the secure path returned HTTP 500 with zero peer calls. All 24 public samples returned HTTP 200. [Client identity withdrawal](identity.json)

The Standard CRD rejected the Experimental filter. The unfiltered control reached the origin. These runs used local Kubernetes, Octavia and native HAProxy. Networking and worker delivery used local fixtures. Regional deployment remains unqualified. [Standard control](standard.json) · [Builds and supplier releases](receipt.json) · [Operator configuration](../../../operator-octavia-http-actions.md#httproute-external-authorization)
