# Full-length HTTP routing hostnames

A local Gateway on each v1.7.0 channel served three HTTPRoutes through one HTTP listener. One route named an exact 253-character DNS hostname, one specified a leading wildcard and one used a short hostname. Each probe opened a new connection.

Kubernetes admitted the same 253-character hostname with signed controller `3b7ad5dccd7d`. That controller reported Accepted=False/UnsupportedValue for the route parent. The request received HTTP 503 with zero origin calls. The short hostname reached the origin with HTTP 200.

| Channel | Exact hostname | Wildcard hostname | Suffix apex | Neighbor of the exact hostname |
| --- | --- | --- | --- | --- |
| Standard | HTTP 200 bare, uppercase or with a numeric port (269-byte rule) | HTTP 200 with one or two labels (295-byte rule) | HTTP 503, zero origin requests | HTTP 503, zero origin requests |
| Experimental | HTTP 200 bare, uppercase or with a numeric port (269-byte rule) | HTTP 200 with one or two labels (295-byte rule) | HTTP 503, zero origin requests | HTTP 503, zero origin requests |

The short hostname returned HTTP 200 on both channels. The origin received the original Host value for every successful probe, including the uppercase and port-bearing authorities. The HTTPRoutes reported Accepted, ResolvedRefs and Programmed. Each long hostname produced a single active HOST_NAME regular expression visible through the Octavia API and in the native HAProxy configuration.

The runs used Kubernetes envtest, the signed controller extracted from its OCI image, installed Octavia packages, the Amphora driver and native HAProxy. Keystone, Neutron, worker completion and Amphora agent transport used local fixtures. Regional qualification is pending. [Requests, route conditions and artifact digests](receipt.json) · [Operator setup](../../../../providers/hibiscus-exact-hostname.md) · [Octavia release](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-long-http-hostnames-b4e94843a37e)
