# Exact matches on 256-byte HTTP field names

With the regional Amphora capability enabled, an HTTPRoute selected its backend using a mixed-case field name of 256 ASCII bytes. A GRPCRoute did the same using lowercase HTTP/2 metadata. The value in both requests was `Match,full`. The matching backend received the complete field.

| Channel | HTTP/1.1 exact | gRPC exact | Value case change, partial value, shortened name or missing field | Long-name ports after withdrawal | Independent 255-byte HTTP match |
| --- | --- | --- | --- | --- | --- |
| Standard | 200 to reserved | OK to reserved | 8/8 to fallback | ConnectionRefused, gRPC UNAVAILABLE | 200 to reserved |
| Experimental | 200 to reserved | OK to reserved | 8/8 to fallback | ConnectionRefused, gRPC UNAVAILABLE | 200 to reserved |

The two routes using 256-byte fields kept their generation and reported `Accepted=True` and `ResolvedRefs=True` throughout. After the regional capability was withdrawn, their programmed conditions became false. Both dependent listeners closed and neither backend received their probes. Restoring the capability reopened both listeners and the same positive and negative probes reached their expected backends again.

A separate HTTP/1.1 measurement used the previous HAProxy binary. A 255-byte exact match returned 200 and reached the origin. The otherwise identical 256-byte request returned 400 before reaching the origin.

Envtest ran the Kubernetes API and the manager extracted from the linux/amd64 controller image. Installed Octavia packages served the REST requests. The Amphora agent validated and reloaded the native HAProxy configuration through a local service-control adapter. Keystone, Neutron, worker delivery and agent transport used fixtures. Qualification in a deployed Hibiscus region remains open. [Measured route conditions and traffic](receipt.json) · [Octavia source and packages](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-exact-http-header-name-256-300b2638c072)
