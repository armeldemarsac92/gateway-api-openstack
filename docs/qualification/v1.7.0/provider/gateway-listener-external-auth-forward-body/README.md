# Forward request bodies to HTTP and HTTPS listener authorizers

On Experimental Gateway API, set `externalAuth.forwardBody.maxSize` to send a bounded request body from an HTTP or TLS-terminating HTTPS listener to an HTTP authorization Service. HTTP and HTTPS have separate regional enrollment settings. [Operator instructions](../../../../operator-octavia-listener-request-filters.md) · [Octavia release](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-http-https-listener-external-auth-body-1c25ce825547)

| Local Gateway trial | Protected hostname | Neighbor on the same port |
| --- | --- | --- |
| Experimental HTTP, grant present | At 65,536 bytes the authorizer and origin each received the complete body. At 65,537 bytes HAProxy returned HTTP 413 before calling either peer. | HTTP 200 with all 65,537 bytes at the origin |
| Experimental HTTP, grant withdrawn | The listener reported `ResolvedRefs=False/RefNotPermitted` and `Programmed=False/Invalid`. Both sizes returned HTTP 403 with zero peer calls. | HTTP 200 after reconciliation. During withdrawal the probes recorded 4 HTTP 200, 8 connection refusals. |
| Experimental HTTP, grant restored | The listener reported `ResolvedRefs=True` and `Programmed=True`. Both body-limit checks returned their original results. | HTTP 200 after reconciliation. During restoration the probes recorded 11 HTTP 200, 5 connection refusals. |
| Experimental HTTPS, shared port 443 | TLS 1.3 negotiated HTTP/1.1. At the limit both peers received the complete body. One byte over the limit returned HTTP 413. Mismatched SNI and HTTP authority returned HTTP 421. Both rejections generated zero peer calls. | HTTP 200 with all 65,537 bytes at the origin |
| Standard HTTP | Strict server-side dry-run rejected the listener filter and admitted ordinary listeners. | Each ordinary hostname forwarded to its own origin. |

The shared HTTPS Gateway used the exact Secret revision enrolled in local Barbican. The native HAProxy suite ran the body-limit and SNI/authority checks over HTTP/1.1 and negotiated HTTP/2. Stable and master Octavia each passed 7 installed-package tests and 7 native HAProxy tests.

The Gateway trials ran on Kubernetes v1.36.2 with the signed controller image, installed Octavia and Octavia-lib wheels, the Amphora renderer and native HAProxy. The fixture supplied Keystone, Neutron, RPC completion and Amphora agent transport. Qualification on a deployed Hibiscus region remains open. [Artifact digests, byte counts and conditions](receipt.json)
