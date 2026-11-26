# Forward a bounded request body to a rule authorizer

On the Experimental channel, a rule can set `externalAuth.forwardBody.maxSize`. This local run used a 65,536-byte limit on an HTTP and an Envoy gRPC authorizer.

| Client request | HTTP authorizer | gRPC authorizer | Origin |
| --- | --- | --- | --- |
| At the limit, on the corresponding protected path | 65,536 bytes on the HTTP path | 65,536 raw bytes on the gRPC path | One call with the same bytes and SHA-256 |
| One byte over the limit, on either protected path | Zero calls | Zero calls | Zero calls. The client received HTTP 413. |
| One byte over the limit, on the public path | Zero calls | Zero calls | One call with all 65,537 bytes |
| HTTP path after setting the limit to zero | One call with an empty body | Zero calls | One call with all 65,537 bytes |

The controller reconciled the zero limit on the same route and VIP. The Standard CRD rejected the filter on server-side dry run and admitted the public control. The Experimental run exercised real Kubernetes, installed Octavia and native HAProxy. [Experimental traffic, sizes and digests](experimental.json) · [Standard admission](standard.json) · [Build and qualification boundary](../http-route-grpc-external-auth/receipt.json)
