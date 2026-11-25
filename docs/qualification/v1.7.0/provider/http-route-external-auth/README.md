# HTTPRoute ExternalAuth on a selected rule

These local runs verified the source signatures.

An Experimental HTTPRoute on an HTTP listener had a protected `/private` rule and an unfiltered `/public` rule. Both used the same origin pool. The authorizer was a Service in a separate namespace. The test removed and recreated only its ReferenceGrant and checked that the HTTPRoute spec, UID, generation and Gateway VIP stayed the same.

| ReferenceGrant | Request | HTTP | Authorizer requests | Origin requests |
|---|---|---:|---:|---:|
| Present | `/private`, allow | 200 | 1 | 1 |
| Present | `/private`, refuse | 401 | 1 | 0 |
| Present | `/public` | 200 | 0 | 1 |
| Removed | `/private` | 500 | 0 | 0 |
| Removed | `/public` | 200 | 0 | 1 |
| Recreated | `/private`, allow | 200 | 1 | 1 |
| Recreated | `/private`, refuse | 401 | 1 | 0 |
| Recreated | `/public` | 200 | 0 | 1 |

The HTTPRoute stayed `Accepted=True`. After removal its `ResolvedRefs` became `False/RefNotPermitted` and its controller `Programmed` condition became `False/Pending`. Both recovered on restoration. The Gateway and listener stayed programmed. Octavia returned the authorization pool on the protected L7 policy and its listener association in both the pool list and detail. Removing the grant changed the protected policy to a fixed HTTP 500. The listener had no listener-wide authorization, and the public policy kept forwarding to the same pool.

The authorizer received the selected request header and prefixed path without the POST body. After allow, the origin received the body and the selected authorization response header. That identity header was absent from the client response.

On Standard, the HTTPRoute CRD omitted ExternalAuth. Both unfiltered rules returned HTTP 200. Each made one origin request and zero authorizer requests. Counts cover the recorded probes.

The runs used Kubernetes envtest and etcd, the running manager, installed stable provider wheels, Octavia WSGI and SQLAlchemy, the Amphora renderer and native HAProxy. Cloud infrastructure and agent transport used local adapters. This record covers one HTTP listener and one origin Service. Terminated HTTPS traffic, weighted origins, deployed Hibiscus and upstream conformance were outside these runs.

[Source, image and both runs](receipt.json) · [Standard receipt](standard-receipt.json) · [Experimental receipt](experimental-receipt.json)
