# Literal HTTP path routing

The local Standard and Experimental Gateways each served an HTTPRoute with a 1024-byte exact path and another with a 1024-byte segment prefix. Requests sent their original paths and query strings through native HAProxy. The backend recorded each request target before parsing it. The table shows the runs with both literal paths and fixed HTTP responses enabled.

| Channel | Exact path and its child | Prefix, child and near miss | Apostrophe and ampersand | Encoded `%41` |
| --- | --- | --- | --- | --- |
| Standard | 200 / 404 | 200 / 200 / 404 | 200 / 200 | 200 for the literal path |
| Experimental | 200 / 404 | 200 / 200 / 404 | 200 / 200 | 200 for the literal path |

Standard served 7 matching requests with HTTP 200. For the 6 misses, the path-only configuration returned HTTP 503. Enabling fixed HTTP responses on the same binaries changed every miss to HTTP 404. The origin received zero requests for those paths in both runs.

Experimental served 7 matching requests with HTTP 200. For the 6 misses, the path-only configuration returned HTTP 503. Enabling fixed HTTP responses on the same binaries changed every miss to HTTP 404. The origin received zero requests for those paths in both runs.

Every successful request reached the backend with its full request target unchanged. Paths outside the configured matches produced zero origin requests. The HTTPRoutes reported Accepted, ResolvedRefs and Programmed for their admitted generations.

A local Standard control using signed controller `3b7ad5dccd7d` and Octavia `592e32f88b0a` admitted the same 1024-byte exact path in Kubernetes. Its route parent reported Accepted=False/UnsupportedValue. That path returned HTTP 503 with zero origin requests, while the short control returned HTTP 200.

Kubernetes envtest ran the manager from the reproducible controller image alongside the installed Octavia wheels, the Amphora renderer and HAProxy. The controller and Octavia source revisions were signed. Keystone, Neutron, the worker delivery and agent transport used fixtures. Tests in a deployed Hibiscus region remain open. [Receipts and artifact digests](receipt.json) · [Octavia release](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-literal-http-paths-81ff5285a88f)
