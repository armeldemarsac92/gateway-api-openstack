# HTTPRoute method matching on Amphora

With method matching, exact headers and fixed responses enabled, the same VIP and path can select different Services for a GET and a POST. Both local release bundles passed the unchanged Gateway API v1.7.0 `HTTPRouteMethodMatching` case. [Upstream case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-method-matching.go) · [Operator configuration](../../../operator-octavia-http-method-match.md)

| Bundle | Upstream traffic assertions | Case elapsed | HTTP requests recorded by the fixture | Evidence |
|---|---:|---:|---:|---|
| Standard | 12/12 | 26.068 s | 523 | [Original report](http-method-match-evidence/standard-report.yaml) · [Receipt](http-method-match-evidence/standard.json) |
| Experimental | 12/12 | 26.981 s | 539 | [Original report](http-method-match-evidence/experimental-report.yaml) · [Receipt](http-method-match-evidence/experimental.json) |

Each run installed its own release bundle on a Kubernetes 1.36.2 API server. Five ready echo endpoints served three Services. The controller used the public Octavia API. The Amphora renderer supplied the configuration for HAProxy 3.0.19. Identity, networking and worker transport were fixtures. The upstream runner used its original deadline. Both reports cover one extended case in local-single-case mode.

## Traffic and status

After reconciliation, the test sent these additional requests through HAProxy on each channel.

| Method | Target | Request header | HTTP status | Backend |
|---|---|---|---:|---|
| POST | `/` | | 200 | infra-backend-v1 |
| GET | `/` | | 200 | infra-backend-v2 |
| POST | `/path2` | `Version: two` | 200 | infra-backend-v3 |
| HEAD | `/` | | 404 | |
| PUT | `/` | | 404 | |

Octavia reported eight active method predicates, and the loaded HAProxy configuration contained the same eight method ACLs. The Gateway reported `Accepted=True`, `Programmed=True` and one attached route. The route reported current-generation `Accepted=True` and `ResolvedRefs=True` when the upstream case finished. Its controller-specific programming condition was still pending at that observation and reached `True/Programmed` within the following 35 seconds on each channel.

## Revisions and replay

The tested manager came from controller source `1239d29f3faeaedeaab4805758946e6c951dccef`. Two independent builds produced the same linux/amd64 OCI manifest `sha256:28cc50a3bd94349805a0ac46606328a2aa111bd5ba56ea3b245ba9a122afeb3c`. The controller prerelease includes the OCI archive and `hibiscus-http-method-match-1239d29f3fae.tar.gz`, which contains the runner, full logs and file checksums. [Controller prerelease](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-1239d29f3faeaedeaab4805758946e6c951dccef)

The tested Octavia wheel was `19.0.1.dev8` from `4444e6a83a8e977c118cf588c4dd2f1cfbea162e`. The driver library was `3.12.1.dev3`. The provider replay includes the database migration and traffic through a terminating HTTPS frontend. [Octavia prerelease](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-http-method-match-4444e6a83a8e) · [Octavia master integration](https://github.com/armeldemarsac92/octavia/pull/7)

The earlier controller image `46b258bfbc48` and Octavia `19.0.1.dev7` passed ten of twelve assertions in one run per channel. The two failures reached the 30-second deadline during reconciliation. A timed replay of that same earlier image passed twelve on both channels. The local observations do not establish a convergence bound for deployed Amphora. [Development runs](http-method-match-evidence/development-runs.json)

The operator guide records the overlapping redirect limitation. Runtime qualification on a deployed Hibiscus region is open. [Regional procedure](../live-runs.md)
