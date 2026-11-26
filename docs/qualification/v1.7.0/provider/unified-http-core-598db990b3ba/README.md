# Local Gateway API v1.7.0 HTTP Core on the signed controller

The signed release at `598db990b3ba3ef2851f4e9cb319e12beb432eb2` passed all **40 GATEWAY-HTTP Core cases on each channel**, Standard and Experimental, with the Amphora HTTP and shared HTTPS capabilities enabled. The upstream executable selected `GATEWAY-HTTP` with the `Gateway,HTTPRoute,ReferenceGrant` feature list. It used the original case registry, manifests and per-case deadlines, and ran without case exclusions. [Standard upstream report](standard-core.yaml) · [Experimental upstream report](experimental-core.yaml) · [Published image record](../../image-build.md)

Each disposable kind cluster used real echo Pods and Endpoint mode. The extracted controller executable called Octavia's WSGI API, which stored its graph in SQLite. The fixture dispatched worker tasks to the installed Octavia code and the Amphora renderer. Native HAProxy validated and served the rendered configuration. The fixture supplied Keystone and Neutron responses, worker dispatch and the Amphora agent transport. It used a local Barbican API to store the frontend certificates. These runs qualify local HTTP Core with this provider and controller configuration. Deployed Hibiscus results are tracked separately. [Regional ledger](../../live-runs.md)

| Run | Passed | Failed | Core cases skipped | Upstream CLI duration |
| --- | ---: | ---: | ---: | ---: |
| Standard Core | 40 | 0 | 0 | 542.33 s |
| Experimental Core | 40 | 0 | 0 | 587.90 s |
| Standard targeted replay | 3 | 0 | 37 excluded by selection | 95.49 s |

The fixture deleted each cluster and shut down its guest after collecting the report.

## Kubernetes API relay and traffic timing

The signed controller reached the kind API through a TLS-verified TCP–Unix–TCP relay and an SSH forward. The effective relay used `TCP_NODELAY` on both TCP sockets of the Kubernetes API path. In the interleaved comparison using the same API endpoint and authenticated HTTP/2 client, the earlier relay recorded 7 of 30 reused requests at or above 35 ms (maximum 44.433 ms). The corrected relay recorded 0 of 30 (maximum 1.457 ms).

The [targeted Standard replay](standard-targeted.yaml) exercised the three cases that previously exposed slow route publication on the same signed controller. The first active HAProxy configuration appeared within each case's original 30-second request window. The local Octavia readback confirmed all 14 policies were enabled and `ACTIVE` across the three batch activations.

| Case | Policies in first serving configuration | First publication after first request | Median between reconcile writes |
| --- | ---: | ---: | ---: |
| `HTTPRouteHeaderMatching` | 7 | 7.092 s | 0.504 s |
| `HTTPRouteMatching` | 4 | 5.937 s | 0.566 s |
| `HTTPRouteServiceTypes` | 3 | 6.141 s | 0.398 s |

The recorded CLI setup through creation of the first HTTPRoute was 34.95 seconds in the corrected targeted replay and 21.34 seconds in the earlier targeted run. Publication timing in the table starts with the first application request after the route is created. [Earlier targeted report and readback](../initial-http-predicates/receipt.json)

## Channel comparison

Experimental installed the complete v1.7.0 Experimental bundle by server-side apply. Kubernetes discovery confirmed the served versions of all 14 established Gateway API CRDs, including XBackend, XBackendTrafficPolicy, XMesh and XTelemetryPolicy at `v1alpha1`. The manager used the Experimental RBAC component and `--gateway-api-channel=experimental`. The resulting GatewayClass advertised `GatewayAddressRoutability`. The explicit upstream Core feature selection was identical on both channels.

The full runs matched on the upstream executable and base manifests, manager and OCI manifest, Octavia wheels, HAProxy, kind node, ready Pod image digests and both legs of the corrected API relay. The only manager flag difference was the channel. The upstream CLI used its original per-case deadlines on both channels. The Go process-wide test timeout was disabled for both.

## Input pins

| Input | Revision or SHA-256 |
| --- | --- |
| Upstream Gateway API v1.7.0 | `e26d183dbbc1bb782b78addbd26faba03955c152` |
| Upstream executable | `02f22a3712b9a8af7d1515d81d1d2efdac2ddc302bda343ad7eea19ca8a9653e` |
| Upstream base manifests | `9f9ef20a1c81851e08380cfb87b7b611dc16390e1e4fe3be0b0a788115465352` |
| v1.7.0 Standard install bundle | `4e2602c4cbe1afbc394d86f4ac6522e1a5263b01133bba1e40a6d1a549e753a1` |
| v1.7.0 Experimental install bundle | `e6d2d3407717f54b579b22ad4d164b6863895b3524f21250b0da0c1ff9a05d53` |
| Controller OCI manifest | `ghcr.io/armeldemarsac92/gateway-api-openstack@sha256:ea16501f654beb2cd0df13125c32bffe78cb3dca32efff690a46f2d7771e52b3` |
| Running controller executable | `ba909b92305d94ee69c7a3a7721dd184fff2954c3222dcdae859ec59f2033cf5` |
| Octavia 19.0.1.dev92 wheel at `bb8bf986b57ad0acef3f29676e4ae436fbdd30f7` | `9dd6335d82c76656d54a2dc792f260030b7e4342118f38fd9e009181894b74c0` |
| octavia-lib 3.12.1.dev21 wheel at `eb4c646b59758334629feb99d517f938c8af3712` | `a184a491c44d654317c6d1ae3225e3bed73f1eb8632132291ce01953e28dfa5d` |
| Native HAProxy 3.0.19 | `57ce7ace05aa5a094ac71726dcce65e89971d521c3528de1c6c198a3918132e8` |
| kind v0.33.0 executable | `aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d` |
| kind v0.33.0 / Kubernetes v1.36.4 node manifest | `597367624b4748b74b98e4fe2d661cd78063d02ecdaca702fd90572375b83e67` |
| Echo-basic v0.1.0 OCI archive | `b7675809fd2cc0b9fb9273f1b0c85c422bc3c12361d66b99b83173ddb2d3c9b7` |
| Echo-basic v0.2.0 OCI archive | `60951109f49e2cffe3a41de08d09be7e6d454cfb09268625447de002b16a069d` |
| Echo-basic v0.1.0 / v0.2.0 manifests loaded under the upstream tags | `sha256:fed223b186fbd83d50a4578d168c9e7d89341d8c1af616ebe0f62d53ab7451dc` / `sha256:c93c83e97be4d04cc5f2045f4b29c19cdeb5eb4319382b4244051d3d35545be6` |
| Corrected API relay module | `b2fce2863ae5c2857e5e26628a566cbf0806cfde1623f2f005f9584c6d1d3054` |

On both channels the manager used `--octavia-https-listener-isolation`, `--octavia-http-header-exact`, `--octavia-http-request-redirect`, `--octavia-request-header-modifier`, `--octavia-fixed-http-responses`, `--octavia-weighted-http-targets`, and the discovered `--octavia-l7policy-initial-rules` and `--octavia-l7policy-batch-activation` capabilities. Its local fixture settings were Octavia microversion 2.8, resync 2 s, polling 50 ms, API QPS 120, burst 160 and operation timeout 30 s. The runner used a dedicated service account and verified its limited Secret permissions. The upstream suite generated the frontend Secrets and the fixture verified enrollment in local Barbican.

The upstream reports are retained byte for byte.

| Report | SHA-256 |
| --- | --- |
| [Standard Core](standard-core.yaml) | `635c4ae4bed4f6b548503a6ca2dc7452e6085bc062999967a03bdc721a4a4764` |
| [Experimental Core](experimental-core.yaml) | `cf1e8f3f3bcd63391fd40987185b80005d2bc7434bd3f0c5c9cb49500c7a6a5e` |
| [Standard targeted](standard-targeted.yaml) | `0a69b66708f88b9d799996367cdf6256b7e16bcd091965f3fa6b7275ce22eb25` |
