# HTTP listeners, hostnames and delegated routes

On the signed v1.7.0 controller image `7ed5e90f086200258d1aa0f3b6af3244c8655b81`, five of six original cases passed on each channel. Each case ran separately with the upstream manifests and timeouts. Standard and Experimental each used a fresh local kind cluster, ready echo Pods, the Octavia REST API, the Amphora L7 worker and HAProxy. Keystone, Neutron and the Amphora agent transport were simulated. The regional configuration and controller both enabled grouped initial L7 rules. These are local case results. [Controller release and image](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-7ed5e90f086200258d1aa0f3b6af3244c8655b81) · [Live qualification](../../live-runs.md)

| Registered case | Standard | Experimental | What the original case checked |
|---|---|---|---|
| `ListenerSetGatewayParentSectionNameNotFound` | PASS | PASS | The delegated parent served `/goodsection`. The Gateway parent reported `NoMatchingParent` for the missing section and `/badsection` returned 404. |
| `ListenerSetDualParentRefIndependence` | PASS, 4/4 | PASS, 4/4 | Separate outcomes for two parent references when both, or only one, accept the route. |
| `ListenerSetAllowedRoutesNamespaces` | PASS, 11/11 | PASS, 11/11 | Same-namespace and cross-namespace ListenerSet route admission and traffic. |
| `GatewayHTTPListenerIsolation` | PASS, 32/32 | PASS, 32/32 | A matching path reached its backend through the selected exact, wildcard or unspecified listener. Paths belonging to another listener returned 404. Both original manifests ran. |
| `HTTPRouteHostnameIntersection` | PASS, 33/33 requests | PASS, 33/33 requests | All four groups passed, including route attachment counts and a listener with no hostname. |
| `ListenerSetHTTPRouting` | FAIL, 35/36 | FAIL, 34/36 | Requests for `gateway-listener-1.com/route` timed out in both runs. Experimental also timed out on the second ListenerSet's listener 2 route. |

The suite reported failure on each channel because `ListenerSetHTTPRouting` missed its deadline. In the Standard run the first request received 404 up to the 30-second deadline. The first recorded HAProxy configuration containing the `/route` path appeared about 0.535 s later. The trace records paths across listeners. It does not establish the response for the failing hostname after convergence. [Standard results and original reports](standard/results.json) · [Experimental results and original reports](experimental/results.json) · [Failed request and reload timing](routing-timing.json)

The earlier `1206fc60da9d` controller passed 14 of 32 listener-isolation assertions on each channel. Each report and its receipt are retained with the corresponding source, provider and bundle hashes. [Earlier Standard run](baseline/standard/results.json) · [Earlier Experimental run](baseline/experimental/results.json)

## Replay

Use the shared HTTPS fixture and prerequisites listed in the signed controller's redirect replay. Start with a fresh extraction of `hibiscus-httproute-initial-rules-7ed5e90f0862.tar.gz` (SHA-256 `fb038b4941c2f87d30cb665992a3837e406e46f93b17609a1ae7085626ad4de4`). Copy this directory's two [runner inputs](runner) into its `httproute-initial-rules/runner/` directory. All other runner files matched that release byte for byte.

Use revision `e26d183dbbc1bb782b78addbd26faba03955c152` for the upstream sources and the archived controller executable from `7ed5e90f0862`. Set `HIBISCUS_INITIAL_RULES_MODE=enabled` and supply the **19.0.1.dev38** Octavia and **3.12.1.dev10** octavia-lib wheels and their SHA-256 values shown in the results. Set the remaining local paths as described in the redirect replay. With a fresh `HIBISCUS_REPLAY_EVIDENCE` directory, run the command below from the extracted `httproute-initial-rules` directory. An upstream failure produces a nonzero exit and the reports for both channels. [Redirect replay](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-7ed5e90f086200258d1aa0f3b6af3244c8655b81/hibiscus-httproute-initial-rules-7ed5e90f0862.tar.gz) · [Shared fixture](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-98b0267937081ee4e2ff0c17114c2c08d9766ec8) · [Octavia wheel](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-cors-c9f6c92f6143) · [octavia-lib wheel](https://github.com/armeldemarsac92/octavia-lib/releases/tag/hibiscus-cors-provider-e782f250ad2f)

```sh
rtk proxy bash runner/run-kind-vm.sh standard experimental
```
