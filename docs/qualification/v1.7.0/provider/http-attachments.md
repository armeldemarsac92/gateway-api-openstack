# HTTP attachment and ReferenceGrant traffic

The local qualification runs Gateway API v1.7.0 against real Kubernetes Services and pods in Endpoint mode. The controller programs the stock Octavia 19.0.0 public API. HAProxy 3.0.19 runs the Amphora worker's rendered configuration in a disposable kind guest. Subnet discovery, VIP allocation, provider completion and agent delivery use the local fixture. [Fixture topology and dependencies](http-websocket.md) · [Runner](../../../../internal/controller/kind_http_attachment_test.go)

The focused selection covers a route and Gateway in one namespace, a route attaching to a Gateway in a different namespace, and a route with a backend Service in another namespace. The third case sends a request with a valid ReferenceGrant, deletes the grant and requests the same route again. The upstream assertion then requires HTTP 500. [Same-namespace case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-simple-same-namespace.go) · [Cross-namespace Gateway](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-cross-namespace.go) · [Grant deletion](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-reference-grant.go)

## Results on both channels

The controller source was `0b512ec28ae055d6e1eb1460dc2a43e7b87c441f`. The qualification runner was built at `4aca0f99332370cb5a67512b8e621198ec42247d` with Go 1.27.4. kind v0.33.0 ran two Kubernetes v1.36.4 nodes per channel. Each cluster had its own official CRD bundle.

| Upstream case | Standard | Experimental | Observed traffic |
|---|---|---|---|
| `HTTPRouteSimpleSameNamespace` | PASS | PASS | The requested route reached the infrastructure namespace’s backend. |
| `HTTPRouteCrossNamespace` | PASS | PASS | The Gateway admitted a route from the web namespace and the web backend replied. |
| `HTTPRouteReferenceGrant` | FAIL | FAIL | The authorized backend replied before deletion. After reconciliation the VIP returned 503. The upstream assertion requires 500. |

After the upstream client deleted the grant, the backend still answered some requests while the change propagated. The counts below cover application requests in the upstream post-deletion assertion. The member capture identifies the web backend as the source of all the 200 responses. After forwarding stopped, every remaining client request returned 503 and the member received no further application requests. The route reported `Accepted=True`, `ResolvedRefs=False/RefNotPermitted` and the implementation’s `Programmed=False/Invalid` with the grant absent.

| Channel | HTTP 200 at the VIP and member | HTTP 503 at the VIP | HTTP 500 at the VIP |
|---|---:|---:|---:|
| Standard | 42 | 253 | 0 |
| Experimental | 35 | 258 | 0 |

Each focused run deleted all 24 Octavia resource IDs it created, including the three load balancers and six members. Both kind clusters and their guest were removed after collection. The case summaries, per-channel counts, conditions and profile setup are in the [qualification receipt](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-0b512ec28ae055d6e1eb1460dc2a43e7b87c441f/hibiscus-http-case-results-4aca0f993323.tar.gz) (SHA-256 `b574b8bdcf25b327a65e41d7acf76bb4e9129380701ee8ed9c175f0119e27e68`).

### Full GATEWAY-HTTP Core profile

On both channels all 23 upstream pods became Ready. The shared `same-namespace-with-https-listener` Gateway declares four HTTPS listeners on port 443. The controller reported `Accepted=False/ListenersNotValid` and `Programmed=False/Invalid`. Both original profile runs reached the upstream 300-second namespace-readiness timeout during setup. **Zero case bodies ran on either channel.** The three individual results above come from the focused base.

## Reproduce

Use the two-node kind guest, root-owned HAProxy and callback, release CRDs and pinned Python environment from the WebSocket qualification. Prepare one cluster per channel and select the matching pod routes and kubeconfig. The focused mode verifies the published SHA-256 of `tcp-status-base.yaml` and exposes only those bytes alongside the embedded release manifests. The full GATEWAY-HTTP Core attempt loads the original upstream base, case registry, Core feature set and timeout defaults. [Guest setup](http-websocket.md#reproduce-the-focused-case) · [Scoped base and digests](../kind-tcp.md#reproduce-the-scoped-check)

```sh
export GATEWAY_API_CHANNEL=standard
export KIND_CLUSTER_NAME="hibiscus-ws-$GATEWAY_API_CHANNEL"
export HIBISCUS_TEST_KUBECONFIG="/home/fedora/kubeconfig-$GATEWAY_API_CHANNEL"
export HIBISCUS_HTTP_CASES=attachments
export HIBISCUS_HTTP_BASE_DIR=/home/fedora
out=$(mktemp -d)
export HTTP_TEST_BINARY="$out/http-attachment.test"
CGO_ENABLED=0 go test -mod=readonly -tags=envtest,kindtest -c -o "$HTTP_TEST_BINARY" ./internal/controller
cat >"$out/run" <<'SH'
#!/bin/sh
exec "$HTTP_TEST_BINARY" -test.v -test.run '^TestKindUpstreamHTTPAttachment$' -test.timeout=10m
SH
cat >"$out/apply" <<'SH'
#!/bin/sh
exec sudo -n /usr/local/libexec/hibiscus-qualification/http-upstream-guest.sh "$@"
SH
chmod 700 "$out/run" "$out/apply"
"$OCTAVIA_PYTHON" docs/qualification/v1.7.0/provider/udp-upstream-api.py \
  --protocol http --run "$out/run" --apply "$out/apply" --timeout 590
```

For the full profile, set `HIBISCUS_HTTP_CASES=profile`, unset `HIBISCUS_HTTP_BASE_DIR` and run the final command again. Keep the raw exit status and each Go subtest result. When the shared setup fails, zero case bodies count as executed.
