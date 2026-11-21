# HTTP attachment and ReferenceGrant traffic

The local qualification runs Gateway API v1.7.0 against real Kubernetes Services and pods in Endpoint mode. The controller programs the stock Octavia 19.0.0 public API. HAProxy 3.0.19 runs the Amphora worker's rendered configuration in a disposable kind guest. Subnet discovery, VIP allocation, provider completion and agent delivery use the local fixture. [Fixture topology and dependencies](http-websocket.md) · [Runner](../../../../internal/controller/kind_http_attachment_test.go)

The focused selection covers a route and Gateway in one namespace, a route attaching to a Gateway in a different namespace, and a route with a backend Service in another namespace. The third case sends a request with a valid ReferenceGrant, deletes the grant and requests the same route again. The upstream assertion then requires HTTP 500. [Same-namespace case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-simple-same-namespace.go) · [Cross-namespace Gateway](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-cross-namespace.go) · [Grant deletion](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-reference-grant.go)

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
