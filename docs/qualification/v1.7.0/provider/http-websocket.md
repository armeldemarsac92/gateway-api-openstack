# WebSocket through an HTTPRoute

The v1.7.0 case creates an HTTPRoute for a Service with `appProtocol: kubernetes.io/ws` on port 8082. The Service selects ready echo pods on TCP port 3000. The client opens `/ws`, sends the text `Websocket Support!` and then seven binary bytes on the same connection. The case compares both replies with the original messages. [Upstream case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-backend-protocol-websocket.go) · [Route manifest](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-backend-protocol-websocket.yaml)

The local runner starts the production reconcilers against a real kind API server. In Endpoint mode the shared Gateway writer submits the HTTP listener, L7 route, HTTP pool and pod members through Gophercloud to the pinned Octavia v2 WSGI app. The DB and Amphora worker build the HAProxy configuration. The fixture supplies the project, network lookup, VIP allocation, RPC completion and agent transport. A disposable guest applies the worker's configuration to HAProxy 3.0.19 and routes the private test VIP to the upstream echo pods. A deployed Magnum cluster, Amphora and cloud security groups still require live qualification. [Kubernetes runner](../../../../internal/controller/kind_http_websocket_test.go) · [Provider fixture](udp-upstream-api.py) · [Guest agent callback](http-upstream-guest.sh)

## Results on both channels

Controller [0b512ec28ae0](https://github.com/armeldemarsac92/gateway-api-openstack/commit/0b512ec28ae055d6e1eb1460dc2a43e7b87c441f) ran the unmodified case in Endpoint mode on two separate, two-node Kubernetes v1.36.4 clusters. The table counts HTTP responses for `GET /ws` at the published VIP, including every attempt during convergence. For each HTTP 101, the same WebSocket handshake nonce appeared on the pod-facing connection. The client and pod captures both recorded zero kernel drops. [Capture archive and per-channel receipts](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-0b512ec28ae055d6e1eb1460dc2a43e7b87c441f/hibiscus-websocket-qualification-0b512ec28ae0.tar.gz)

| Gateway API v1.7.0 bundle | Upstream case | HTTP 503 before convergence | Consecutive HTTP 101 at both VIP and pod |
|---|---|---:|---:|
| Standard | PASS | 91 | 3 |
| Experimental | PASS | 70 | 3 |

On each upgraded connection the client sent `Websocket Support!` as opcode 1 and `01 02 03 04 05 06 07` as opcode 2. The actual upstream pod returned the original bytes with opcode 1 in both replies. Both sides of HAProxy recorded that same sequence. The upstream test compares the bytes of the second response. In each channel the three exchanges reached two distinct ready pod addresses.

On the same harness, the previous controller source `36223ef75ef6` failed the Standard case at route acceptance with `ResolvedRefs=False/UnsupportedProtocol`. The shared setup created three load balancers and three listeners. Both probe interfaces saw zero matching packets before the case ended. In each passing run all created Octavia resource IDs appeared in the deletion log, including three load balancers, three listeners, one pool, two members, one health monitor, one L7 policy and one L7 rule.

The packet archive has SHA-256 `3a591b06a94dfe8927cb5d5f5a646b88223f3a646e911ceb1c5fc16b0aa2b386`. It contains the synthetic `/ws` streams, suite results, installed HAProxy version and digest, and a JSON count per channel. To inspect the frames on the member side, run the command below after extracting the archive. For the VIP side use `standard-client.pcap` and port `80`. The full GATEWAY-HTTP profile and NodePort WebSocket traffic on a deployed Amphora require separate qualification.

```sh
tshark -n -r standard-member.pcap -d tcp.port==3000,http \
  -Y 'http.response.code == 101 || websocket.opcode == 1 || websocket.opcode == 2' \
  -T fields -e tcp.stream -e tcp.srcport -e http.response.code \
  -e websocket.opcode -e websocket.mask -e websocket.payload
```

## Reproduce the focused case

1. Prepare one disposable Linux guest with HAProxy 3.0.19, Linux iproute2, procfs, Bash, util-linux and the Octavia 19.0.0 test dependencies. As the guest administrator, install the verified HAProxy executable and the reviewed `http-upstream-guest.sh` into `/usr/local/libexec/hibiscus-qualification/`, owned by root and writable only by root. Name the files `haproxy` and `http-upstream-guest.sh`. Record the installed executable's SHA-256 and version. The callback checks the runtime version against the version given to the worker. The guest assigns TEST-NET-1 VIPs on loopback and needs to reach the pod IPs from the kind bridge. The provider fixture uses ephemeral SQLite. [Pinned Octavia setup](stock-udp.md#reproduce-the-public-api-and-configuration-check)
2. Create a two-node kind v0.33.0 cluster with Kubernetes v1.36.4. Name it `hibiscus-ws-standard` or `hibiscus-ws-experimental`. Install the matching official v1.7.0 bundle and the fork's OpenStackGatewayClassConfig CRD. Preload the upstream echo images or allow kind to pull them. Route each node's `spec.podCIDR` via its address on the kind bridge. Use separate clusters and kubeconfigs for the two channels. [Node image, release bundles and echo digests](udp-upstream.md#reproduce-the-local-run)
3. Generate `tcp-status-base.yaml` with the published recipe. The scoped base omits the Gateway with four HTTPS listeners on port 443. The original WebSocket case, HTTPRoute, Service, echo workload and assertions come from the pinned Go module. [Base manifest recipe and digests](../kind-tcp.md#reproduce-the-scoped-check)
4. From the fork root, run the following commands in the disposable guest. Set `OCTAVIA_PYTHON` to the pinned Python interpreter, `HIBISCUS_HTTP_BASE_DIR` to the directory containing the scoped base, and `HIBISCUS_TEST_KUBECONFIG` to the selected cluster's absolute kubeconfig path. The guest callback runs with root privileges in this isolated environment.

```sh
export GATEWAY_API_CHANNEL=standard
export KIND_CLUSTER_NAME="hibiscus-ws-$GATEWAY_API_CHANNEL"
out=$(mktemp -d)
export WS_TEST_BINARY="$out/websocket.test"
CGO_ENABLED=0 go test -mod=readonly -tags=envtest,kindtest -c -o "$WS_TEST_BINARY" ./internal/controller
cat >"$out/run" <<'SH'
#!/bin/sh
exec "$WS_TEST_BINARY" -test.v -test.run '^TestKindUpstreamHTTPWebSocket$' -test.timeout=10m
SH
cat >"$out/apply" <<'SH'
#!/bin/sh
exec sudo -n /usr/local/libexec/hibiscus-qualification/http-upstream-guest.sh "$@"
SH
chmod 700 "$out/run" "$out/apply"
"$OCTAVIA_PYTHON" docs/qualification/v1.7.0/provider/udp-upstream-api.py \
  --protocol http --run "$out/run" --apply "$out/apply" --timeout 590
```

Retain the Go subtest result and the fixture exit status. A capture on loopback shows the VIP side of the HTTP upgrade. Capture TCP port 3000 on the kind bridge to see the selected pod's response. The split-host qualification forwarded the WSGI socket to guest loopback over SSH.

If the suite times out or an agent callback fails, discard that disposable guest before repeating the run. The loss of the fixture's in-memory graph can leave its last HAProxy process and loopback address in the guest.
