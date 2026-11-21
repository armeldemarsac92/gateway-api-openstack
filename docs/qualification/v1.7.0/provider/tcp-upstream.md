# TCPRoute traffic through stock Octavia on kind

The local runner uses the production controller watches and Endpoint mode with the original Gateway API v1.7.0 Services and echo pods. Gophercloud sends load balancer, listener, pool and member requests to the public Octavia 19.0.0 WSGI application. The Amphora worker renders the configuration that HAProxy 3.0.19 runs in the kind guest. The fixture provides synthetic network topology, VIP allocation, provisioning completion and agent transport. [Runner](../../../../internal/controller/kind_tcp_traffic_test.go) · [Provider fixture](udp-upstream-api.py) · [Guest agent callback](http-upstream-guest.sh)

The focused traffic cases use one TCP listener per Gateway. `TCPRouteMultipleRoutesAttachment` requires both routes to report `Accepted=True` and the listener to count two attachments. The upstream client then opens 100 separate TCP connections and verifies that only the older route's pod answers. `TCPRouteReferenceGrant` checks a cross-namespace echo reply before deleting the grant. After deletion, its assertion requires the route status to report `ResolvedRefs=False/RefNotPermitted`. [Multiple routes](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/tcproute-multiple-routes-attachment.go) · [ReferenceGrant](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/tcproute-reference-grant.go)

## Focused traffic results

The controller source snapshot was `a02e83e4ab974fd64042648ee1cbd6c71389a280`. The qualification runner was built with Go 1.27.4. [Exact runner revision](https://github.com/armeldemarsac92/gateway-api-openstack/tree/17c2ca64210ca8fb065e5bf23b27d7b147221d8f) The guest ran two-node Kubernetes v1.36.4 clusters with the official Standard and Experimental CRDs. The scoped runs omitted only the shared HTTPS Gateway.

| Tagged case | Standard | Experimental | Observed traffic on each channel |
|---|---|---|---|
| `TCPRouteMultipleRoutesAttachment` | PASS | PASS | The VIP and pod-side captures each recorded 101 echo replies from the older route's pod, including all 100 sampled connections. Both pods were Ready on the worker. The newer route's pod returned zero replies through the VIP. |
| `TCPRouteReferenceGrant` | PASS | PASS | The expected web namespace pod answered the upstream request and three independent VIP probes. After the grant was deleted and the worker's listener update was applied, three fresh VIP connections closed before the greeting. The same pod still answered three direct probes. |

All four captures recorded zero packets dropped by the kernel. Each focused run deleted all 16 Octavia resources it created, including five load balancers, five listeners, two pools, two members and two health monitors. The two case runs also removed their Kubernetes namespaces and classes, HAProxy processes, and loopback VIPs. The proof covers Pod IP endpoints on a kind worker. A deployed Amphora, its private VIP and NodePort forwarding require separate qualification.

## Weighted TCP connections

At controller [`75101c26553d`](https://github.com/armeldemarsac92/gateway-api-openstack/commit/75101c26553d89b81ab2281297be6ce7daac4f2a), the original `TCPRouteWeightedRouting` case passed on both channels. A dedicated Endpoint-mode Gateway listened on port 9300. The three Services specified weights 70, 30 and 0. Each had a Ready upstream pod on the kind worker. Stock Octavia 19.0.0 rendered the HAProxy 3.0.19 configuration from the controller's public API writes. [Tagged test and traffic oracle](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/tcproute-weighted-routing.go)

| Channel | Connections to tcp-backend-v1 | Connections to tcp-backend-v2 | Connections to tcp-backend-v3 |
|---|---:|---:|---:|
| Standard | 350 | 150 | 0 |
| Experimental | 350 | 150 | 0 |

Each sample contains 500 fresh connections. One earlier availability echo went to the first Service on each channel. The VIP and pod-side captures each identified the same 501 responses per channel. All four captures reported zero kernel drops. The upstream distribution check succeeded on its first attempt in both runs. The release image at `ce6f51797fb7` differs from the measured controller revision only in the capitalization of a readback error message. On each cluster, the per-kind IDs matched for creation and deletion of four load balancers, four listeners, one pool, two members and one health monitor. The runner verified removal of its namespaces, GatewayClasses, HAProxy instances and VIPs. [Per-channel results and capture hashes](tcp-weighted-results.json) · [Per-connection observations](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-ce6f51797fb7c31a63c1f63577cb6938ecaff618/hibiscus-transport-qualification-75101c26553d.tar.gz)

A weighted TCP pool is detached if an owned active member becomes a backup. The controller clears that flag and requires explicit `backup: false` on readback before reattaching the pool. A missing or null value leaves the route progressing. The provider probe verified the change and the sibling's weight using the public Octavia member API. [Provider probe](stock-tcp.md#reproduce-the-weighted-member-probe) · [Rollback procedure](../../../operator-recovery.md)

## Experimental status cases

Three other original cases passed through the same fixture with the Experimental bundle. `TCPRouteInvalidNonTCPListener` reported `Accepted=False/NotAllowedByListeners` and zero attached routes. `TCPRouteInvalidCrossNamespaceBackendRef` and `TCPRouteInvalidBackendRefNonexistent` reported `ResolvedRefs=False` with reasons `RefNotPermitted` and `BackendNotFound`, respectively. The run removed its six load balancers, six listeners and Kubernetes resources. The earlier Standard status qualification used a recording provider. [Standard status results](../kind-tcp.md)

## Full GATEWAY-TCP Core profile attempts

Both attempts used the complete upstream base. All 23 upstream pods became Ready on each channel. The shared `same-namespace-with-https-listener` Gateway reported `Accepted=False/ListenersNotValid` and `Programmed=False/Invalid`. Its four HTTPS listeners share port 443 and each reported `Accepted=False/UnsupportedProtocol`. Both attempts reached the upstream 300-second readiness timeout during setup. **Zero case bodies ran on either channel.** Each attempt removed the three load balancers and three listeners created during setup, along with its Kubernetes resources and local VIPs.

The machine-readable record includes the per-channel case outcomes, capture hashes, independent grant checks and cloud inventory. The first Standard wrapper stopped before executing the suite. Its temporary capture files and pod routes were removed before the corrected wrapper ran. [Per-channel results](tcp-kind-results.json)

## Reproduce

Prepare one disposable guest and separate two-node kind clusters for Standard and Experimental. Use the pinned Octavia environment, root-owned HAProxy and callback, and kind pod routing from the HTTP traffic qualification. Use the matching official release bundle and kubeconfig for each cluster. The scoped modes verify the published hash of `tcp-status-base.yaml`, which omits the single HTTPS Gateway from shared setup. The original release case bodies and case manifests are embedded in the runner. [Guest setup](http-websocket.md#reproduce-the-focused-case) · [Base manifest and Standard hash](../kind-tcp.md#reproduce-the-scoped-check) · [Experimental hash and images](udp-upstream.md#reproduce-the-local-run)

```sh
export GATEWAY_API_CHANNEL=standard
export KIND_CLUSTER_NAME="hibiscus-tcp-$GATEWAY_API_CHANNEL"
export HIBISCUS_TEST_KUBECONFIG="/home/fedora/kubeconfig-$GATEWAY_API_CHANNEL"
export HIBISCUS_TCP_CASES=traffic
export HIBISCUS_TCP_BASE_DIR=/home/fedora
out=$(mktemp -d)
export TCP_TEST_BINARY="$out/tcp-kind.test"
CGO_ENABLED=0 go test -mod=readonly -tags=envtest,kindtest -c -o "$TCP_TEST_BINARY" ./internal/controller
cat >"$out/run" <<'SH'
#!/bin/sh
exec "$TCP_TEST_BINARY" -test.v -test.run '^TestKindUpstreamTCPTraffic$' -test.timeout=10m
SH
cat >"$out/apply" <<'SH'
#!/bin/sh
exec sudo -n /usr/local/libexec/hibiscus-qualification/http-upstream-guest.sh "$@"
SH
chmod 700 "$out/run" "$out/apply"
"$OCTAVIA_PYTHON" docs/qualification/v1.7.0/provider/udp-upstream-api.py \
  --protocol tcp --run "$out/run" --apply "$out/apply" --timeout 590
```

Use `HIBISCUS_TCP_CASES=status` for the three tagged invalid-route cases. Use `HIBISCUS_TCP_CASES=weighted` for the individual upstream weighted Endpoint routing case. For the full Core attempt, set `HIBISCUS_TCP_CASES=profile` and unset `HIBISCUS_TCP_BASE_DIR`. This selects the original upstream base, registry, GATEWAY-TCP Core features and the 300-second namespace readiness timeout. Retain the setup result and the number of executed case bodies separately.

A capture of loopback ports 9310 and 9320 shows the VIP side. A capture of TCP port 3000 on the kind bridge identifies the responding backend. The echo protocol starts with a server greeting and the `TEST` client line returns a JSON record with the pod and namespace. [Upstream client](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/tcp/tcp.go)
