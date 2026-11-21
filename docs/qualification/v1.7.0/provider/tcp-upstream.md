# TCPRoute traffic through stock Octavia on kind

The local runner uses the production controller watches and Endpoint mode with the original Gateway API v1.7.0 Services and echo pods. Gophercloud sends load balancer, listener, pool and member requests to the public Octavia 19.0.0 WSGI application. The Amphora worker renders the configuration that HAProxy 3.0.19 runs in the kind guest. The fixture provides synthetic network topology, VIP allocation, provisioning completion and agent transport. [Runner](../../../../internal/controller/kind_tcp_traffic_test.go) · [Provider fixture](udp-upstream-api.py) · [Guest agent callback](http-upstream-guest.sh)

The earlier attachment and grant cases used one TCP listener per Gateway. `TCPRouteMultipleRoutesAttachment` requires both routes to report `Accepted=True` and the listener to count two attachments. The upstream client then opens 100 separate TCP connections and verifies that only the older route's pod answers. `TCPRouteReferenceGrant` checks a cross-namespace echo reply before deleting the grant. After deletion, its assertion requires the route status to report `ResolvedRefs=False/RefNotPermitted`. [Multiple routes](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/tcproute-multiple-routes-attachment.go) · [ReferenceGrant](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/tcproute-reference-grant.go)

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

## Multiple TCP listeners on one Gateway

The original v1.7.0 `TCPRouteParentRefAttachAll` and `TCPRouteParentRefPortAndSectionName` cases use four and three distinct ports respectively. The first selects one Service across an entire Gateway. The second selects three different Services with a port, a listener name, and the intersection of both selectors. The scoped runner changes case selection and omits the common HTTPS Gateway from setup. It embeds the tagged manifests and uses the original case bodies and echo assertions. [Attach-all case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/tcproute-parentref-attach-all.go) · [Port and section case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/tcproute-parentref-port-and-section-name.go) · [Scoped runner](https://github.com/armeldemarsac92/gateway-api-openstack/blob/00a9799650bc03d4e96dc80afa4b5437573be183/internal/controller/kind_tcp_traffic_test.go)

The multiport test binary used controller source `00a9799650bc03d4e96dc80afa4b5437573be183`. The release image source `63ec0796cb57cd7ac0f698879e6b635d6069019b` has the same production code. The only change is to an envtest assertion for the validation callback and the desired graph. These focused runs use the disposable kind topology, local Octavia WSGI API, stock Amphora worker renderer and HAProxy. [Tested source](https://github.com/armeldemarsac92/gateway-api-openstack/tree/00a9799650bc03d4e96dc80afa4b5437573be183) · [Release test-only delta](https://github.com/armeldemarsac92/gateway-api-openstack/compare/00a9799650bc03d4e96dc80afa4b5437573be183...63ec0796cb57cd7ac0f698879e6b635d6069019b)

Both cases passed on Standard and Experimental. Each run used one active two-node kind cluster on Kubernetes 1.36.4 with etcd on tmpfs and the matching official v1.7.0 bundle.

| Original case | Port | Parent selector | Expected Service | Standard matched echoes / VIP connections | Experimental matched echoes / VIP connections |
|---|---:|---|---|---:|---:|
| `TCPRouteParentRefAttachAll` | 9310 | Gateway | `tcp-echo-attach-all` | 1 / 82 | 1 / 82 |
| `TCPRouteParentRefAttachAll` | 9311 | Gateway | `tcp-echo-attach-all` | 1 / 20 | 1 / 22 |
| `TCPRouteParentRefAttachAll` | 9312 | Gateway | `tcp-echo-attach-all` | 1 / 21 | 1 / 18 |
| `TCPRouteParentRefAttachAll` | 9313 | Gateway | `tcp-echo-attach-all` | 1 / 22 | 1 / 23 |
| `TCPRouteParentRefPortAndSectionName` | 9300 | `port: 9300` | `tcp-echo-one` | 1 / 264 | 1 / 242 |
| `TCPRouteParentRefPortAndSectionName` | 9301 | `sectionName: two` | `tcp-echo-two` | 1 / 26 | 1 / 26 |
| `TCPRouteParentRefPortAndSectionName` | 9302 | `sectionName: three` and `port: 9302` | `tcp-echo-three` | 1 / 30 | 1 / 24 |

Each echo was paired by request and reply bytes across the VIP and member captures. The member address matched the IP of a Ready pod on the kind worker. The four Gateway-wide ports reached the same pod on each channel. The three selected ports reached three distinct expected pods on each channel. The captures reported zero kernel drops. In each run the public API recorded matching creation and deletion IDs for five load balancers, ten listeners, four pools, four members and four health monitors. The fixture verified removal of its Kubernetes namespaces and classes, HAProxy processes and loopback VIPs.

The cases verify fresh connections and selected backends. The provider callback restarts HAProxy when applying a new configuration. The runs use a synthetic loopback VIP with the actual Kubernetes pod IPs as members. Deployed Amphora and private VIP reachability, existing streams during changes, failover, multiport NodePort forwarding and a complete GATEWAY-TCP profile remain unqualified. [Independent stock reload probe](stock-tcp.md#tcp-stream-during-a-stock-reload) · [Live ledger](../live-runs.md)

An earlier Experimental binary at `80b48fbd11c5a232ba78f50495538f1a64ee83ae` added only the scoped case selector to the single-listener controller. Both original cases failed. Both Gateways reported `Accepted=False/ListenersNotValid` and `Programmed=False/Invalid`. All seven wanted ports recorded zero attached routes, zero VIP connections and zero matched echo exchanges. The fixture matched all three load balancer and three listener IDs during deletion and verified cleanup. [Baseline source](https://github.com/armeldemarsac92/gateway-api-openstack/tree/80b48fbd11c5a232ba78f50495538f1a64ee83ae)

A separate local replay checked four captured route bindings across two Gateway UIDs per channel. The measured controller accepted the Gateway and route bindings and resolved the expected ownership identities. The prior weighted-TCP revision `ce6f51797fb7` rejected both sets of enrolled bindings with an ownership conflict. [Rollback procedure](../../../operator-recovery.md#multiport-tcp-upgrades)

The evidence archive contains the sanitized per-channel observations, the earlier single-listener baseline, build receipts and checksums. The provider probe archive covers independent stock API and agent reload studies. [Multiport results archive](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-63ec0796cb57cd7ac0f698879e6b635d6069019b/hibiscus-tcp-multiport-00a9799650bc.tar.gz) · [Native provider probes](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-63ec0796cb57cd7ac0f698879e6b635d6069019b/hibiscus-native-provider-probes-00a9799650bc.tar.gz)

The SHA-256 of the per-channel multiport archive is `349675ad8f059d8553f4ef94520f2368b69bdcd58543434557bab3075f964fea`.

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

Use `HIBISCUS_TCP_CASES=status` for the three tagged invalid-route cases. Use `HIBISCUS_TCP_CASES=weighted` for the individual upstream weighted Endpoint routing case. For the two original parent reference cases set `HIBISCUS_TCP_CASES=multiport` and allow `-test.timeout=35m` and a fixture `--timeout 2110`. Run each channel on its own active cluster with the scoped base. Use the selected channel’s release bundle and kubeconfig. For the full Core attempt, set `HIBISCUS_TCP_CASES=profile` and unset `HIBISCUS_TCP_BASE_DIR`. This selects the original upstream base, registry, GATEWAY-TCP Core features and the 300-second namespace readiness timeout. Retain the setup result and the number of executed case bodies separately.

For the earlier attachment and grant cases, capture loopback ports 9310 and 9320. For the multiport cases, capture ports 9310–9313 and 9300–9302 on loopback. Match the echo exchange against TCP port 3000 on the kind bridge and the Ready pod’s observed IP and worker node. The echo protocol starts with a server greeting and the `TEST` client line returns a JSON record with the pod and namespace. [Upstream client](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/tcp/tcp.go)
