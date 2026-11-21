# Gateway API v1.7.0 UDP traffic on kind

The upstream client sends UDP datagrams to the IP published in Gateway status and the port in the listener spec. In this qualification the Go controller reads the upstream ClusterIP Services and EndpointSlices. Its OpenStack adapter programs the Octavia 19.0.0 public API. The fixture renders the persisted UDP listener with the Amphora LVS template, starts Keepalived with those bytes and routes packets through kernel IPVS to the actual echo pod. [Upstream UDP client](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/udp/udp.go) · [Go runner](../../../../internal/controller/kind_udp_traffic_test.go) · [Octavia fixture](udp-upstream-api.py) · [Guest network](udp-upstream-guest.sh)

The fixture supplies subnet discovery, VIP allocation, RPC completion and delivery of each changed listener configuration. It runs HTTP base Gateways through the public API and installs the UDP listeners in the guest dataplane. Testing a deployed Amphora, Neutron access rules and Magnum lifecycle requires an OpenStack environment. [Live qualification ledger](../live-runs.md)

The recorded runs used serial cases and a 90-second namespace-readiness timeout. The current focused runner keeps that timeout. The full profile runner uses the v1.7.0 default of 300 seconds. The case bodies, workload images and case manifests come from the tagged v1.7.0 module.

## Multiple UDP listeners on one Gateway

At the 2026-11-21 controller revision [`00eabae770f4`](https://github.com/armeldemarsac92/gateway-api-openstack/commit/00eabae770f40014af8cb6cb53a225f1450ac52a), the two scoped cases passed on the Standard Kubernetes v1.36.4 cluster. The Experimental cluster passed the port-and-section case and failed the Gateway-wide attachment case. Both clusters used the original v1.7.0 cases, workload images and case manifests. The scoped common setup omits the HTTPS Gateway described below. This run covered two cases and a separate add/remove lifecycle.

| Upstream case | Standard | Experimental | Responses received through the Gateway address |
|---|---|---|---|
| `UDPRouteParentRefAttachAllListeners` | PASS | FAIL | Standard returned the same `udp-echo-attach-all` pod on all three ports. In Experimental, 21 probes reached 5310 and none reached the backend. Ports 5311 and 5312 both returned the expected pod. |
| `UDPRouteParentRefPortAndSectionName` | PASS | PASS | Ports 5300, 5301 and 5302 returned `udp-echo-by-port`, `udp-echo-by-section` and `udp-echo-by-section-and-port` respectively. |

The independent lifecycle began with one serving listener. Adding a second port preserved the VIP, original listener ID and pool ID. Removing the original port preserved the sibling's listener ID and shared pool. During the removal, all 15 fresh probes on Standard and all 14 on Experimental returned the expected Service from the sibling port. After settlement, three interleaved fresh probes to the retired port received no response and none reached the member-facing interface. Both Kubernetes status and the Octavia listener inventory reflected the new port set. The four captures recorded zero kernel drops. [Lifecycle test](../../../../internal/controller/kind_udp_listener_lifecycle_test.go)

Each upstream run recorded matching create and delete IDs for five fixture load balancers, nine listeners, four pools, four members and four monitors. The corresponding lifecycle counts were four, five, one, one and one. After both channels finished, no conformance namespaces, IPVS virtual services, fixture VIPs or listener state directories remained. The four nodes remained Ready for a separate qualification.

Before enrollment, a previous controller's binding readers accepted snapshots of the same Gateway and UDPRoute. They rejected both objects after enrollment on each channel. This check used revision `4aca0f99332370cb5a67512b8e621198ec42247d` against metadata collected from the API server. The lifecycle ran the new controller throughout. A dedicated adapter regression also lost a successful listener-create response and recreated the adapter before observing the same listener and pool. [Recovery procedure](../../../operator-recovery.md) · [Adapter regression](../../../../internal/cloud/openstack/graph/tcp_graph_test.go)

Two earlier runs show the behavior before this revision. The previous single-listener controller at `4aca0f993323` served the original port and rejected the second with `ListenersNotValid`. On an intermediate multiport build (`231e06c9d059`), the all-listeners case passed on Standard but the port-selection case timed out on 5300. All 21 probes reached the VIP and none reached a backend. The default-pool write occurred 12.9 seconds after the failure. Its other two ports reached the expected Services. During that 27.842-second interval, etcd recorded 146 slow requests and two disk synchronizations over one second.

On the final Experimental run, public Octavia traces show a 24.875-second interval without requests after member creation. During that window, etcd recorded 131 slow requests, 14 delayed ReadIndex responses and four disk synchronizations between 1.021 and 2.068 seconds. The API server logged four deadline errors. The port 5310 test expired at 05:33:19.322 UTC. Octavia dispatched the listener update at 05:33:21.855 UTC. The traces establish control-plane storage delays during the gap. They do not assign the full interval to a single reconciliation step. All runs kept the upstream traffic timeout.

The public archive contains per-channel results, an ordered excerpt of each trace with anonymous endpoint labels, hashes of the full local captures, fixture versions, per-port request counts, bounded control-plane log counts and file checksums. Its SHA-256 is `7ccf394d77a22fb61b94cf1f59bf94e6753b7c500bef9ee9b23f3942479afe10`. [Download the UDP results](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-00eabae770f40014af8cb6cb53a225f1450ac52a/hibiscus-udp-multiport-00eabae770f4.tar.gz)

## Experimental run with isolated etcd storage

The original `UDPRouteParentRefAttachAllListeners` case passed once on a new two-node Experimental cluster. It used the same controller test binary from `00eabae770f4`, pinned workload images, scoped common manifest and upstream traffic deadline as the preceding disk-backed run. The earlier two clusters were removed after checking that all test resources had drained. The replacement ran alone in the guest with its control-plane `/var/lib/etcd` on tmpfs. The case selected only the Gateway-wide attachment. [Machine-readable observation](udp-attach-all-experimental.json)

| UDP port | Requests at VIP | Requests at pod | Replies from the expected pod |
|---|---:|---:|---:|
| 5310 | 8 | 1 | 1 |
| 5311 | 1 | 1 | 1 |
| 5312 | 1 | 1 | 1 |

All 1,186 etcd WAL synchronizations during the test fell in the histogram bucket at or below 1 ms. Both the forward and return packets for all three echoes identified the same pod. The capture recorded zero kernel drops. The WSGI fixture recorded matching create/delete IDs for four load balancers, six listeners, one pool, one member and one monitor. The case left zero conformance namespaces, GatewayClasses, fixture VIPs, IPVS services and listener state directories. On the earlier disk-backed trial port 5310 got zero backend requests from 21 attempts. The cluster count, storage and case selection all changed, so this observation does not isolate the cause of the deadline miss. The matrix counts that mixed result as a failure. [Trace counts and checksums](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-ce6f51797fb7c31a63c1f63577cb6938ecaff618/hibiscus-transport-qualification-75101c26553d.tar.gz)

To reproduce the storage experiment, create one disposable Experimental kind cluster using the versions and commands below. On the control-plane node, bind an empty directory backed by tmpfs to `/var/lib/etcd` with a kind `extraMounts` entry. Verify the mount from inside the node using `stat -f -c %T /var/lib/etcd`. Select `HIBISCUS_UDP_CASES=UDPRouteParentRefAttachAllListeners` and use the original scoped manifest. Record `etcd_disk_wal_fsync_duration_seconds_bucket` immediately before and after the case and inspect traffic on the VIP and member interfaces.

## Weighted Endpoint results

The 2026-11-21 controller revision [`c49134b1183b`](https://github.com/armeldemarsac92/gateway-api-openstack/commit/c49134b1183bbc6b3e4cf01c720bef2ae154d70e) ran the nine UDP cases on two independent, two-node kind clusters with Kubernetes v1.36.4. Each cluster installed its own published v1.7.0 CRD bundle. Each focused run passed six cases, failed two and skipped one. Both failing cases requested three UDP listeners on distinct ports of a single Gateway. The skipped case requires a TLSRoute-capable Gateway. With the exact same binary, the literal GATEWAY-UDP Core setup stopped on both channels before any case body ran. All 23 upstream pods were Ready. The base Gateway with four HTTPS listeners on port 443 reported `Accepted=False/ListenersNotValid` and `Programmed=False/Invalid`.

| Upstream v1.7.0 case | Standard | Experimental |
|---|---|---|
| `UDPRoute` | PASS | PASS |
| `UDPRouteReferenceGrant` | PASS | PASS |
| `UDPRouteMultipleRoutesAttachment` | PASS | PASS |
| `UDPRouteInvalidCrossNamespaceBackendRef` | PASS | PASS |
| `UDPRouteInvalidBackendRefNonexistent` | PASS | PASS |
| `UDPRouteParentRefAttachAllListeners` | FAIL | FAIL |
| `UDPRouteParentRefPortAndSectionName` | FAIL | FAIL |
| `UDPRouteWeightedRouting` | PASS | PASS |
| `UDPRouteNotAllowedByListeners` | SKIP | SKIP |

The upstream weighted test opens a new UDP socket per datagram. Its successful readiness response marks the start of the weighted case in each capture. Each row counts all 500 distribution replies that followed it, including replies from any Service. The 20 earlier packets from the competing-route case carry the same distribution payload and are recorded separately. The stock Amphora template configured Keepalived 2.3.4 and kernel IPVS on Fedora 45. Each backend identified its Kubernetes Service and pod in its reply.

| Channel | `udp-backend-v1` (70) | `udp-backend-v2` (30) | `udp-backend-v3` (0) |
|---|---:|---:|---:|
| Standard | 351 | 149 | 0 |
| Experimental | 351 | 149 | 0 |

The additional grant test reached both Services before deleting their cross-namespace ReferenceGrant. On each cluster, the route reported `ResolvedRefs=False/RefNotPermitted`. All three fresh probes sent after withdrawal got no reply and none appeared on the backend-facing interface. After the denied Service's weight changed to zero, the permitted Service returned all 20 distinct payloads on the same VIP. The route kept its reference condition. The four packet captures reported zero kernel drops. After the final fixtures were removed, the guest IPVS table contained zero virtual services. Both disposable clusters, their Podman network and the VM overlay were removed. In each focused run, Octavia logged matching create and delete IDs for nine load balancers, nine listeners, four pools, five members and four health monitors. Selective packet loss for an invalid positive share remains open. [Local grant test](../../../../internal/controller/kind_udp_grant_test.go)

Two separate provider checks locate the multi-listener gap. The public Octavia 19.0.0 API accepted three distinct UDP listeners on one VIP. A `PUT` also made a single pool the default for two listeners (HTTP 200). Clearing the middle listener's default after restoring its own pool left the other two rendered configurations byte-identical. The test verified all ten synthetic resources were gone after cleanup. In a separate kernel smoke test, the stock renderer consumed synthetic model objects. Two ports on one VIP reached the same real echo pod via NodePort and PodIP respectively. Withdrawing one port preserved replies on the other. The controller at `c49134b1183b` rejected both upstream three-listener Gateways. The multiport run above records their later results.

The evidence bundle includes both case lists, packet counts, four synthetic packet captures, the three-port API probe and output, the two-port kernel log, tool and echo-image versions, common-setup failures for both controller revisions and file checksums. Bundle SHA-256 is `f864a84d7a432e2479df48884033c8f9f4b8f5507e74b92b7886ce76b42bdf1c`. [Download the report and captures](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-c49134b1183bbc6b3e4cf01c720bef2ae154d70e/hibiscus-udp-weighted-qualification-c49134b1183b.tar.gz)

## Baseline on the single-Service controller

At the 2026-11-21 controller revision `3819e699441970e51d7e546582dd53a827e6a320`, five upstream UDP cases passed on Standard. The simple route returned its expected Service and namespace. The cross-namespace Service answered with a grant, and deleting it produced `ResolvedRefs=False/RefNotPermitted`. Both competing routes were accepted, the listener counted two attached routes, and all 20 probes identified the older route's backend. The denied and missing backend reference cases also passed. [Upstream grant case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/udproute-reference-grant.go) · [Competing routes case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/udproute-multiple-routes-attachment.go)

Three cases failed. Both multi-listener Gateways reported `Accepted=False/ListenersNotValid`. The valid three-Service route specified weights of 70, 30 and 0. It reported `ResolvedRefs=False/UnsupportedValue`, and none of the ten upstream distribution attempts could send a full batch. The TLS-listener rejection case was recorded as a skip because its upstream assertion body returns immediately without TLSRoute support. [Upstream weighted case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/udproute-weighted-routing.go) · [TLS prerequisite](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/udproute-not-allowed-by-listeners.go)

On both channels the literal GATEWAY-UDP Core invocation with the original base stopped during setup. All 23 upstream pods were Ready. The `same-namespace-with-https-listener` Gateway requested four HTTPS listeners on port 443 and reported `Accepted=False/ListenersNotValid` and `Programmed=False/Invalid`. Zero profile test bodies ran. The scoped results above use the base with that Gateway omitted. [Upstream base](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/base/manifests.yaml)

## Weighted NodePort results

On 2026-11-21, a reconciler test selected two eligible workers for a weight-3 Service and one for a weight-1 Service. The Go Octavia adapter sent the compiled NodePort members to the stock Octavia 19.0.0 WSGI app. The public API returned weights 3 and 3 on port 32000 across the two worker addresses, and weight 2 on port 32001 on one address. The zero-weight backend contributed no member. [Controller test](../../../../internal/controller/l4route/udp_test.go)

Keepalived 2.2.8 loaded the exact stock Amphora configuration in a disposable VM with kernel IPVS 1.2.1. A client sent 400 datagrams on fresh UDP flows and checked each reply's identity and payload. The primary group returned 300 replies, 150 from each destination. The secondary returned 100. The backend sockets were synthetic worker destinations. This run covers the controller's weight calculation, the stock API and the rendered dataplane. Weighted traffic through real Kubernetes NodePorts, Neutron and a deployed Amphora requires separate evidence.

Native weights must be integers from 1 to 256. When the equal division of a Service's share among its eligible workers cannot preserve the requested ratio within that range, the controller withdraws forwarding. A positive backend with no eligible worker also causes withdrawal. The ratio applies to new flows while the programmed members are healthy. Dropping only an invalid backend's share remains open.

A separate cold-reconcile regression kept an operator-drained historical member at weight zero. When a weighted route returned to one reference, the surviving member's controller-managed weight returned to 1 and its marker was removed before the pool reattached. The public Octavia API confirmed the same transition for the surviving NodePort destination. [Reconciliation checks](../../../../internal/cloud/openstack/graph/tcp_graph_test.go) · [Downgrade procedure](../../../operator-recovery.md#network-prerequisites)

## Reproduce the local run

To exercise the two multi-port upstream cases, use the scoped base and set `HIBISCUS_UDP_CASES=UDPRouteParentRefAttachAllListeners,UDPRouteParentRefPortAndSectionName` for the runner below. For the independent listener lifecycle test, use the same cluster, base, Octavia fixture and binary, and select `-test.run '^TestKindUDPListenerLifecycle$'`. It starts with a single listener, adds a port, then removes the original one. It compares the VIP, listener and pool IDs and samples the surviving port during removal and checks the retired port afterward. [Lifecycle test](../../../../internal/controller/kind_udp_listener_lifecycle_test.go)

1. Start a fresh disposable Linux guest with rootful Podman, kind v0.33.0, kubectl v1.36.4, Keepalived, iproute2, ipvsadm, util-linux and the `ip_vs_wrr` module. Use one control-plane node and one worker, the kind bridge `10.89.0.0/24` and the default IPv4 pod subnet `10.244.0.0/16`. Name the cluster `hibiscus-udp-standard` or `hibiscus-udp-experimental`. Pin the node image to `kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed`. Install the matching release bundle and the fork's class-configuration CRD. Use a fresh cluster for the other channel. [Bundle verification and scoped base recipe](udp-kind.md#rerun)
2. Build the Go test binary from the fork with `CGO_ENABLED=0 go test -c -tags=envtest,kindtest -o /tmp/upstream-udp.test ./internal/controller`. On the Linux guest, set `GATEWAY_API_CHANNEL`, `KIND_CLUSTER_NAME` and `HIBISCUS_TEST_KUBECONFIG` for this cluster. Its kubeconfig context must be `kind-$KIND_CLUSTER_NAME`. Run `sudo ./udp-upstream-guest.sh setup "$KIND_CLUSTER_NAME"` in the provider fixture directory.
3. For the scoped run, set `HIBISCUS_STATUS_BASE_DIR` to the directory containing `tcp-status-base.yaml` and `HIBISCUS_UDP_CASES=all`. The [base selection recipe](../kind-tcp.md#reproduce-the-scoped-check) removes one HTTPS Gateway. The original base SHA-256 is `9f9ef20a1c81851e08380cfb87b7b611dc16390e1e4fe3be0b0a788115465352`. The scoped base SHA-256 is `eec15359b2f8ce095f3da26dfbc9485725c06a985a15796f1bde2ac79158f6ed`. For an attempt at the upstream GATEWAY-UDP profile with its three Core features, unset `HIBISCUS_STATUS_BASE_DIR` and set `HIBISCUS_UDP_CASES=profile`.
4. Install the pinned Octavia test environment and set `OCTAVIA_PYTHON` to its Python executable. The cluster's pools use UDP-CONNECT. Install Bash, `which`, `grep` and `nc` in the guest. Copy Octavia 19's stock `udp_check.sh` to `/var/lib/octavia/lvs/check/udp_check.sh`, owned by root with mode `0100`. Its SHA-256 is `e852c04778de895b99a42d28ef2476a8ef68867cb956cdc9d989a9a317cda2d7`. [Octavia installation](stock-udp.md#reproduce-the-public-api-and-configuration-check) · [Tagged health checker](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/amphorae/backends/utils/udp_check.sh)

A colocated guest can run the example below from the fork root. The guest user needs read access to the explicit kubeconfig. As the guest administrator, install the reviewed callback at the root-owned path below before running it with `sudo -n`. The observed split setup forwarded the WSGI socket over loopback SSH and ran the Go binary inside the guest.

```sh
out=$(mktemp -d)
sudo install -d -o root -g root -m 0755 /usr/local/libexec/hibiscus-qualification
sudo install -o root -g root -m 0755 docs/qualification/v1.7.0/provider/udp-upstream-guest.sh \
  /usr/local/libexec/hibiscus-qualification/udp-upstream-guest.sh
cat >"$out/run" <<'SH'
#!/bin/sh
exec /tmp/upstream-udp.test -test.v -test.run '^TestKindUpstreamUDPTraffic$' -test.timeout=35m
SH
cat >"$out/apply" <<'SH'
#!/bin/sh
exec sudo -n /usr/local/libexec/hibiscus-qualification/udp-upstream-guest.sh "$@"
SH
chmod 700 "$out/run" "$out/apply"
"$OCTAVIA_PYTHON" docs/qualification/v1.7.0/provider/udp-upstream-api.py \
  --run "$out/run" --apply "$out/apply" --timeout 2130
```

Retain the output and exit status for each run, then delete the named kind cluster and discard the guest. A failed common setup leaves every profile case unrun. A scoped case that fails or skips stays visible in the Go test output.

For the additional packet check, run the scoped setup with `HIBISCUS_UDP_CASES=grant-traffic`. It uses the upstream grant manifest and adds the local UDP Service as a second backend. Both Services must answer before grant deletion. New sockets must stop receiving replies with the denied reference at positive weight. After its weight changes to zero, all 20 new probes must identify the permitted Service while `ResolvedRefs` remains false. This is a separately named local test. [Grant withdrawal test](../../../../internal/controller/kind_udp_grant_test.go)
