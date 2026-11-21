# UDP forwarding with stock Hibiscus Amphora

## Controller adapter and private VIP

The fork’s Go adapter and Gophercloud submitted 18 mutations to the unmodified Octavia 19.0.0 v2 WSGI application. They created a private UDP listener, a UDP `ROUND_ROBIN` pool and a member. A new adapter rediscovered the same load balancer, listener and pool and issued zero writes. Changing the member from A to B detached the listener before replacing the member. Enabling `UDP-CONNECT` on B also detached the listener until the monitor existed. Deleting the route and Gateway restored the original project inventory. It contained the simulated Magnum API and OCCM Service load balancers with their two TCP listeners, two pools and two members. [Go adapter check](../../../../internal/cloud/openstack/graph/hibiscus_udp_traffic_test.go) · [Public API fixture](stock-udp.py)

For each listener change the Amphora worker driver rendered the graph it read from Octavia’s database. An isolated Linux guest replayed the six resulting configurations in order. The client opened a fresh UDP socket for each request. Both members had native weight 1.

| Listener state | Guest observation |
| --- | --- |
| A attached | Kernel IPVS selected A. A returned its identifier and the original 256 bytes. |
| Detached, then B attached | The detached VIP returned `ECONNREFUSED`. The next configuration sent the datagram to B. |
| Detached, then B attached with `UDP-CONNECT` | The public API reported delay 10 s, timeout 5 s and both retry counts 3. The stock checker sent a probe to B and a separate 256-byte request through the VIP reached B. |
| Route withdrawn | IPVS had no virtual service. A new VIP query returned `ECONNREFUSED`. Direct queries still reached both backends. |

The API run used the tagged SQLite functional fixture, simulated Neutron and messaging, and the actual Amphora worker database tasks for deletion. The Linux run used Keepalived 2.2.8, kernel IPVS and Octavia’s own `udp_check.sh`. The Fedora binary reported a `script_security` warning for the stock monitored template. Its optional `--config-test` exited 6. The Amphora service starts the file directly, and that path sent the probe and forwarded traffic in the guest. A deliberately injected echo on the detached VIP made the checker fail. [Amphora upload and restart](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/amphorae/backends/agent/api_server/keepalivedlvs.py) · [UDP checker](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/amphorae/backends/utils/udp_check.sh)

Separate Kubernetes 1.36.2 API servers exercised the Standard and Experimental release bundles. A Service exposed port 53 over both TCP and UDP. The controller selected only its UDP worker NodePort. Switching the EndpointSlice to TCP withdrew forwarding, restoring UDP resumed it, and revoking the UDPRoute grant withdrew it again with the TCPRoute grant still present. The cloud provider recorded the requested graph in this test. Live Magnum provisioning, delivery through a deployed Amphora to the selected Kubernetes Service, weighted invalid backends and the complete UDP profile remain open. [Controller watches and NodePort test](../../../../internal/controller/envtest_udp_test.go)

A separate Standard rehearsal used a live Kubernetes UDPRoute and NodePort with the Go adapter and the Octavia worker renderer. Packet capture confirmed the resulting VIP request and reply at a dedicated kind worker. [Controller to worker packet evidence](udp-worker.md)

## Direct provider probe

One Octavia UDP listener at `192.0.2.30:18053` forwarded a 256-byte binary datagram to backend A. Updating its public `default_pool_id` to a second UDP pool switched the same VIP and port to backend B. Both replies included the expected backend identifier and the unchanged payload. Clearing `default_pool_id` removed the IPVS virtual service. A new UDP socket then received `ECONNREFUSED` from the VIP and neither backend received a third datagram. Both backends still echoed a direct client datagram after the detach. Each saw exactly one packet via the VIP and one direct control packet.

| Public listener state | Kernel IPVS readback | Client |
| --- | --- | --- |
| Pool A attached | UDP, `wrr`, NAT destination `198.51.100.11:15001`, weight 3 | Backend A and all 256 bytes |
| Pool B attached | UDP, `wrr`, NAT destination `198.51.100.12:15002`, weight 1 | Backend B and all 256 bytes |
| Default pool cleared | No virtual service | `ECONNREFUSED`, no echo |

The Octavia v2 API created an unattached UDP listener, two independent `ROUND_ROBIN` UDP pools and one member in each pool. Each listener `PUT` was followed by a `GET` of the selected ID. Both pool members remained readable after the detach. A separate API test returned HTTP 501 for an L7 `REJECT` policy on a UDP listener. The Amphora driver reported that UDP listeners do not support L7 policies and issued no policy creation message. [Tagged Amphora API driver](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/drivers/amphora_driver/v2/driver.py#L395-L406)

The API fixture uses the upstream WSGI app, SQLite and the real Amphora API provider. It substitutes Neutron and messaging and advances provisioning status in place of the worker. The real Amphora worker-side driver reads the persisted load balancer and sends the stock `LvsJinjaTemplater` output to the Amphora REST client. The fixture captures `upload_udp_config` and checks that a `reload_listener` follows. The installed agent configuration defaults to `keepalived_lvs`. [Worker driver](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/amphorae/drivers/haproxy/rest_api_driver.py#L118-L166) · [LVS renderer](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/jinja/lvs/jinja_cfg.py) · [Agent configuration](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/config.py#L214)

The local traffic run passed those exact bytes to the Fedora Keepalived `2.2.8-10.fc44` binary with LVS support on Linux `7.2.8-200.fc44.x86_64`, IPVS `1.2.1`. Each file passed `--config-test` before a fresh Keepalived process used it. Replacing the process follows the stock agent's choice to translate UDP reload into restart. The guest kept the same VIP, links and echo processes throughout. The unprivileged host QEMU process used `-nic none`. IPVS, its WRR scheduler, and veth modules loaded only in the guest, and the client, Keepalived and backends were in separate guest network namespaces. The host allowed user and network namespace creation but its IPVS and WRR modules were unloaded, so the probe used a separate kernel. [Fedora Keepalived build](https://packages.fedoraproject.org/pkgs/keepalived/keepalived/fedora-44.html) · [Amphora UDP restart behavior](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/amphorae/backends/agent/api_server/keepalivedlvs.py#L173-L197)

This direct provider test isolates one selected pool per listener and withdrawal of its virtual service. The detached socket error leaves silent-drop behavior unverified. The weights in the table belong to native members in separate pools. A later run exercised the controller's weighted members and grant withdrawal using new UDP sockets on two clusters. Existing-flow persistence and dropping only the invalid backend's share remain open. [Weighted Endpoint results](udp-upstream.md#weighted-endpoint-results) [Gateway API v1.7.0 UDPRoute contract](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/udproute_types.go)

## Reproduce the public API and configuration check

Use Python 3.14 and the pinned Hibiscus requirements. Install `pyroute2` explicitly for the worker-side driver's import. The optional output directory must be new. [Octavia and octavia-lib releases](https://releases.openstack.org/hibiscus/index.html#octavia) · [Pinned constraints](https://github.com/openstack/requirements/blob/bbeadfcdd6cd8ac9abf888661d3fc65fa45c7480/upper-constraints.txt)

```sh
python3.14 -m venv /tmp/octavia-udp-venv
/tmp/octavia-udp-venv/bin/python -m pip install \
  -c 'https://raw.githubusercontent.com/openstack/requirements/bbeadfcdd6cd8ac9abf888661d3fc65fa45c7480/upper-constraints.txt' \
  'octavia==19.0.0' 'octavia-lib==3.12.0' fixtures oslotest WebTest pyroute2
/tmp/octavia-udp-venv/bin/python -u \
  docs/qualification/v1.7.0/provider/stock-udp.py --output-dir /tmp/octavia-udp-configs
```

Expect two passing tests and three generated files named `pool-a.conf`, `pool-b.conf` and `detached.conf`.

To exercise the controller adapter against Octavia, run these commands from the fork root. The fixture opens a loopback WSGI endpoint and verifies the complete inventory after cleanup. This run emits `pool-b-monitored.conf` in addition to the other three files.

```sh
go test -c -o /tmp/hibiscus-udp-graph.test ./internal/cloud/openstack/graph
/tmp/octavia-udp-venv/bin/python docs/qualification/v1.7.0/provider/stock-udp.py \
  --controller-test /tmp/hibiscus-udp-graph.test --output-dir /tmp/controller-udp-configs
```

## Reproduce the local traffic check

Copy [the traffic script](stock-udp-traffic.py) and the directory of generated files into a fresh disposable Linux guest with no external NIC. In the guest, use root for the namespace setup and test. Install Python 3.14, iproute2, and a Keepalived build with LVS support before starting the guest offline. The test expects a fresh `amphora-haproxy` namespace with an empty IPVS table.

Run this from the guest directory containing `stock-udp-traffic.py` and `octavia-udp-configs/`. The module and network preparation is confined to the guest.

```sh
modprobe ip_vs_wrr
modprobe veth

for ns in client amphora-haproxy backend; do
  ip netns add "$ns"
  ip -n "$ns" link set lo up
done

ip link add client0 netns client type veth peer name front0 netns amphora-haproxy
ip link add backend0 netns backend type veth peer name back0 netns amphora-haproxy

ip -n client address add 192.0.2.10/24 dev client0
ip -n amphora-haproxy address add 192.0.2.30/24 dev front0
ip -n amphora-haproxy address add 198.51.100.1/24 dev back0
ip -n backend address add 198.51.100.11/24 dev backend0
ip -n backend address add 198.51.100.12/24 dev backend0

ip -n client link set client0 up
ip -n amphora-haproxy link set front0 up
ip -n amphora-haproxy link set back0 up
ip -n backend link set backend0 up

ip -n client route add default via 192.0.2.30
ip -n backend route add default via 198.51.100.1
ip netns exec amphora-haproxy sysctl -w net.ipv4.ip_forward=1

python3.14 -S stock-udp-traffic.py octavia-udp-configs
```

For the controller’s configurations, install Bash, `which`, `grep` and `nc` inside the disposable guest. The run used Fedora Ncat 7.92 and the stock Octavia 19 checker installed root-owned at `/var/lib/octavia/lvs/check/udp_check.sh` with mode `0100`, as the Amphora agent installs it. Keepalived and the checker inherit the normal system service `PATH`. With the same isolated namespace setup, run `python3.14 -S stock-udp-traffic.py controller-udp-configs --first-weight 1 --with-monitor`.

The script starts two echo servers, runs Keepalived against each of the three files in order, reads the kernel IPVS table for each state, and uses fresh UDP sockets in the client namespace. It exits successfully with `STOCK_UDP_TRAFFIC_PASS` once both backends report exactly one VIP datagram and one direct control datagram. In the detached phase a UDP reply fails the run. The script reports `ECONNREFUSED` and a receive timeout separately because a timeout alone cannot establish that packets were silently dropped. The script stops its child processes on exit. A separate negative run with an echo socket bound to the detached VIP returned a 257-byte reply and triggered exit status 1. Both runs left no Keepalived or echo processes. Discard the guest after the test.
