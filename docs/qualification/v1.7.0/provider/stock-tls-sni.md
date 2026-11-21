# Stock Hibiscus Amphora TLS passthrough

On `127.0.0.1:9443`, a stock TCP listener passed the TLS stream to the pool selected by `default_pool_id`. With pool A attached, `b.example.test` received A's leaf certificate and application reply, where the two-route fixture requires B. With pool B attached, `a.example.test` received B. Both names worked directly against both backends before and after the proxy run. [Two TLSRoute inputs](qualification-routes.yaml) · [Probe](stock-tcp.py)

| Listener default pool | `a.example.test` | `b.example.test` |
|---|---|---|
| A | A's leaf and reply | A's leaf and reply |
| Cleared | TCP connected, TLS EOF | TCP connected, TLS EOF |
| B | B's leaf and reply | B's leaf and reply |

Every probe opened a fresh TCP and TLS connection. The client trusted a one-run CA and verified the requested name. Both leaves covered both names, so the client could positively identify the wrong backend without a certificate-name error masking the result. The probe checked SHA-256 over each leaf's DER, the backend ID plus an unchanged 256-byte application payload, and the SNI observed by the selected backend. In this run the leaf digests were `72250469940710431b67a094003f46bf4d049f5b9d03ce8d90d58b1215169a32` for A and `1fc5198d3b989f404e5e352ae036e254ca195ad60675906e6dc9e15e22e4d883` for B.

The public API created one TCP listener with `timeout_tcp_inspect: 100`, two TCP pools and one member per pool. A listener `PUT` selected A, cleared the pool, then selected B, with a `GET` after each update. The upstream Octavia v2 WSGI fixture used SQLite and the Amphora provider driver. It substituted Neutron and messaging and advanced provisioning statuses for each API mutation. The stock Amphora worker-side driver read the persisted load balancer, rendered its combined HAProxy configuration and called upload and reload on a test REST client. Each uploaded configuration went unchanged into a fresh HAProxy process. The stock listener rendered a TCP bind, `tcp-request inspect-delay 100`, and at most one `default_backend`. It rendered no routing ACL or `use_backend` directive. [Worker upload path](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/amphorae/drivers/haproxy/rest_api_driver.py#L118-L208) · [Frontend template](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/jinja/haproxy/combined_listeners/templates/macros.j2#L173-L206)

An independent fixture submitted `REDIRECT_TO_POOL` to an opaque `HTTPS` listener using an `HTTPS` pool. The stock API returned HTTP 501 and issued no policy creation message to the worker. The existing TCP smoke test returned the same result for a TCP listener. HTTP and `TERMINATED_HTTPS` are the Amphora protocols admitted for L7 policies. The public L7 rule types include HTTP Host and client-certificate fields. Same-port TLSRoute passthrough needs a ClientHello SNI predicate and a pool-selection action in TCP mode. [TCP API result](stock-tcp.md) · [Tagged schema snapshot](stock-api-snapshot.txt) · [Amphora admission](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/drivers/amphora_driver/v2/driver.py#L395-L406)

## Packet evidence

The qualification capture contained 258 packets from the private namespace's loopback interface. TCP streams 4→5 and 6→7 carried both ClientHellos from the VIP to A on port 53415. Streams 10→11 and 12→13 carried both from the VIP to B on port 46407. Each ingress and egress pair carried the same SNI and ClientHello random. Streams 8 and 9 carried the two VIP-side ClientHellos after the detach, with no corresponding backend leg. TLS key material was ephemeral and stays out of the capture and report. Capture SHA-256 `d2e727d84ca152ded06576c70dd8eafff816f92eda001965989397a58f32af6a`. [Download the packet capture](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-0b512ec28ae055d6e1eb1460dc2a43e7b87c441f/stock-octavia19-tls-sni-d2e727d84ca1.pcap)

The wire run covered the Amphora TCP listener. Opaque HTTPS was checked for API admission. Wildcard precedence, unmatched names, OVN, deployed control-plane convergence, Amphora agent reload and Gateway API conformance were outside this run.

## Reproduce

Use the pins and Python environment in the [stock TCP probe](stock-tcp.md#reproduce), OpenSSL and a Linux user/network namespace with only loopback brought up. `OCTAVIA_PYTHON` and `HAPROXY_BIN` are absolute paths to the pinned Python and HAProxy executables. Run from the repository root.

```sh
unshare --user --map-root-user --net sh -c '
  ip link set lo up &&
  unshare --user --map-user=65534 --map-group=65534 "$1" -u \
    docs/qualification/v1.7.0/provider/stock-tcp.py --haproxy "$2" --tls-sni
' sh "$OCTAVIA_PYTHON" "$HAPROXY_BIN"
```

The selected mode exits zero after two tests, reporting the HTTPS policy rejection, eight direct exchanges, four proxied TLS exchanges and both detached failures. Omit `--tls-sni` to run the original TCP smoke tests.
