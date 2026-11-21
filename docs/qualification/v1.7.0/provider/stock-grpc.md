# Stock Hibiscus Amphora gRPC probe

A dedicated Octavia `TERMINATED_HTTPS` listener routed `/feasibility.v1.Echo/First` to backend A and `/feasibility.v1.Echo/Second` to backend B. Two independent grpcio servers each implemented both methods. The client used authority `grpc.example.test`, validated the listener certificate, and presented its own certificate. Both TLS hops used HTTP/2.

| Stock configuration | Observed result |
|---|---|
| Default pool A with no L7 policies | A, A, A, A |
| Two `REDIRECT_TO_POOL` policies with `PATH/EQUAL_TO` rules and no default pool | A, B, A, B |

Each run called `First`, `Second`, `First`, `Second` over one client channel. All four RPCs in each run returned gRPC `OK`, the expected backend and method in the response, and that backend's `echo-backend` trailer. HAProxy's frontend stats counted one connection and four requests. A fifth RPC on the exact-path connection used the case-variant `/feasibility.v1.Echo/second` and reached neither server. HAProxy returned HTTP 503, which grpcio mapped to `UNAVAILABLE`.

The probe creates and reads back the listener, pools, members, policies and rules through Octavia's upstream v2 WSGI/SQLite test fixture and real Amphora provider driver. Octavia's real Amphora HAProxy worker driver reads the stored graph, loads certificates through `local_cert_manager`, and renders the stock configuration. Its agent client writes uploaded certificate files and the rendered configuration into a temp directory. HAProxy validates and runs the exact bytes from that upload. The fixture uses no-auth, simulates Neutron, messaging and provisioning status transitions, and starts a local HAProxy process for each configuration in place of an Amphora agent reload. The test CA and its keys stay in the temp directory. The upstream TLS pool members each use a distinct leaf certificate valid for `grpc.example.test`, which is the SNI the stock template forwards. [Tagged Amphora worker driver](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/amphorae/drivers/haproxy/rest_api_driver.py) · [Tagged HAProxy template](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/jinja/haproxy/combined_listeners/templates/macros.j2)

## Cleartext backend transport

The v1.7.0 exact-match fixture uses an HTTP listener and declares `kubernetes.io/h2c` on port 8080 of both backend Services. The local probe registered the fixture’s `GrpcEcho/Echo` and `GrpcEcho/EchoTwo` paths on two plaintext grpcio servers and selected their HTTP pools with Octavia `PATH/EQUAL_TO` rules. [Upstream route](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/grpcroute-exact-method-matching.yaml) · [Backend Service ports](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/base/manifests.yaml#L617-L691)

| Connection | Result |
|---|---|
| Direct h2c to each backend before and after the proxy runs | The expected method returned gRPC `OK`, the backend identifier and its trailer. |
| HTTP listener to non-TLS HTTP pools | The client sent the HTTP/2 prior-knowledge preface. The proxy sent the matching RPC path in an HTTP/1.1 POST to each selected member. HAProxy returned HTTP/2 `:status 502` without a `grpc-status` trailer. grpcio mapped the response to `UNAVAILABLE`. Neither application handler ran. |
| Same graph with `alpn_protocols=["h2"]` on the listener and both pools | Octavia accepted the updates and returned the setting on readback. The rendered configurations were byte-identical and both calls failed the same way. The listener and pool APIs rejected `["h2c"]` with HTTP 400. |

The packet capture was taken on loopback in a private network namespace. It shows the client preface on the listener connection, then the literal request line `POST /gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo HTTP/1.1` on the first backend connection and the `EchoTwo` path on the second. Frames 53, 80, 105 and 132 contain the four HTTP/1.1 requests. Frames 57, 84, 109 and 136 contain the corresponding 502 responses from the listener. Capture SHA-256 is `f33f2a8602574031821cc92c70f0d05ae69c2a4d8d0ca35af0bf6eb6836daeba`. [Download the synthetic traffic capture](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-c49134b1183bbc6b3e4cf01c720bef2ae154d70e/stock-octavia19-grpc-h2c-f33f2a860257.pcap)

On this Amphora version, the missing setting is on the connection to the member. A candidate public extension would let a tenant request HTTP/2 prior knowledge on an HTTP pool with TLS disabled. The provider could advertise that capability and select the backend HTTP/2 multiplexer. HAProxy documents this as the server-side `proto h2` option. The capture used the stock rendered bytes. [Octavia worker mapping](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/jinja/haproxy/combined_listeners/jinja_cfg.py#L474-L497) · [HAProxy backend h2c](https://www.haproxy.com/documentation/haproxy-configuration-tutorials/protocol-support/http/#http2-over-http-h2c-to-the-server) · [Octavia pool API](https://docs.openstack.org/api-ref/load-balancer/v2/index.html#update-pool)

These runs exercise unary RPCs on a local provider graph. The controller’s Kubernetes GRPCRoute reconciliation, complete hostname and metadata matching, weighted invalid-target responses and live Gateway and Mesh conformance remain open.

## Reproduce

Use Linux with unprivileged user namespaces, Python 3.14, and the OpenSSL CLI. The tested HAProxy build reports `3.0.19-ade9c1262`. Verify archive SHA-256 `85f2743a44e4226a0176926fbc8eebfa5551d22fe5f44366877570b6e18c6664`, then run `make TARGET=linux-glibc USE_OPENSSL=1 USE_PCRE2=1` in the extracted directory with a C compiler and OpenSSL and PCRE2 development headers. The tested OpenSSL and PCRE2 versions were 3.5.8 and 10.47. [HAProxy source archive](https://www.haproxy.org/download/3.0/src/haproxy-3.0.19.tar.gz) · [Published SHA-256](https://www.haproxy.org/download/3.0/src/haproxy-3.0.19.tar.gz.sha256)

From this repository's root, set `HAPROXY_BIN` to that binary's absolute path. The UID/GID mapping lets HAProxy honor the stock `user nobody` directive. All three service ports are ephemeral loopback ports. The Hibiscus constraints pin grpcio 1.83.0, SQLAlchemy 2.0.52 and pyroute2 0.8.1. The probe explicitly installs pyroute2 because the Octavia wheel's dependency metadata omits it. [Hibiscus Octavia release](https://releases.openstack.org/hibiscus/index.html#octavia) · [Hibiscus octavia-lib release](https://releases.openstack.org/hibiscus/index.html#octavia-lib) · [Pinned Hibiscus constraints](https://github.com/openstack/requirements/blob/bbeadfcdd6cd8ac9abf888661d3fc65fa45c7480/upper-constraints.txt)

```sh
curl -fsSLo /tmp/octavia-grpc-constraints.txt \
  https://raw.githubusercontent.com/openstack/requirements/bbeadfcdd6cd8ac9abf888661d3fc65fa45c7480/upper-constraints.txt
python3.14 -m venv /tmp/octavia-grpc-venv
/tmp/octavia-grpc-venv/bin/python -m pip install -c /tmp/octavia-grpc-constraints.txt \
  'octavia==19.0.0' 'octavia-lib==3.12.0' grpcio pyroute2 fixtures oslotest WebTest
unshare --user --map-user=65534 --map-group=65534 \
  /tmp/octavia-grpc-venv/bin/python -u \
  docs/qualification/v1.7.0/provider/stock-grpc.py --haproxy "$HAPROXY_BIN"
```

The command runs two tests. The TLS test checks the default pool, exact paths and case-variant control. The plaintext test checks direct success before and after the proxied calls, the 502 response, backend handler counts and the API readback. A successful probe prints `Ran 2 tests` and `OK`. Its cleartext assertions reproduce the provider limitation.
