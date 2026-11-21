# Stock Hibiscus Amphora gRPC probe

A dedicated Octavia `TERMINATED_HTTPS` listener routed `/feasibility.v1.Echo/First` to backend A and `/feasibility.v1.Echo/Second` to backend B. Two independent grpcio servers each implemented both methods. The client used authority `grpc.example.test`, validated the listener certificate, and presented its own certificate. Both TLS hops used HTTP/2.

| Stock configuration | Observed result |
|---|---|
| Default pool A with no L7 policies | A, A, A, A |
| Two `REDIRECT_TO_POOL` policies with `PATH/EQUAL_TO` rules and no default pool | A, B, A, B |

Each run called `First`, `Second`, `First`, `Second` over one client channel. All four RPCs in each run returned gRPC `OK`, the expected backend and method in the response, and that backend's `echo-backend` trailer. HAProxy's frontend stats counted one connection and four requests. A fifth RPC on the exact-path connection used the case-variant `/feasibility.v1.Echo/second` and reached neither server. HAProxy returned HTTP 503, which grpcio mapped to `UNAVAILABLE`.

The probe creates and reads back the listener, pools, members, policies and rules through Octavia's upstream v2 WSGI/SQLite test fixture and real Amphora provider driver. Octavia's real Amphora HAProxy worker driver reads the stored graph, loads certificates through `local_cert_manager`, and renders the stock configuration. Its agent client writes uploaded certificate files and the rendered configuration into a temp directory. HAProxy validates and runs the exact bytes from that upload. The fixture uses no-auth, simulates Neutron, messaging and provisioning status transitions, and starts a local HAProxy process for each configuration in place of an Amphora agent reload. The test CA and its keys stay in the temp directory. The upstream TLS pool members each use a distinct leaf certificate valid for `grpc.example.test`, which is the SNI the stock template forwards. [Tagged Amphora worker driver](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/amphorae/drivers/haproxy/rest_api_driver.py) · [Tagged HAProxy template](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/jinja/haproxy/combined_listeners/templates/macros.j2)

This result covers unary gRPC on the loopback TLS graph. The upstream Gateway API exact-match fixture uses an HTTP listener and h2c backends and still needs separate cleartext qualification. Hostname routing, long identifiers, route-level metadata mutation, weighted invalid targets, cloud provisioning, the Gateway API controller and conformance also remain outside this run. [Upstream exact-match test](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/grpcroute-exact-method-matching.go)

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

The probe exits with `Ran 1 test` and `OK` after the two configurations and the case-variant control pass.
