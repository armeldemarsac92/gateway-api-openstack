# Stock Hibiscus Amphora TCP probe

Octavia v2 created a TCP listener, a separate TCP pool, and a member. After a listener `PUT` set `default_pool_id`, HAProxy forwarded a 256-byte non-HTTP payload unchanged to a TCP echo server and returned the same bytes. A second `PUT` cleared `default_pool_id`. A fresh HAProxy process used the resulting configuration on the same VIP and port. The client could connect but got no echo, and the member received no additional payload. The member, pool, and listener were then deleted through the API and returned `404` after simulated worker completion.

The probe uses Octavia's upstream `BaseAPITest` WSGI/SQLite fixture, the real Amphora API provider driver, and `JinjaTemplater.build_config` with model data read back from SQLite. The generated configuration goes directly to HAProxy. The fixture uses no-auth, substitutes Neutron and messaging dependencies, and advances provisioning statuses in place of the Amphora worker. The probe covers the Octavia API shape and local TCP forwarding. Cloud credentials, Neutron reachability, worker convergence, Amphora agent reload, failover, the Gateway API controller and its conformance suite remain outside the run.

## Reproduce

Use Linux with unprivileged user namespaces, Python 3.14, and HAProxy 3.0.19. The version pins are `octavia` 19.0.0 at `93689cb7701da65b5aad387bb824c477c64b0f2b` and `octavia-lib` 3.12.0 at `bc03e1856b60517e1b75db4fdf9b3edf55f3a019`. [Hibiscus Octavia release](https://releases.openstack.org/hibiscus/index.html#octavia) · [Hibiscus octavia-lib release](https://releases.openstack.org/hibiscus/index.html#octavia-lib)

The tested HAProxy build reports `3.0.19-ade9c1262`. To build it from the upstream archive, verify SHA-256 `85f2743a44e4226a0176926fbc8eebfa5551d22fe5f44366877570b6e18c6664`, then run `make TARGET=linux-glibc USE_PCRE2=1` in the extracted directory with a C compiler and PCRE2 development headers. [HAProxy source archive](https://www.haproxy.org/download/3.0/src/haproxy-3.0.19.tar.gz) · [Published SHA-256](https://www.haproxy.org/download/3.0/src/haproxy-3.0.19.tar.gz.sha256)

From this repository's root, set `HAPROXY_BIN` to the built binary's absolute path. The UID/GID mapping lets HAProxy honor the stock `user nobody` directive without host privileges. Client and server use ephemeral loopback ports.

```sh
python3.14 -m venv /tmp/octavia-tcp-venv
/tmp/octavia-tcp-venv/bin/python -m pip install \
  'octavia==19.0.0' 'octavia-lib==3.12.0' \
  'fixtures==4.3.2' 'oslotest==6.2.0' 'WebTest==3.0.7'
unshare --user --map-user=65534 --map-group=65534 \
  /tmp/octavia-tcp-venv/bin/python -u \
  docs/qualification/v1.7.0/provider/stock-tcp.py --haproxy "$HAPROXY_BIN"
```

The probe exits successfully with `Ran 1 test` and `OK` after validating both HAProxy configurations and checking delivery before and after detachment.
