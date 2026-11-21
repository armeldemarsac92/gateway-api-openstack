# Stock Hibiscus Amphora TCP probe

Octavia v2 created a TCP listener, a separate TCP pool, and a member. After a listener `PUT` set `default_pool_id`, HAProxy forwarded a 256-byte non-HTTP payload unchanged to a TCP echo server and returned the same bytes. A second `PUT` cleared `default_pool_id`. A fresh HAProxy process used the resulting configuration on the same VIP and port. The client could connect but got no echo, and the member received no additional payload. The member, pool, and listener were then deleted through the API and returned `404` after simulated worker completion.

A separate TCP listener refused a public `REDIRECT_TO_POOL` L7 policy with HTTP 501. The Amphora driver reported that TCP listeners do not support L7 policies and sent no creation message to a worker. This covers a prerequisite for the proposed TLS passthrough mapping. Choosing between backends using the ClientHello SNI still requires a public stream-routing primitive. [Provider study](provider-feasibility.md#minimal-traffic-cases)

In the upstream in-memory SQLite fixture, listing policies immediately after that 501 returned one `PENDING_CREATE` record. The API controller's error path explicitly rolls back its SQL transaction. Database rollback on a deployed Octavia service remains unverified. The rejection runs in its own fresh fixture and does not advance worker status after the failure. [Controller transaction](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/controllers/l7policy.py#L164)

The probe uses Octavia's upstream `BaseAPITest` WSGI/SQLite fixture, the real Amphora API provider driver, and `JinjaTemplater.build_config` with model data read back from SQLite. The generated configuration goes directly to HAProxy. The fixture uses no-auth, substitutes Neutron and messaging dependencies, and advances provisioning statuses in place of the Amphora worker. The probe covers the Octavia API shape and local TCP forwarding. Cloud credentials, Neutron reachability, worker convergence, Amphora agent reload, failover, the Gateway API controller and its conformance suite remain outside the run.

## Controller and provider checks

The controller envtest creates a TCP Gateway with a namespace selector and two tenant TCPRoutes. Both routes report `Accepted=True` and the listener counts two attachments. Only the oldest route supplies a forwarding endpoint in the provider graph. Removing its TCPRoute ReferenceGrant withdraws that endpoint while an HTTPRoute grant to the same Service remains. When the listener stops admitting the oldest route's namespace, the second route becomes the only attachment and supplies its own endpoint. The test uses the release CRDs, a Kubernetes API server and the production controller watches. Its provider records the graph. [API-server scenario](../../../../internal/controller/envtest_tcp_test.go)

The adapter checks IDs reported by the load balancer against the independently listed children. An incomplete inventory holds the route's cleanup checkpoint. A failed listener on a mutable parent permits a verified withdrawal. [Recovery procedure](../../../operator-recovery.md)

The Gophercloud adapter creates a TCP listener with no default pool, provisions the selected pool and members, then updates the listener. Its independent traffic test replays that inventory through the stock Amphora renderer and HAProxy. A new adapter rediscovers the graph without writes. After the listener clears its selected pool, a fresh connection delivers zero bytes to the still-running member. Changes to the serving membership detach the pool before reconciliation. [Provider lifecycle](../../../../internal/cloud/openstack/graph/tcp_graph_test.go) · [Binary traffic test](../../../../internal/cloud/openstack/graph/hibiscus_traffic_test.go)

If the release CRDs change while a route is bound, the controller checks the existing parent, programmed route generation and authorized Service. An edited backend or a lost Service triggers cleanup even while new provisioning is frozen. A NodePort Service without ready endpoints keeps `ResolvedRefs=True` after the port resolves and reports unavailable members through its program condition. A restarted writer can also remove a detached pool after the listener disappears, using the TCPRoute UID and verified cloud ownership. [Controller regressions](../../../../internal/controller/l4route/graph_test.go) · [Cloud cleanup regression](../../../../internal/cloud/openstack/graph/tcp_graph_test.go)

In a separate local weighted probe, Gophercloud created and updated TCP `ROUND_ROBIN` members through the pinned Octavia public API and read each weight back by member ID. Octavia accepted 0 and 256 and returned HTTP 400 for 257. It accepted clearing `SOURCE_IP` persistence on the pool. HAProxy 3.0.19 ran the stock configuration rendered from those API writes. With weights 7, 7 and 6, three echo backends received 703, 702 and 595 of 2,000 new connections. After changing the same members to equal weights, each received 500 of 1,500 new connections. [Member API types](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/types/member.py) · [HAProxy pool template](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/jinja/haproxy/combined_listeners/templates/macros.j2)

The loopback observations isolate the stock API and template. The upstream weighted TCPRoute case also reached real worker pods on both kind channels. The private VIP, failover and cleanup on a deployed Amphora and the full GATEWAY-TCP report remain open. [Kubernetes connection counts](tcp-upstream.md#weighted-tcp-connections)

## Reproduce the weighted member probe

The source archive includes the Gophercloud helper and the Python probe used for the TCP 7:7:6 and 1:1:1 runs. Its README gives the build and invocation from the pinned Go module. The test uses three fresh loopback echo servers and the stock Amphora template rendered from the fixture’s persisted public-API writes. Archive SHA-256 is `9395b52e4a3033b3e7b7bfa5b2a58fdb3f47f397e3606767b3fa7bc64e217a15`. [Source and checksums](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-ce6f51797fb7c31a63c1f63577cb6938ecaff618/hibiscus-provider-probes-75101c26553d.tar.gz)

## Reproduce

Use Linux with unprivileged user namespaces, Python 3.14, and HAProxy 3.0.19. The version pins are `octavia` 19.0.0 at `93689cb7701da65b5aad387bb824c477c64b0f2b` and `octavia-lib` 3.12.0 at `bc03e1856b60517e1b75db4fdf9b3edf55f3a019`. [Hibiscus Octavia release](https://releases.openstack.org/hibiscus/index.html#octavia) · [Hibiscus octavia-lib release](https://releases.openstack.org/hibiscus/index.html#octavia-lib)

The tested HAProxy build reports `3.0.19-ade9c1262`. To build it from the upstream archive, verify SHA-256 `85f2743a44e4226a0176926fbc8eebfa5551d22fe5f44366877570b6e18c6664`, then run `make TARGET=linux-glibc USE_PCRE2=1` in the extracted directory with a C compiler and PCRE2 development headers. [HAProxy source archive](https://www.haproxy.org/download/3.0/src/haproxy-3.0.19.tar.gz) · [Published SHA-256](https://www.haproxy.org/download/3.0/src/haproxy-3.0.19.tar.gz.sha256)

From this repository's root, set `HAPROXY_BIN` to the built binary's absolute path. The UID/GID mapping lets HAProxy honor the stock `user nobody` directive without host privileges. Client and server use ephemeral loopback ports.

```sh
python3.14 -m venv /tmp/octavia-tcp-venv
/tmp/octavia-tcp-venv/bin/python -m pip install \
  -c 'https://raw.githubusercontent.com/openstack/requirements/bbeadfcdd6cd8ac9abf888661d3fc65fa45c7480/upper-constraints.txt' \
  'octavia==19.0.0' 'octavia-lib==3.12.0' fixtures oslotest WebTest
unshare --user --map-user=65534 --map-group=65534 \
  /tmp/octavia-tcp-venv/bin/python -u \
  docs/qualification/v1.7.0/provider/stock-tcp.py --haproxy "$HAPROXY_BIN"
```

The probe exits successfully with `Ran 2 tests` and `OK` after checking the rejected L7 policy and validating TCP delivery before and after detachment. The pinned Hibiscus constraints select SQLAlchemy 2.0.52. [OpenStack requirements](https://github.com/openstack/requirements/blob/bbeadfcdd6cd8ac9abf888661d3fc65fa45c7480/upper-constraints.txt)

With the same stock Octavia environment and HAProxy, run the controller adapter's binary traffic test from the repository root.

```sh
HIBISCUS_OCTAVIA_PYTHON=/tmp/octavia-tcp-venv/bin/python \
HIBISCUS_HAPROXY="$HAPROXY_BIN" \
go test -race -count=1 -v -run '^TestHibiscusTCPTraffic$' ./internal/cloud/openstack/graph
```

After `make envtest-assets`, run `make test-envtest` for Standard and `make test-envtest GATEWAY_API_CHANNEL=experimental` for Experimental. Each command starts a separate API server. The scenario is named `TCP listener selects one authorized backend`.
