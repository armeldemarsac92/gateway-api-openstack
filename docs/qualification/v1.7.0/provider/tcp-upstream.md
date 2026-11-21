# TCPRoute traffic through stock Octavia on kind

The local runner uses the production controller watches and Endpoint mode with the original Gateway API v1.7.0 Services and echo pods. Gophercloud sends load balancer, listener, pool and member requests to the public Octavia 19.0.0 WSGI application. The Amphora worker renders the configuration that HAProxy 3.0.19 runs in the kind guest. The fixture provides synthetic network topology, VIP allocation, provisioning completion and agent transport. [Runner](../../../../internal/controller/kind_tcp_traffic_test.go) · [Provider fixture](udp-upstream-api.py) · [Guest agent callback](http-upstream-guest.sh)

The focused traffic cases use one TCP listener per Gateway. `TCPRouteMultipleRoutesAttachment` requires both routes to report `Accepted=True` and the listener to count two attachments. The upstream client then opens 100 separate TCP connections and verifies that only the older route's pod answers. `TCPRouteReferenceGrant` checks a cross-namespace echo reply before deleting the grant. After deletion, its assertion requires the route status to report `ResolvedRefs=False/RefNotPermitted`. [Multiple routes](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/tcproute-multiple-routes-attachment.go) · [ReferenceGrant](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/tcproute-reference-grant.go)

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

Use `HIBISCUS_TCP_CASES=status` for the three tagged invalid-route cases. For the full Core attempt, set `HIBISCUS_TCP_CASES=profile` and unset `HIBISCUS_TCP_BASE_DIR`. This selects the original upstream base, registry, GATEWAY-TCP Core features and the 300-second namespace readiness timeout. Retain the setup result and the number of executed case bodies separately.

A capture of loopback ports 9310 and 9320 shows the VIP side. A capture of TCP port 3000 on the kind bridge identifies the responding backend. The echo protocol starts with a server greeting and the `TEST` client line returns a JSON record with the pod and namespace. [Upstream client](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/tcp/tcp.go)
