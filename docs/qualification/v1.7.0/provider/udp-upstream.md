# Gateway API v1.7.0 UDP traffic on kind

The upstream client sends UDP datagrams to the IP published in Gateway status and the port in the listener spec. In this qualification the Go controller reads the upstream ClusterIP Services and EndpointSlices. Its OpenStack adapter programs the Octavia 19.0.0 public API. The fixture renders the persisted UDP listener with the Amphora LVS template, starts Keepalived with those bytes and routes packets through kernel IPVS to the actual echo pod. [Upstream UDP client](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/udp/udp.go) · [Go runner](../../../../internal/controller/kind_udp_traffic_test.go) · [Octavia fixture](udp-upstream-api.py) · [Guest network](udp-upstream-guest.sh)

The fixture supplies subnet discovery, VIP allocation, RPC completion and delivery of each changed listener configuration. It runs HTTP base Gateways through the public API and installs the UDP listeners in the guest dataplane. Testing a deployed Amphora, Neutron access rules and Magnum lifecycle requires an OpenStack environment. [Live qualification ledger](../live-runs.md)

## Reproduce the local run

1. Start a fresh disposable Linux guest with rootful Podman, kind v0.33.0, kubectl v1.36.4, Keepalived, iproute2, ipvsadm, util-linux and the `ip_vs_wrr` module. Use one control-plane node and one worker, the kind bridge `10.89.0.0/24` and the default IPv4 pod subnet `10.244.0.0/16`. Name the cluster `hibiscus-udp-standard` or `hibiscus-udp-experimental`. Pin the node image to `kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed`. Install the matching release bundle and the fork's class-configuration CRD. Use a fresh cluster for the other channel. [Bundle verification and scoped base recipe](udp-kind.md#rerun)
2. Build the Go test binary from the fork with `CGO_ENABLED=0 go test -c -tags=envtest,kindtest -o /tmp/upstream-udp.test ./internal/controller`. On the Linux guest, set `GATEWAY_API_CHANNEL`, `KIND_CLUSTER_NAME` and `HIBISCUS_TEST_KUBECONFIG` for this cluster. Its kubeconfig context must be `kind-$KIND_CLUSTER_NAME`. Run `sudo ./udp-upstream-guest.sh setup "$KIND_CLUSTER_NAME"` in the provider fixture directory.
3. For the scoped run, set `HIBISCUS_STATUS_BASE_DIR` to the directory containing `tcp-status-base.yaml` and `HIBISCUS_UDP_CASES=all`. The [base selection recipe](../kind-tcp.md#reproduce-the-scoped-check) removes one HTTPS Gateway. The original base SHA-256 is `9f9ef20a1c81851e08380cfb87b7b611dc16390e1e4fe3be0b0a788115465352`. The scoped base SHA-256 is `eec15359b2f8ce095f3da26dfbc9485725c06a985a15796f1bde2ac79158f6ed`. For an attempt at the upstream GATEWAY-UDP profile with its three Core features, unset `HIBISCUS_STATUS_BASE_DIR` and set `HIBISCUS_UDP_CASES=profile`.
4. Install the pinned Octavia test environment and set `OCTAVIA_PYTHON` to its Python executable. The cluster's pools use UDP-CONNECT. Install Bash, `which`, `grep` and `nc` in the guest. Copy Octavia 19's stock `udp_check.sh` to `/var/lib/octavia/lvs/check/udp_check.sh`, owned by root with mode `0100`. Its SHA-256 is `e852c04778de895b99a42d28ef2476a8ef68867cb956cdc9d989a9a317cda2d7`. [Octavia installation](stock-udp.md#reproduce-the-public-api-and-configuration-check) · [Tagged health checker](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/amphorae/backends/utils/udp_check.sh)

A colocated guest can run the example below from the fork root. The guest user needs read access to the explicit kubeconfig and permission to run the guest callback with `sudo -n`. The observed split setup forwarded the WSGI socket over loopback SSH and ran the Go binary inside the guest.

```sh
out=$(mktemp -d)
export HIBISCUS_GUEST_CALLBACK="$PWD/docs/qualification/v1.7.0/provider/udp-upstream-guest.sh"
cat >"$out/run" <<'SH'
#!/bin/sh
exec /tmp/upstream-udp.test -test.v -test.run '^TestKindUpstreamUDPTraffic$' -test.timeout=35m
SH
cat >"$out/apply" <<'SH'
#!/bin/sh
exec sudo -n "$HIBISCUS_GUEST_CALLBACK" "$@"
SH
chmod 700 "$out/run" "$out/apply"
"$OCTAVIA_PYTHON" docs/qualification/v1.7.0/provider/udp-upstream-api.py \
  --run "$out/run" --apply "$out/apply" --timeout 2130
```

Retain the output and exit status for each run, then delete the named kind cluster and discard the guest. A failed common setup leaves every profile case unrun. A scoped case that fails or skips stays visible in the Go test output.
