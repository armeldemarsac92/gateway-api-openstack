#!/usr/bin/env bash
set -euo pipefail
umask 077
here=$(rtk proxy realpath -- "$(rtk proxy dirname -- "$0")")
if [[ $# -ne 2 ]]; then
    rtk proxy printf 'Usage: %s {standard|experimental} NEW_RESULT_DIR\n' "$0" >&2
    exit 2
fi
: "${SHARED_REPLAY:?Unpack the pinned shared HTTPS replay first}" "${CONTROLLER_SOURCE:?}"
: "${CONTROLLER_REVISION:?}" "${CONTROLLER_MANAGER:?}" "${MANAGER_SHA256:?}" "${GATEWAY_API_SOURCE:?}" "${OCTAVIA_PYTHON:?}"
export SHARED_HTTPS_FIXTURE="$SHARED_REPLAY/shared-https-upstream"
prior="$SHARED_REPLAY/backend-tls-upstream"
export GATEWAY_API_REVISION=e26d183dbbc1bb782b78addbd26faba03955c152 GATEWAY_API_CHANNEL="$1"
case "$1" in
    standard) bundle=4e2602c4cbe1afbc394d86f4ac6522e1a5263b01133bba1e40a6d1a549e753a1 ;;
    experimental) bundle=e6d2d3407717f54b579b22ad4d164b6863895b3524f21250b0da0c1ff9a05d53 ;;
    *) exit 2 ;;
esac
export GATEWAY_API_BUNDLE="$prior/inputs/bundles/$1-install.yaml" HAPROXY_BIN="$prior/inputs/haproxy"
export KUBEBUILDER_ASSETS="$prior/inputs/envtest-1.36.2" ECHO_BINARY="$prior/inputs/echo-basic"
export WIRE_FIXTURE="$SHARED_REPLAY/method-gateway-case/release-1239d29f3fae/qualification/wire"
export OPERATOR_FIXTURE="$SHARED_REPLAY/sni-image-release" KIND_ECHO_ARCHIVE="$prior/echo-basic-v0.1.0.oci.tar"
: "${OCTAVIA_WHEEL:?}" "${OCTAVIA_LIB_WHEEL:?}" "${OCTAVIA_WHEEL_SHA256:?}" "${OCTAVIA_LIB_WHEEL_SHA256:?}"
# Git paths stay read-only, and the inputs must match the requested revision.
export GIT_OPTIONAL_LOCKS=0
export KIND_NODE_IMAGE=docker.io/kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed
if [[ -z "${BARBICAN_PATH:-}" ]]; then
    BARBICAN_PATH=$(rtk proxy "$OCTAVIA_PYTHON" -c 'import sysconfig; print(sysconfig.get_path("purelib"))')
    export BARBICAN_PATH
fi
export KIND_EXPERIMENTAL_PROVIDER=podman
RESULT_DIR=$(rtk proxy realpath -m -- "$2")
export RESULT_DIR
rtk proxy mkdir -- "$RESULT_DIR"
record_exit() {
    local status=$?
    rtk proxy printf '%s\n' "$status" > "$RESULT_DIR/runner.exit"
}
trap record_exit EXIT
for source in "$GATEWAY_API_SOURCE" "$CONTROLLER_SOURCE"; do
    test -z "$(rtk proxy git -C "$source" status --porcelain --untracked-files=no)"
done
test "$(rtk proxy git -C "$GATEWAY_API_SOURCE" rev-parse HEAD)" = "$GATEWAY_API_REVISION"
test "$(rtk proxy git -C "$CONTROLLER_SOURCE" rev-parse HEAD)" = "$CONTROLLER_REVISION"
(cd "$GATEWAY_API_SOURCE" && rtk proxy sha256sum -c "$here/upstream.sha256")
rtk proxy printf '%s  %s\n' \
    "$MANAGER_SHA256" "$CONTROLLER_MANAGER" "$bundle" "$GATEWAY_API_BUNDLE" \
    b7675809fd2cc0b9fb9273f1b0c85c422bc3c12361d66b99b83173ddb2d3c9b7 "$KIND_ECHO_ARCHIVE" \
    aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d "$prior/inputs/kind" \
    fa8468efa26f95ed83a36afeb97b79730de285450b7a8fae94f7cf5e6d2d0824 "$HAPROXY_BIN" \
    "$OCTAVIA_WHEEL_SHA256" "$OCTAVIA_WHEEL" \
    "$OCTAVIA_LIB_WHEEL_SHA256" "$OCTAVIA_LIB_WHEEL" | rtk proxy sha256sum -c -
work=$(rtk proxy mktemp -d /tmp/redirect.XXXXXXXX)
name="hibiscus-btls-redirect-$1-$$"
created=0
export LOCAL_KIND_ADMIN_CONFIG="${HIBISCUS_KIND_REMOTE_ADMIN_CONFIG:-$work/kubeconfig}" PROVIDER_INSTALL="$work/provider" CONFORMANCE_BINARY="${HIBISCUS_CASE_BINARY:-$work/registered.test}"
cleanup() {
    local status=$?
    if (( created )); then
        rtk proxy "$prior/inputs/kind" delete cluster --name "$name" --kubeconfig "$LOCAL_KIND_ADMIN_CONFIG" ||
            { if (( status == 0 )); then status=1; fi; }
    fi
    rtk proxy rm -rf -- "$work" || { if (( status == 0 )); then status=1; fi; }
    rtk proxy printf '%s\n' "$status" > "$RESULT_DIR/runner.exit"
    exit "$status"
}
trap cleanup EXIT
rtk proxy ln -s -- "$CONTROLLER_MANAGER" "$work/manager"
rtk proxy install -m 700 -- "$here/start-manager.sh" "$work/start-manager.sh"
export CONTROLLER_BINARY="$work/start-manager.sh"
rtk proxy "$OCTAVIA_PYTHON" -m pip --disable-pip-version-check install --no-index --no-deps --no-compile \
    --target "$PROVIDER_INSTALL" "$OCTAVIA_WHEEL" "$OCTAVIA_LIB_WHEEL"
if [[ -n ${HIBISCUS_CASE_BINARY:-} ]]; then
    rtk proxy printf '%s  %s\n' e58e80e1f89e92314f35210d7b1125c5e4855f01fcb2dcbc03c53a5b7fc9d983 "$here/registered_test.go" de9cdeb62c794b24716cc6f2548d784f8c2de9fcf0773c26d31fb828d3257d4d "$CONFORMANCE_BINARY" | rtk proxy sha256sum -c -
else
    rtk proxy go -C "$GATEWAY_API_SOURCE" test -mod=readonly -c -o "$CONFORMANCE_BINARY" "$here/registered_test.go"
fi
# These adjustments only extend fixture supervision and credential lifetime.
# The Go cases and their upstream timeouts use the original source checkout.
for module in scenario.py native_cloud.py kind_multi_relay.py; do
    rtk proxy cp -- "$SHARED_HTTPS_FIXTURE/$module" "$work/$module"
done
rtk proxy patch --silent --fuzz=0 --directory="$work" -p0 < "$here/fixture-deadlines.patch"
rtk proxy mv -- "$work/scenario.py" "$work/shared_scenario.py"
export SHARED_HTTPS_SCENARIO="$work/shared_scenario.py"
rtk proxy python3 "$here/instrument.py" "$work"
rtk proxy python3 "$here/grouped-worker.py" "$work"
if [[ -z ${HIBISCUS_KIND_REMOTE_ADMIN_CONFIG:-} ]]; then
    rtk proxy podman image exists "$KIND_NODE_IMAGE"
    rtk proxy mkdir "$work/etcd"
    rtk proxy cp -- "$prior/kind.yaml" "$work/kind.yaml"
    rtk proxy printf '  extraMounts:\n  - hostPath: %s\n    containerPath: /var/lib/etcd\n' "$work/etcd" >> "$work/kind.yaml"
    created=1
    rtk proxy systemd-run --user --scope --quiet -p Delegate=yes rtk proxy "$prior/inputs/kind" create cluster \
        --name "$name" --image "$KIND_NODE_IMAGE" --config "$work/kind.yaml" --kubeconfig "$LOCAL_KIND_ADMIN_CONFIG" --wait 90s
    test "$(rtk proxy podman exec "$name-control-plane" stat -f -c %T /var/lib/etcd)" = tmpfs
    rtk proxy podman exec -i "$name-control-plane" ctr -n k8s.io images import --digests - < "$KIND_ECHO_ARCHIVE"
fi
rtk proxy patch --silent --output="$work/kind_relay.py" "$prior/kind_relay.py" "$here/api-relay.patch"
export PYTHONPATH="$PROVIDER_INSTALL:$work:$WIRE_FIXTURE:$BARBICAN_PATH:$SHARED_HTTPS_FIXTURE:$prior"
export PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 QUALIFICATION_DEBUG=1 TMPDIR="$work" GOMAXPROCS=2
rtk proxy "$OCTAVIA_PYTHON" -B "$here/grpc_relay.py" outer \
    rtk proxy timeout -k 12 7450 unshare --user --map-root-user --net --pid --fork --kill-child --mount-proc \
    rtk proxy bash "$here/relay-network.sh" "$OCTAVIA_PYTHON" -B "$prior/network.py" \
    "$OCTAVIA_PYTHON" -B "$here/grpc_relay.py" inner "$OCTAVIA_PYTHON" -Bu "$here/scenario.py"
rtk proxy jq -e '.passed' "$RESULT_DIR/receipt.json" >/dev/null
