#!/usr/bin/env bash
set -euo pipefail
umask 077

# Run as root inside a disposable guest with rootful Podman and Keepalived.
# The caller supplies the stock Amphora renderer output on stdin for each apply.
namespace=amphora-haproxy
director=10.89.0.250
client=172.31.248.2
state=/run/hibiscus-udp-upstream

die() { printf '%s\n' "$*" >&2; exit 1; }

setup() {
    local cluster=$1 bridge node address cidr destination
    [[ "$cluster" == hibiscus-udp-standard || "$cluster" == hibiscus-udp-experimental ]] || die "Unknown test cluster $cluster"
    bridge=$(podman network inspect kind --format '{{.NetworkInterface}}')
    modprobe ip_vs_wrr
    modprobe veth
    if ! ip -n "$namespace" link show back0 >/dev/null 2>&1; then
        ip netns add "$namespace"
        ip -n "$namespace" link set lo up
        ip link add hib-udp-client type veth peer name front0 netns "$namespace"
        ip link add hib-udp-bridge type veth peer name back0 netns "$namespace"
        ip link set hib-udp-bridge master "$bridge" up
        ip addr add "$client/30" dev hib-udp-client
        ip link set hib-udp-client up
        ip -n "$namespace" addr add 172.31.248.1/30 dev front0
        ip -n "$namespace" addr add "$director/24" dev back0
        ip -n "$namespace" link set front0 up
        ip -n "$namespace" link set back0 up
        ip netns exec "$namespace" sysctl -qw net.ipv4.ip_forward=1
    fi
    ip route replace 192.0.2.0/24 via 172.31.248.1 dev hib-udp-client src "$client"
    for node in "$cluster-control-plane" "$cluster-worker"; do
        address=$(podman inspect "$node" --format '{{(index .NetworkSettings.Networks "kind").IPAddress}}')
        cidr=$(podman exec "$cluster-control-plane" kubectl --kubeconfig /etc/kubernetes/admin.conf get node "$node" -o jsonpath='{.spec.podCIDR}')
        [[ -n "$address" && -n "$cidr" ]] || die "Missing address or Pod CIDR for $node"
        ip -n "$namespace" route replace "$cidr" via "$address" dev back0
        podman exec "$node" ip route replace "$client/32" via "$director"
        # PodIP members must return with their source PodIP for IPVS reverse NAT.
        for destination in "$client/32" "$director/32"; do
            if ! podman exec "$node" iptables -w -t nat -C POSTROUTING -s "$cidr" -d "$destination" -j RETURN 2>/dev/null; then
                podman exec "$node" iptables -w -t nat -I POSTROUTING -s "$cidr" -d "$destination" -j RETURN
            fi
        done
        printf '%s %s %s\n' "$node" "$address" "$cidr"
    done
}

virtual_targets() {
    awk '$1 == "virtual_server_group" { in_group = 1; next }
        in_group && $1 == "}" { in_group = 0 }
        in_group && NF >= 2 { print $1 ":" $2 }' "$1"
}

wait_for_targets() {
    local want=$1 targets=$2 table target matched attempt
    [[ -n "$targets" ]] || return 0
    for ((attempt=0; attempt<50; attempt++)); do
        table=$(ip netns exec "$namespace" ipvsadm -Sn | awk '$1 == "-A" && $2 == "-u" { print $3 }')
        matched=yes
        while IFS= read -r target; do
            if [[ "$want" == present ]]; then
                grep -Fxq "$target" <<< "$table" || matched=no
            else
                grep -Fxq "$target" <<< "$table" && matched=no
            fi
        done <<< "$targets"
        [[ "$matched" == yes ]] && return 0
        sleep 0.1
    done
    die "UDP virtual servers should be $want: $targets"
}

stop_listener() {
    local pid targets attempt
    if [[ -s "$listener/keepalived.pid" ]]; then
        pid=$(<"$listener/keepalived.pid")
        if [[ -r "/proc/$pid/cmdline" ]] && grep -Fxzq "$listener/keepalived.conf" "/proc/$pid/cmdline"; then
            kill -TERM "$pid"
            for ((attempt=0; attempt<50; attempt++)); do
                [[ ! -s "$listener/keepalived.pid" ]] && break
                sleep 0.1
            done
            [[ ! -s "$listener/keepalived.pid" ]] || die "Keepalived did not stop: $listener"
        fi
    fi
    if [[ -f "$listener/keepalived.conf" ]]; then
        targets=$(virtual_targets "$listener/keepalived.conf")
        wait_for_targets absent "$targets"
    fi
}

apply_listener() {
    local targets target attempt incoming="$listener/incoming.conf"
    mkdir -p "$listener"
    cat > "$incoming"
    [[ -s "$incoming" ]] || die "Expected an Amphora configuration on stdin"
    targets=$(virtual_targets "$incoming")
    while IFS= read -r target; do
        [[ -z "$target" || "$target" == "$vip:"* ]] || die "Configuration contains a different VIP: $target"
    done <<< "$targets"
    if [[ -f "$listener/vip" ]]; then
        [[ $(<"$listener/vip") == "$vip" ]] || die "Listener VIP changed: $listener"
    fi
    stop_listener
    ip -n "$namespace" addr replace "$vip/32" dev front0
    printf '%s\n' "$vip" > "$listener/vip"
    mv "$incoming" "$listener/keepalived.conf"
    keepalived --check --log-detail \
        --pid "$listener/keepalived.pid" --checkers_pid "$listener/checkers.pid" \
        --use-file "$listener/keepalived.conf" 9>&-
    for ((attempt=0; attempt<50; attempt++)); do
        [[ -s "$listener/keepalived.pid" ]] && break
        sleep 0.1
    done
    [[ -s "$listener/keepalived.pid" ]] || die "Keepalived did not start: $listener"
    wait_for_targets present "$targets"
    sha256sum "$listener/keepalived.conf"
}

delete_listener() {
    local address
    [[ -f "$listener/vip" ]] || return 0
    [[ $(<"$listener/vip") == "$vip" ]] || die "Listener VIP changed: $listener"
    stop_listener
    rm -rf -- "$listener"
    for address in "$state"/*/*/vip; do
        if [[ -f "$address" && $(<"$address") == "$vip" ]]; then
            return 0
        fi
    done
    ip -n "$namespace" addr del "$vip/32" dev front0
}

[[ $EUID == 0 ]] || die "Run inside the disposable guest as root"
mkdir -p "$state"
exec 9> "$state/lock"
flock 9
if [[ $# == 2 && "$1" == setup ]]; then
    setup "$2"
    exit
fi
[[ $# == 4 ]] || die "Usage: $0 setup CLUSTER | {apply|delete} LB_UUID LISTENER_UUID VIP"
uuid='^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$'
[[ "$2" =~ $uuid && "$3" =~ $uuid ]] || die "Expected Octavia load balancer and listener UUIDs"
vip=$4
[[ "$vip" =~ ^192\.0\.2\.([1-9][0-9]{0,2})$ && ${BASH_REMATCH[1]} -lt 255 ]] || die "VIP is outside the guest test network"
listener="$state/$2/$3"
case "$1" in
    apply) apply_listener ;;
    delete) delete_listener ;;
    *) die "Unknown operation $1" ;;
esac
