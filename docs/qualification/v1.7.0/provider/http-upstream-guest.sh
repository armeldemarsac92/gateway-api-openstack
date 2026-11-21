#!/usr/bin/env bash
# Stand in for Amphora's agent inside the disposable qualification VM.
set -euo pipefail
umask 077

[[ $# == 3 && $1 =~ ^(apply|delete)$ ]] || exit 2
operation=$1
lb=$2
vip=$3
[[ $lb =~ ^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$ ]]
[[ $vip =~ ^192\.0\.2\.([1-9][0-9]{1,2})$ && ${BASH_REMATCH[1]} -ge 30 && ${BASH_REMATCH[1]} -le 254 ]]
base=/run/hibiscus-http-upstream
dir=$base/$lb
haproxy=/usr/local/libexec/hibiscus-qualification/haproxy
mkdir -p "$base"
exec 9>"$base/lock"
flock -x 9

vip_exists() {
    ip -4 -o address show dev lo | awk -v address="$vip/32" '$4 == address { found=1 } END { exit !found }'
}

stop_proxy() {
    [[ -f $dir/pid ]] || return 0
    local pid
    read -r pid < "$dir/pid"
    [[ $pid =~ ^[1-9][0-9]*$ ]]
    if [[ -e /proc/$pid/exe ]]; then
        [[ $(readlink "/proc/$pid/exe") == "$haproxy" ]]
        grep -Fxzq -- "$dir/haproxy.cfg" "/proc/$pid/cmdline"
        kill -TERM "$pid"
        for _ in {1..100}; do
            [[ ! -e /proc/$pid/exe ]] && break
            sleep 0.05
        done
        [[ ! -e /proc/$pid/exe ]]
    fi
    rm -f "$dir/pid" "$base/$lb.sock"
}

if [[ -f $dir/vip ]]; then
    [[ $(<"$dir/vip") == "$vip" ]]
elif [[ $operation == delete ]]; then
    exit 0
else
    ! vip_exists || { echo "VIP already assigned" >&2; exit 1; }
fi

if [[ $operation == delete ]]; then
    stop_proxy
    if vip_exists; then ip address del "$vip/32" dev lo; fi
    rm -f "$dir/haproxy.cfg" "$dir/next.cfg" "$dir/vip"
    rmdir "$dir"
    echo "Removed $lb $vip"
    exit 0
fi

mkdir -p "$dir"
case "$("$haproxy" -v)" in
    "HAProxy version 3.0.19-"*|"HAProxy version 3.0.19 "*) ;;
    *) echo "Expected HAProxy 3.0.19 for the Octavia worker" >&2; exit 1 ;;
esac
cat > "$dir/next.cfg"
grep -Fxq -- "# Configuration for loadbalancer $lb" "$dir/next.cfg"
awk -v vip="$vip" '$1 == "bind" { n++; split($2, part, ":"); if (part[1] != vip || part[2] !~ /^[0-9]+$/) exit 1 } END { if (!n) exit 1 }' "$dir/next.cfg"
"$haproxy" -c -f "$dir/next.cfg" >/dev/null
printf '%s\n' "$vip" > "$dir/vip"
if ! vip_exists; then ip address add "$vip/32" dev lo; fi
stop_proxy
mv "$dir/next.cfg" "$dir/haproxy.cfg"
"$haproxy" -D -p "$dir/pid" -f "$dir/haproxy.cfg" 9>&-
for _ in {1..100}; do
    [[ -s $dir/pid && -S $base/$lb.sock ]] && break
    sleep 0.05
done
[[ -s $dir/pid && -S $base/$lb.sock && -e /proc/$(<"$dir/pid")/exe ]]
sha256sum "$dir/haproxy.cfg"
