#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf '%s\n' 'Usage: install.sh render CHART_DIR [--consumer-routes] [--service-import-parents]' \
        '       install.sh install CHART_DIR KUBECONFIG CONTEXT standard|experimental [--consumer-routes] [--service-import-parents]' >&2
    exit 2
}

mode=${1:-}
charts=${2:-}
[[ -n "$charts" ]] || usage
consumer_routes=false
service_import_parents=false
while (( $# )); do
    case ${!#} in
        --consumer-routes) consumer_routes=true ;;
        --service-import-parents) service_import_parents=true ;;
        *) break ;;
    esac
    set -- "${@:1:$#-1}"
done
case "$mode" in
    render) [[ $# -eq 2 ]] || usage ;;
    install)
        [[ $# -eq 5 && -n "$3" && -n "$4" ]] || usage
        case "$5" in standard|experimental) ;; *) usage ;; esac
        ;;
    *) usage ;;
esac

module=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
(cd -- "$charts" && sha256sum --check --strict --status "$module/charts.sha256") || {
    printf '%s\n' 'The Istio 1.31.0 chart archives do not match charts.sha256.' >&2
    exit 1
}

namespace=istio-system
if [[ "$mode" == install ]]; then
    kubeconfig=$3
    context=$4
    channel=$5
    kube() { kubectl --kubeconfig "$kubeconfig" --context "$context" "$@"; }
    kube get namespace "$namespace" -o name
    for kind in gatewayclasses gateways httproutes grpcroutes referencegrants; do
        installed=$(kube get crd "$kind.gateway.networking.k8s.io" \
            -o 'jsonpath={.metadata.annotations.gateway\.networking\.k8s\.io/bundle-version}{" "}{.metadata.annotations.gateway\.networking\.k8s\.io/channel}')
        if [[ "$installed" != "v1.7.0 $channel" ]]; then
            printf 'Expected Gateway API v1.7.0 %s for %s, got %s\n' "$channel" "$kind" "$installed" >&2
            exit 1
        fi
    done
fi

deploy_chart() {
    local release=$1 chart=$2
    shift 2
    if [[ "$mode" == render ]]; then
        helm template "$release" "$charts/$chart-1.31.0.tgz" --namespace "$namespace" "$@"
    else
        helm upgrade --install "$release" "$charts/$chart-1.31.0.tgz" \
            --kubeconfig "$kubeconfig" --kube-context "$context" --namespace "$namespace" \
            --atomic --wait --timeout 5m "$@"
    fi
}

deploy_chart hibiscus-ambient-base base --set global.enableReaderRBAC=false
pilot_values=(--values "$module/istiod.yaml")
if [[ "$service_import_parents" == true ]]; then
    pilot_values+=(--values "$module/service-import-parents.yaml")
elif [[ "$consumer_routes" == true ]]; then
    pilot_values+=(--values "$module/consumer-routes.yaml")
fi
deploy_chart hibiscus-ambient-istiod istiod "${pilot_values[@]}"
deploy_chart hibiscus-ambient-cni cni --values "$module/cni.yaml"
deploy_chart hibiscus-ambient-ztunnel ztunnel --values "$module/ztunnel.yaml"
