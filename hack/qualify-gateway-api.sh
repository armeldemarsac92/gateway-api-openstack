#!/usr/bin/env bash
set -euo pipefail
umask 077

channel=${1:-}
revision=${2:-}
source_dir=${3:-}
gateway_profiles=GATEWAY-HTTP,GATEWAY-TLS,GATEWAY-TCP,GATEWAY-UDP,GATEWAY-GRPC
mesh_profiles=MESH-HTTP,MESH-GRPC
suite_revision=e26d183dbbc1bb782b78addbd26faba03955c152
experimental=ClusterTrustBundle,GatewayAddressRoutability,GatewayAddressRoutabilityCluster,GatewayListenerFilters,HTTPRouteDestinationPortMatching
case "$channel" in
    standard) bundle_sha=4e2602c4cbe1afbc394d86f4ac6522e1a5263b01133bba1e40a6d1a549e753a1 ;;
    experimental) bundle_sha=e6d2d3407717f54b579b22ad4d164b6863895b3524f21250b0da0c1ff9a05d53 ;;
    *) echo 'Use qualify-gateway-api.sh standard|experimental CONTROLLER_SHA UPSTREAM_CHECKOUT' >&2; exit 2 ;;
esac

stage='runner configuration'
run_dir=''
exec 3>&1
# Called by the EXIT trap.
# shellcheck disable=SC2329
finish() {
    local status=$1 profiles profile core extended recorded core_status extended_status
    profiles=$gateway_profiles,$mesh_profiles
    {
        printf '### Gateway API v1.7.0 %s\n\n' "$channel"
        printf 'Upstream suite %s.\n\n' "$suite_revision"
        if [[ $revision =~ ^[a-f0-9]{40}$ ]]; then
            printf 'Requested controller revision %s.\n\n' "$revision"
        fi
        if ((status != 0)); then
            printf 'Qualification did not complete successfully (%s).\n\n' "$stage"
        fi
        if [[ -n $run_dir ]]; then
            printf 'Private runner evidence %s.\n\n' "${run_dir##*/}"
        fi
        printf 'Profile | Core | Extended\n--- | --- | ---\n'
        while IFS= read -r profile; do
            core=UNRUN extended=UNRUN
            if [[ -n $run_dir && -f $run_dir/profile-status.tsv ]]; then
                while IFS=$'\t' read -r recorded core_status extended_status; do
                    if [[ $recorded == "$profile" ]]; then
                        core=$core_status extended=$extended_status
                    fi
                done <"$run_dir/profile-status.tsv"
            fi
            printf '%s | %s | %s\n' "$profile" "$core" "$extended"
        done < <(tr ',' '\n' <<<"$profiles")
    } | tee -a -- "${GITHUB_STEP_SUMMARY:-/dev/null}" >&3
}
trap 'finish "$?"' EXIT

[[ $revision =~ ^[a-f0-9]{40}$ ]]
[[ ${GATEWAY_QUALIFICATION_ROOT:-} == /* ]]
root=$(realpath -e -- "$GATEWAY_QUALIFICATION_ROOT" 2>/dev/null)
repo=$(realpath -e -- "$(dirname -- "$0")/..")
workspace=$(realpath -m -- "${GITHUB_WORKSPACE:-$repo}")
[[ -d $root && $root != "$workspace" && $root != "$workspace/"* ]]
run_dir=$(mktemp -d "$root/$channel.XXXXXXXX" 2>/dev/null)
exec >"$run_dir/runner.log" 2>&1

# This command handles pinned release structure and trusts only fixed outcome values in the public summary.
evidence() {
    python3 -E - "$@" <<'PY'
import json
import re
import sys
from pathlib import Path

import yaml

action, channel, revision, source, experimental, *mode_args = sys.argv[1:]
experimental = set(experimental.split(','))

def json_file(name):
    return json.loads(Path(name).read_text())

def yaml_file(name):
    return yaml.safe_load(Path(name).read_text())

def case_status(mode, results):
    cases = json_file('cases.json')[mode]
    with Path(f'{mode}-cases.tsv').open('w') as output:
        print('Case\tStatus', file=output)
        for name, experimental_only in sorted(cases.items()):
            status = results.get(name, 'UNRUN')
            if experimental_only and channel == 'standard':
                assert status in ('SKIP', 'UNRUN'), f'Experimental case unexpectedly executed: {name}'
                status = 'UNRUN_CHANNEL'
            print(f'{name}\t{status}', file=output)

def spec(object):
    spec = object['spec']
    # Kubernetes fills these optional values when persisting the released resources.
    if object['kind'] == 'CustomResourceDefinition':
        spec.setdefault('conversion', {'strategy': 'None'})
        spec.setdefault('preserveUnknownFields', False)
    else:
        match = spec.get('matchConstraints', spec.get('matchResources'))
        match.setdefault('matchPolicy', 'Equivalent')
        match.setdefault('namespaceSelector', {})
        match.setdefault('objectSelector', {})
        for rule in match['resourceRules']:
            rule.setdefault('scope', '*')
    return spec

if action == 'plan':
    cases = {'gateway': {}, 'mesh': {}}
    pattern = r'ShortName:\s*"([^"]+)".*?Features:\s*\[\]features.FeatureName\{([^}]*)\}'
    for file in Path(source, 'conformance/tests').rglob('*.go'):
        for name, required in re.findall(pattern, file.read_text(), re.S):
            features = set(re.findall(r'features.Support(\w+)', required))
            assert ('Gateway' in features) != ('Mesh' in features), name
            mode = 'mesh' if 'Mesh' in features else 'gateway'
            assert name not in cases[mode], name
            cases[mode][name] = bool(features & experimental)
    assert (len(cases['gateway']), len(cases['mesh'])) == (147, 23)
    Path('cases.json').write_text(json.dumps(cases))
    for mode in cases:
        case_status(mode, {})
elif action == 'preflight':
    inventory = yaml_file('inventory.yaml')
    assert inventory['channel'] == channel, 'incorrect channel in target inventory'
    assert inventory['controllerRevision'] == revision, 'deployed source revision differs from requested revision'
    assert re.fullmatch(r'[^\s@]+@sha256:[a-f0-9]{64}', inventory['controllerImage']), 'image must be digest-pinned'
    assert inventory['environment'], 'deployment environment inventory is missing'
    namespaces = {entry['metadata']['name']: entry for entry in json_file('namespaces.json')['items']}
    assert namespaces['kube-system']['metadata']['uid'] == inventory['clusterUID'], 'unexpected target cluster UID'
    assert not any(n.startswith('gateway-conformance-') for n in namespaces), 'existing conformance namespaces'

    version_key = 'gateway.networking.k8s.io/bundle-version'
    channel_key = 'gateway.networking.k8s.io/channel'
    groups = {'gateway.networking.k8s.io', 'gateway.networking.x-k8s.io'}
    expected = list(yaml.safe_load_all(Path('bundle.yaml').read_text()))
    definitions = json_file('crds.json')
    assert definitions['apiVersion'] == 'apiextensions.k8s.io/v1' and definitions['kind'] == 'CustomResourceDefinitionList'
    observed = {obj['metadata']['name']: obj for obj in definitions['items']
                if obj['spec']['group'] in groups or version_key in obj['metadata'].get('annotations', {})
                or channel_key in obj['metadata'].get('annotations', {})}
    assert observed.keys() == {obj['metadata']['name'] for obj in expected if obj['kind'] == 'CustomResourceDefinition'}, 'CRD inventory differs from the release'
    for released in expected:
        name, kind = released['metadata']['name'], released['kind']
        if kind == 'CustomResourceDefinition':
            live = observed[name]
            # The typed list carries its items' GVK in the list envelope on the wire.
            live.setdefault('kind', 'CustomResourceDefinition')
            live.setdefault('apiVersion', definitions['apiVersion'])
            assert any(c['type'] == 'Established' and c['status'] == 'True' for c in live['status']['conditions']), name
        else:
            live = json_file(f'{kind}.json')
        for key in (version_key, channel_key):
            assert live['metadata'].get('annotations', {}).get(key) == released['metadata']['annotations'][key], (name, key)
        assert not live['metadata'].get('deletionTimestamp'), (kind, name, 'terminating')
        assert live['apiVersion'] == released['apiVersion'] and live['kind'] == kind and live['metadata']['name'] == name, (kind, name)
        assert spec(live) == spec(released), (kind, name, 'spec differs from the release')
else:
    mode, profiles = mode_args
    outcomes = {'pass': 'PASS', 'fail': 'FAIL', 'skip': 'SKIP'}
    results = {}
    with Path(f'{mode}.jsonl').open() as events:
        for line in events:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            match = re.fullmatch(r'TestConformance/([^/]+)', event.get('Test', ''))
            if match and event.get('Action') in outcomes:
                results[match[1]] = outcomes[event['Action']]
            elif match and event.get('Action') == 'run':
                results.setdefault(match[1], 'INCOMPLETE')
    case_status(mode, results)
    report = yaml_file(f'{mode}-report.yaml')
    assert report['gatewayAPIVersion'] == 'v1.7.0' and report['gatewayAPIChannel'] == channel
    by_name = {p['name']: p for p in report.get('profiles') or []}
    def result(profile, key):
        value = (profile.get(key) or {}).get('result')
        return value if value in ('success', 'partial', 'failure') else 'UNRUN'
    with Path('profile-status.tsv').open('a') as output:
        for name in profiles.split(','):
            profile = by_name.get(name, {})
            print(name, result(profile, 'core'), result(profile, 'extended'), sep='\t', file=output)
    assert all(name in by_name for name in profiles.split(',')), 'setup or execution left profile results absent'
PY
}

stage='pinned source verification'
source_dir=$(realpath -e -- "$source_dir")
[[ $(git -C "$source_dir" rev-parse HEAD) == "$suite_revision" ]]
# Go can generate this checksum file even in readonly module mode.
source_changes=$(git -C "$source_dir" status --porcelain --untracked-files=all --ignored -- . ':(exclude)go.work.sum')
[[ -z $source_changes ]]
cd -- "$run_dir"
printf 'controllerRevision=%s\nsuiteRevision=%s\nchannel=%s\n' "$revision" "$suite_revision" "$channel" >run.properties
evidence plan "$channel" "$revision" "$source_dir" "$experimental"
channel_exempt=''
if [[ $channel == standard ]]; then
    channel_exempt=,$experimental
fi
printf 'profiles=%s\nallFeatures=true\nexemptFeatures=%s\n' "$gateway_profiles" "Mesh$channel_exempt" >gateway.selection
printf 'profiles=%s\nallFeatures=true\nexemptFeatures=%s\n' "$mesh_profiles" "Gateway$channel_exempt" >mesh.selection

stage='runner configuration'
for name in kubeconfig inventory.yaml options.yaml; do
    [[ -f $root/$name && $(realpath -e -- "$root/$name") == "$root/"* ]]
done
export KUBECONFIG="$root/kubeconfig"
cp -- "$root/inventory.yaml" "$root/options.yaml" .

stage='pinned release manifest verification'
curl -fsSL -o bundle.yaml "https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.7.0/$channel-install.yaml"
printf '%s  bundle.yaml\n' "$bundle_sha" | sha256sum --check --status

stage='cluster preflight'
kube_read() {
    kubectl --request-timeout=30s get --raw "$1" >"$2"
}
kube_read /version kubernetes-version.json
kube_read /api/v1/namespaces namespaces.json
kube_read /apis/apiextensions.k8s.io/v1/customresourcedefinitions crds.json
admission=/apis/admissionregistration.k8s.io/v1
policy=safe-upgrades.gateway.networking.k8s.io
kube_read "$admission/validatingadmissionpolicies/$policy" ValidatingAdmissionPolicy.json
kube_read "$admission/validatingadmissionpolicybindings/$policy" ValidatingAdmissionPolicyBinding.json
evidence preflight "$channel" "$revision" "$source_dir" "$experimental"

run_mode() {
    local mode=$1 profiles=$2 exempt=$3 status=0
    (
        cd -- "$source_dir" || exit 1
        GOFLAGS='' GOTOOLCHAIN=local GOWORK="$source_dir/go.work" go test -mod=readonly -json -count=1 \
            -run '^TestConformance$' -timeout=150m ./conformance -args \
            --conformance-options-file="$run_dir/options.yaml" --version="$revision" \
            --conformance-profiles="$profiles" --all-features=true --exempt-features="$exempt$channel_exempt" \
            --skip-tests= --run-test= --skip-provisional-tests=false --fail-fast=false \
            --allow-crds-mismatch=false --cleanup-base-resources=true --cleanup-test-resources=true \
            --report-output="$run_dir/$mode-report.yaml"
    ) >"$mode.jsonl" 2>"$mode.stderr" || status=$?
    printf '%s\n' "$status" >"$mode.exit"
    evidence report "$channel" "$revision" "$source_dir" "$experimental" "$mode" "$profiles" || return 1
    return "$status"
}

stage='upstream Gateway or mesh suite'
failed=0
run_mode gateway "$gateway_profiles" Mesh || failed=1
run_mode mesh "$mesh_profiles" Gateway || failed=1
exit "$failed"
