"""Run the two open gRPC cold cases on their respective Gateway API channels."""
from importlib.util import module_from_spec, spec_from_file_location
import os
import json
import http.client
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import re
import subprocess

spec = spec_from_file_location("shared_http_fixture", os.environ["SHARED_HTTPS_SCENARIO"])
shared = module_from_spec(spec)
spec.loader.exec_module(shared)
redact = shared.redact

shared.GATEWAYS = ("same-namespace", "all-namespaces")
shared.ECHO_DEPLOYMENTS = ("tls-backend", "grpc-infra-backend-v1", "grpc-infra-backend-v2", "grpc-infra-backend-v3")
by_channel = {
    "standard": ("GRPCRouteHeaderMatching", "grpcroute-header-matching", 11),
    "experimental": ("GRPCRouteListenerHostnameMatching", "grpcroute-listener-hostname-matching", 8),
}
case_name, stem, count = by_channel[os.environ["GATEWAY_API_CHANNEL"]]
CASES = {case_name: (stem, {"": count})}
shared.STEMS.update({name: stem for name, (stem, _) in CASES.items()})
CAPABILITY = "http_request_redirect_v1"


def outcome(name, expected_groups, log):
    root = "TestLocalGatewayCase/" + name
    statuses = re.findall(r"^\s*--- (PASS|FAIL|SKIP): (\S+) \(", log, re.M)
    events = [(status, path) for status, path in statuses if path == root or path.startswith(root + "/")]
    groups = {}
    for prefix, expected in expected_groups.items():
        group = root + "/" + (prefix + "/" if prefix else "")
        pattern = re.compile(re.escape(group) + r"(\d+)_request_to_")
        indices = {"PASS": [], "FAIL": [], "SKIP": []}
        for status, path in events:
            match = pattern.match(path)
            if match:
                indices[status].append(int(match[1]))
        groups[prefix or "requests"] = {
            "expected": expected,
            "passed": sorted(indices["PASS"]),
            "failed": sorted(indices["FAIL"]),
            "skipped": sorted(indices["SKIP"]),
        }
    complete = (
        ("PASS", root) in events
        and all(status == "PASS" for status, _ in events)
        and all(("PASS", root + "/" + prefix) in events for prefix in expected_groups if prefix)
        and all(row["passed"] == list(range(row["expected"])) for row in groups.values())
    )
    return complete, groups, [{"status": status, "test": path} for status, path in events]


def run_case(cloud, name, stem, expected_groups):
    report = cloud.directory / (stem + ".yaml")
    invocation = shared.test_command(
        "-test.v", "-test.run=^TestLocalGatewayCase$", "-test.timeout=8m", "-debug",
        "-run-test=" + name, "-gateway-class=" + shared.qualify.CLASS, "-cleanup-test-resources=false",
        "-conformance-profiles=GATEWAY-GRPC", "-mode=local-single-case", "-report-output=" + str(report),
        "-organization=Gateway API for OpenStack", "-project=Hibiscus",
        "-url=https://github.com/armeldemarsac92/gateway-api-openstack", "-version=" + os.environ["CONTROLLER_REVISION"],
        "-contact=https://github.com/armeldemarsac92/gateway-api-openstack/issues",
    )
    code, timed_out = None, False
    try:
        result = subprocess.run(invocation, capture_output=True, timeout=490)
        code = result.returncode
        output = (result.stdout or b"") + (result.stderr or b"")
    except subprocess.TimeoutExpired as error:
        timed_out = True
        output = (error.stdout or b"") + (error.stderr or b"")
    log = redact(cloud, output.decode(errors="replace"))
    log_name = stem + ".log"
    (shared.OUT / log_name).write_text(log)
    report_name = None
    if report.is_file():
        report_name = stem + "-report.yaml"
        (shared.OUT / report_name).write_text(redact(cloud, report.read_text()))

    complete, groups, events = outcome(name, expected_groups, log)
    inline_paths = ["/goodsection", "/badsection"] if name == "ListenerSetGatewayParentSectionNameNotFound" else []
    passed = code == 0 and complete and report_name is not None and all(path in log for path in inline_paths)
    return {
        "status": "passed" if passed else ("timeout" if timed_out else "failed"),
        "passed": passed,
        "exit": code,
        "timedOut": timed_out,
        "log": log_name,
        "upstreamReport": report_name,
        "expected": sum(expected_groups.values()),
        "subtestsPassed": sum(len(row["passed"]) for row in groups.values()),
        "groups": groups,
        "events": events,
        "requestLogCount": log.count("Sending Request:"),
        "inlineRequestPathsObserved": [path for path in inline_paths if path in log],
    }


def metrics(phase):
    with closing(http.client.HTTPConnection("127.0.0.1", 18080, timeout=5)) as connection:
        connection.request("GET", "/metrics")
        with connection.getresponse() as response:
            response_body = response.read().decode()
    prefixes = ("gateway_api_openstack_graph_writer_stage_duration_seconds_",
                "gateway_api_openstack_openstack_request_duration_seconds_",
                "controller_runtime_reconcile_total{")
    lines = [line for line in response_body.splitlines() if line.startswith(prefixes)]
    (shared.OUT / f"metrics-{phase}.prom").write_text("\n".join(lines) + "\n")


def now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def run_cases(cloud):
    sources = shared.SOURCE / "conformance/tests"
    cases = {
        name: {
            "status": "not-run", "passed": False, "expected": sum(expected_groups.values()),
            "caseGoSHA256": shared.digest(sources / (stem + ".go")),
            "manifestSHA256": shared.digest(sources / (stem + ".yaml")),
        }
        for name, (stem, expected_groups) in CASES.items()
    }
    receipt = {
        "passed": False,
        "channel": shared.qualify.CHANNEL,
        "gatewayAPIRevision": os.environ["GATEWAY_API_REVISION"],
        "controllerRevision": os.environ["CONTROLLER_REVISION"],
        "registeredCases": cases,
    }
    shared.save(receipt)
    try:
        receipt.update({
            "managerSHA256": shared.digest(os.environ["CONTROLLER_MANAGER"]),
            "octavia": shared.installed_wheel("octavia", "OCTAVIA_WHEEL"),
            "octaviaLib": shared.installed_wheel("octavia-lib", "OCTAVIA_LIB_WHEEL"),
            "HAProxy": {"version": cloud.haproxy_version, "sha256": shared.digest(cloud.haproxy)},
            "flags": ["--octavia-http-request-redirect", "--octavia-fixed-http-responses",
                      "--octavia-https-listener-isolation", "--octavia-backend-tls-policy",
                      "--openstack-resync-interval=10m", "--octavia-backend-h2c", "--octavia-http-header-exact"],
            "kubernetesAPIRelayTCPNoDelay": True,
            "infrastructure": "isolated kind with real upstream Pods and local Octavia REST/Amphora/HAProxy",
        })
        mode = os.environ["HIBISCUS_INITIAL_RULES_MODE"]
        assert mode in ("enabled", "disabled")
        receipt["controllerInitialRulesMode"] = mode
        if mode == "enabled":
            receipt["flags"].append("--octavia-l7policy-initial-rules")
        receipt["bundleSHA256"], receipt["baseSHA256"] = shared.bootstrap(cloud)
        mapping, _ = shared.front_map(cloud)
        endpoint = "/octavia/v2.0/lbaas/providers/amphora/gateway_capabilities"
        before = shared.qualify.cloud_api(cloud, endpoint, token=cloud.token)["gateway_capabilities"]
        receipt["amphoraCapabilitiesBeforeOptIn"] = before
        shared.qualify.require("l7policy_initial_rules_v1" not in before, "initial rules must default off")
        shared.qualify.require(CAPABILITY not in before, "Amphora redirect capability already advertised before opt-in")
        cloud.conf.config(group="api_settings", enabled_https_listener_isolation_providers=["amphora"],
                          enabled_http_request_redirect_providers=["amphora"],
                          enabled_l7policy_initial_rules_providers=["amphora"],
                          enabled_backend_h2c_providers=["amphora"], enabled_http_header_exact_providers=["amphora"])
        after = shared.qualify.cloud_api(cloud, endpoint, token=cloud.token)["gateway_capabilities"]
        receipt["amphoraCapabilitiesAfterOptIn"] = after
        shared.qualify.require(all(cap in after for cap in (CAPABILITY, "l7policy_initial_rules_v1", "backend_h2c_v1", "http_header_exact_v1")), "Amphora redirect capability missing after opt-in")
        receipt["echo"] = shared.tls_echo()
        with shared.manager(cloud, mapping) as child:
            shared.qualify.spin("GatewayClass accepted", lambda: child.poll() is None and
                shared.qualify.conditions(shared.qualify.get("gatewayclass", shared.qualify.CLASS)).get("Accepted") == "True/Accepted", duration=40)
            shared.wait_base_infrastructure(cloud, "base-readiness.log")
            for name, (stem, expected_groups) in CASES.items():
                print(f"{shared.qualify.CHANNEL} {name}", flush=True)
                result = cases[name]
                result["status"] = "running"
                shared.save(receipt)
                metrics("before")
                result["caseStartedAt"] = now()
                result.update(run_case(cloud, name, stem, expected_groups))
                result["caseFinishedAt"] = now()
                metrics("after")
                shared.save(receipt)
                result["diagnosticsStartedAt"] = now()
                try:
                    shared.diagnostics(cloud, result)
                except Exception as error:
                    result["diagnosticsError"] = redact(cloud, str(error))
                result["diagnosticsFinishedAt"] = now()
                shared.save(receipt)
                if name != next(reversed(CASES)):
                    shared.remove_case_objects(name)
                    shared.wait_base_infrastructure(cloud, stem + "-cleanup-readiness.log")
        receipt["nativeL7WorkerDispatch"] = dict(cloud.native_l7_worker_calls)
        receipt["passed"] = all(result["passed"] and "diagnosticsError" not in result for result in cases.values())
    except Exception as error:
        receipt["failure"] = redact(cloud, str(error))
        for result in cases.values():
            if result["status"] == "running":
                result["status"] = "interrupted"
        raise
    finally:
        for key, rows in (("native-mutations", cloud.native_mutations),
                          ("native-reloads", cloud.native_reloads),
                          ("native-rest-timing", getattr(cloud, "native_rest_timing", []))):
            (shared.OUT / f"{key}.json").write_text(json.dumps(rows, indent=2) + "\n")
        shared.save(receipt)
    return receipt


if __name__ == "__main__":
    shared.main()
