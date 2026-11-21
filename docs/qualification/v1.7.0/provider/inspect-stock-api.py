#!/usr/bin/env python3
"""Print the public request surface from pinned Octavia source, without importing its server."""

import ast
from pathlib import Path
import subprocess
import sys


def tree(path):
    return ast.parse(path.read_text())


def require_revision(project, path, revision):
    head = subprocess.check_output(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        text=True,
    ).strip()
    if head != revision:
        raise SystemExit(f"{path}: expected {revision}, got {head}")
    print(f"{project}: {head}")


def assignments(node):
    for statement in node.body:
        if isinstance(statement, ast.Assign) and isinstance(statement.targets[0], ast.Name):
            yield statement.targets[0].id, statement.value


def fields(node, class_name):
    class_node = next(
        statement for statement in node.body
        if isinstance(statement, ast.ClassDef) and statement.name == class_name
    )
    return dict(assignments(class_node))


def evaluate(node, names):
    if isinstance(node, ast.Name):
        return names[node.id]
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        return names[f"{node.value.id}.{node.attr}"]
    if isinstance(node, (ast.Tuple, ast.List)):
        return [evaluate(item, names) for item in node.elts]
    return ast.literal_eval(node)


def constant_scope(node, inherited=None):
    names = dict(inherited or {})
    for name, expression in assignments(node):
        try:
            names[name] = evaluate(expression, names)
        except (KeyError, ValueError):
            continue
    return names


def show(scope, name):
    print(f"{name} = {scope[name]}")


octavia, lib, ovn = map(Path, sys.argv[1:])
require_revision("octavia", octavia, "93689cb7701da65b5aad387bb824c477c64b0f2b")
require_revision("octavia-lib", lib, "bc03e1856b60517e1b75db4fdf9b3edf55f3a019")
require_revision("ovn-octavia-provider", ovn, "485b18e661d368feb82f87c499777052ee749458")
lib_values = constant_scope(tree(lib / "octavia_lib/common/constants.py"))
octavia_values = constant_scope(
    tree(octavia / "octavia/common/constants.py"),
    {f"lib_consts.{key}": value for key, value in lib_values.items()},
)
ovn_values = constant_scope(
    tree(ovn / "ovn_octavia_provider/common/constants.py"),
    {f"constants.{key}": value for key, value in lib_values.items()},
)

for key in (
    "SUPPORTED_L7POLICY_ACTIONS", "SUPPORTED_L7RULE_TYPES",
    "SUPPORTED_L7RULE_COMPARE_TYPES", "SUPPORTED_L7POLICY_REDIRECT_HTTP_CODES",
    "SUPPORTED_HTTP_HEADERS", "SUPPORTED_SSL_HEADERS", "SUPPORTED_ALPN_PROTOCOLS",
    "MIN_WEIGHT", "MAX_WEIGHT",
):
    show(octavia_values, key)

for key in ("OVN_NATIVE_LB_PROTOCOLS", "OVN_NATIVE_LB_ALGORITHMS"):
    show(ovn_values, key)

types_path = octavia / "octavia/api/v2/types"
for file, class_names, printed_fields in (
    ("listener", ("ListenerPOST", "ListenerPUT", "ListenerSingleCreate"),
     ("protocol", "insert_headers", "timeout_tcp_inspect", "alpn_protocols")),
    ("pool", ("PoolPOST", "PoolPUT", "PoolSingleCreate"),
     ("protocol", "tls_enabled", "alpn_protocols")),
    ("member", ("MemberPOST", "MemberPUT", "MemberSingleCreate"), ("weight",)),
    ("l7policy", ("L7PolicyPOST", "L7PolicyPUT", "L7PolicySingleCreate"),
     ("action", "redirect_pool_id", "redirect_http_code")),
    ("l7rule", ("L7RulePOST", "L7RulePUT", "L7RuleSingleCreate"),
     ("type", "compare_type", "key", "value")),
):
    module = tree(types_path / (file + ".py"))
    for class_name in class_names:
        attrs = fields(module, class_name)
        print(f"{class_name}: {', '.join(attrs)}")
        for key in printed_fields:
            if key in attrs:
                print(f"  {key} = {ast.unparse(attrs[key])}")

amp_driver = tree(octavia / "octavia/api/drivers/amphora_driver/v2/driver.py")
amp_values = constant_scope(
    amp_driver, {f"lib_consts.{key}": value for key, value in lib_values.items()},
)
show(amp_values, "VALID_L7POLICY_LISTENER_PROTOCOLS")

root = tree(octavia / "octavia/api/root_controller.py")
for node in ast.walk(root):
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr == "_add_a_version" and evaluate(node.args[3], {}) == "CURRENT":
            print(f"Octavia discovery CURRENT: {evaluate(node.args[1], {})}")
