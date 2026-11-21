#!/usr/bin/env python3
"""Probe ordinary exact HTTP header predicates with stock Octavia Amphora."""

import argparse
from contextlib import chdir, closing
import http.client
from http.server import BaseHTTPRequestHandler, HTTPServer
from importlib import import_module
from importlib.metadata import version
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import unittest
from unittest import mock
from uuid import uuid4

from octavia.amphorae.drivers.haproxy import rest_api_driver
from octavia.common import data_models


stock_tcp = import_module("stock-tcp")
LOOPBACK = stock_tcp.LOOPBACK
LONG_NAME = "x" * 255
LONG_VALUE = "v" * 255


class Echo(BaseHTTPRequestHandler):
    def do_GET(self):
        body = f"{self.server.backend} {self.path}".encode("ascii")
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


class StockHTTPHeaders(stock_tcp.StockAmphoraAPI):
    def create_backend(self, lb_id, name):
        server = self.enterContext(HTTPServer((LOOPBACK, 0), Echo))
        server.backend = name
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, timeout=3)
        self.addCleanup(server.shutdown)
        pool = self.create_pool(lb_id, "HTTP", "ROUND_ROBIN")["pool"]
        self.set_lb_status(lb_id)
        self.create_member(pool["id"], LOOPBACK, server.server_port)
        self.set_lb_status(lb_id)
        return pool["id"]

    def create_forwarding_policy(self, lb_id, listener_id, pool_id, position, headers):
        policy = self.create_l7policy(
            listener_id, "REDIRECT_TO_POOL", position=position, redirect_pool_id=pool_id,
        )["l7policy"]
        self.set_lb_status(lb_id)
        for name, value in headers:
            rule = self.create_l7rule(policy["id"], "HEADER", "EQUAL_TO", value, key=name)["rule"]
            self.set_lb_status(lb_id)
            path = self.L7RULE_PATH.format(l7policy_id=policy["id"], l7rule_id=rule["id"])
            stored = self.get(path).json["rule"]
            self.assertEqual(("HEADER", "EQUAL_TO", name, value, False), (
                stored["type"], stored["compare_type"], stored["key"], stored["value"], stored["invert"],
            ))
        return policy["id"]

    def render_worker_config(self, lb_id, config):
        driver = rest_api_driver.HaproxyAmphoraLoadBalancerDriver()
        agent = mock.Mock()
        agent.get_api_version.return_value = {"api_version": "1.0"}
        agent.get_info.return_value = {"haproxy_version": "3.0.19"}
        agent.get_details.return_value = {}
        driver.clients.update({"base": agent, "1.0": agent})
        amphora = data_models.Amphora(id=str(uuid4()))
        with self.get_session() as session:
            lb = self.lb_repo.get(session, id=lb_id)
        driver.update_amphora_listeners(lb, amphora)
        agent.upload_config.assert_called_once()
        target, object_id, rendered = agent.upload_config.call_args.args
        self.assertEqual((amphora, lb_id), (target, object_id))
        agent.reload_listener.assert_called_once_with(amphora, lb_id, timeout_dict=None)
        config.write_bytes(rendered.encode("utf-8"))

    def test_exact_header_routing(self):
        directory = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="oh-")))
        # Keep the stock stats socket path short even inside the fixture's temp tree.
        self.conf.config(group="haproxy_amphora", base_path="unix@.", connection_logging=False)
        with socket.socket() as reserve:
            reserve.bind((LOOPBACK, 0))
            port = reserve.getsockname()[1]

        lb_id = self.create_loopback_lb()
        pool_a = self.create_backend(lb_id, "A")
        pool_b = self.create_backend(lb_id, "B")
        fallback = self.create_backend(lb_id, "C")
        listener = self.create_listener("HTTP", port, lb_id, default_pool_id=fallback)["listener"]
        listener_id = listener["id"]
        self.set_lb_status(lb_id)
        self.assertEqual(fallback, self.get_listener(listener_id)["listener"]["default_pool_id"])

        self.create_forwarding_policy(lb_id, listener_id, pool_a, 1, (("version", "one"),))
        generic = self.create_forwarding_policy(lb_id, listener_id, pool_b, 2, (("version", "two"),))
        specific = self.create_forwarding_policy(
            lb_id, listener_id, pool_a, 1, (("version", "two"), ("color", "orange")),
        )
        boundary = self.create_forwarding_policy(lb_id, listener_id, pool_a, 4, ((LONG_NAME, LONG_VALUE),))
        for policy_id, pool_id, position in ((specific, pool_a, 1), (generic, pool_b, 3)):
            path = self.L7POLICY_PATH.format(l7policy_id=policy_id)
            policy = self.get(path).json["l7policy"]
            self.assertEqual(("REDIRECT_TO_POOL", pool_id, position), (
                policy["action"], policy["redirect_pool_id"], policy["position"],
            ))

        for label, name, value in (
            ("256-character name", LONG_NAME + "x", "one"),
            ("256-character value", "version", LONG_VALUE + "v"),
            ("unquoted interior space", "version", "blue sky"),
        ):
            with self.subTest(api_rejection=label):
                self.create_l7rule(boundary, "HEADER", "EQUAL_TO", value, status=400, key=name)
                print(f"{label}: API HTTP 400")

        config = directory / "haproxy.cfg"
        self.render_worker_config(lb_id, config)
        targets = [
            line.split()[1] for line in config.read_text(encoding="utf-8").splitlines()
            if line.strip().startswith("use_backend ")
        ]
        self.assertEqual([f"{pool_id}:{listener_id}" for pool_id in (pool_a, pool_a, pool_b, pool_a)], targets)
        with chdir(directory), self.run_haproxy(config, directory / f"{lb_id}.sock"):
            for label, headers, expected in (
                ("exact value", (("Version", "one"),), "A"),
                ("no header", (), "C"),
                ("mixed-case name", (("vErSiOn", "one"),), "A"),
                ("mixed-case value", (("Version", "One"),), "C"),
                ("version two alone", (("Version", "two"),), "B"),
                ("two matching predicates", (("Version", "two"), ("Color", "orange")), "A"),
                ("second predicate differs", (("Version", "two"), ("Color", "blue")), "B"),
                ("first predicate absent", (("Color", "orange"),), "C"),
                ("repeated request field", (("Version", "one"), ("Version", "other")), "A"),
                ("comma-separated field value", (("Version", "one, other"),), "A"),
                ("255-character name and value", ((LONG_NAME, LONG_VALUE),), "A"),
            ):
                connection = http.client.HTTPConnection(LOOPBACK, port, timeout=3)
                with self.subTest(request=label), closing(connection) as client:
                    client.putrequest("GET", "/")
                    for name, value in headers:
                        client.putheader(name, value)
                    client.endheaders()
                    with client.getresponse() as response:
                        self.assertEqual(
                            (200, f"{expected} /".encode("ascii")), (response.status, response.read()),
                        )
                    print(f"{label}: HTTP 200 from backend {expected}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--haproxy", required=True, type=Path)
    args = parser.parse_args()
    for dist, expected in (("octavia", "19.0.0"), ("octavia-lib", "3.12.0")):
        installed = version(dist)
        if installed != expected:
            parser.error(f"expected {dist} {expected}, found {installed}")
    StockHTTPHeaders.haproxy = args.haproxy.resolve(strict=True)
    banner = subprocess.check_output([StockHTTPHeaders.haproxy, "-v"], text=True).splitlines()[0]
    if banner.split()[2].split("-")[0] != "3.0.19":
        parser.error(f"expected HAProxy 3.0.19, found {banner}")
    print(f"Octavia 19.0.0, octavia-lib 3.12.0, {banner}", flush=True)
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(StockHTTPHeaders)
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())


if __name__ == "__main__":
    main()
