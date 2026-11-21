#!/usr/bin/env python3
"""Check enrolled cleartext HTTP/2 pools through the public Octavia API."""

import argparse
from collections import Counter
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import runpy
import threading
import unittest

import grpc


stock = runpy.run_path(str(Path(__file__).with_name("stock-grpc.py")))


class BackendH2C(stock["StockGRPC"]):
    def test_dedicated_http11_monitor(self):
        probed = threading.Event()
        checks = []

        class Health(BaseHTTPRequestHandler):
            def do_GET(self):
                checks.append((self.request_version, self.path, self.headers.get("Host")))
                self.send_response(200)
                self.end_headers()
                if len(checks) >= 3:
                    probed.set()

            def log_message(self, *_):
                pass

        with HTTPServer((stock["LOOPBACK"], 0), Health) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                self.conf.config(group="api_settings", enabled_backend_h2c_providers=["amphora"])
                service, method = stock["FIXTURE_SERVICE"], stock["FIXTURE_METHODS"][0]
                calls = Counter()
                backend_port = self._backend("A", calls, service, (method,))
                lb, listener, port = self._listener("HTTP")
                pool, = self._pools(lb, [backend_port], tls_enabled=False, backend_h2c=True)
                member, = self.get(self.MEMBERS_PATH.format(pool_id=pool)).json["members"]
                self.put(self.MEMBER_PATH.format(pool_id=pool, member_id=member["id"]),
                         {"member": {"monitor_port": server.server_port}})
                self.set_lb_status(lb)
                self.create_health_monitor(pool, "HTTP", 1, 1, 2, 2, http_version=1.1,
                                           url_path="/healthz", domain_name="health.example.test")
                self.set_lb_status(lb)
                self._exact_routes(lb, listener, [pool], [stock["rpc_path"](method, service)])
                config = self.directory / "dedicated-monitor.cfg"
                self._configure_amphora(self.directory, lb, config)
                with self._run_proxy(config, self.directory / f"{lb}.sock"), stock["plaintext_channel"](port) as client:
                    self.assertTrue(probed.wait(10), "three HTTP health checks did not arrive")
                    self.assertEqual({("HTTP/1.1", "/healthz", "health.example.test")}, set(checks))
                    self._assert_reply(client, method, "A", service)
                    self.assertEqual({("A", method): 1}, calls)
            finally:
                server.shutdown()
                thread.join()

    def test_pool_transport(self):
        calls = Counter()
        service = stock["FIXTURE_SERVICE"]
        first, second = stock["FIXTURE_METHODS"]
        ports = [self._backend(name, calls, service, (first, second))
                 for name in ("A", "B")]
        lb, listener, port = self._listener("HTTP")
        pools = self._pools(lb, ports, tls_enabled=False)
        methods = [stock["rpc_path"](method, service) for method in (first, second)]
        self._exact_routes(lb, listener, pools, methods)
        socket = self.directory / f"{lb}.sock"
        pool_paths = [self.POOL_PATH.format(pool_id=p) for p in pools]
        discovery = f"{self.PROVIDERS_PATH}/amphora/gateway_capabilities"
        self.assertEqual([], self.get(discovery).json["gateway_capabilities"])
        self.put(pool_paths[0], {"pool": {"backend_h2c": True}}, status=501)

        self.conf.config(group="api_settings", enabled_backend_h2c_providers=["amphora"])
        self.assertEqual(["backend_h2c_v1"], self.get(discovery).json["gateway_capabilities"])
        for count, path in enumerate(pool_paths, start=1):
            self.put(path, {"pool": {"backend_h2c": True}})
            self.set_lb_status(lb)
            self.assertIs(True, self.get(path).json["pool"]["backend_h2c"])
            config = self.directory / f"h2c-{count}.cfg"
            self._configure_amphora(self.directory, lb, config)
            self.assertEqual(count, config.read_text().count(" proto h2"))
            calls.clear()
            with self._run_proxy(config, socket), stock["plaintext_channel"](port) as client:
                self._assert_reply(client, first, "A", service)
                if count == 1:
                    with self.assertRaises(grpc.RpcError) as failed:
                        client.unary_unary(methods[1])(b"", timeout=stock["TIMEOUT"])
                    self.assertIn("502", failed.exception.details())
                    self.assertEqual({("A", first): 1}, calls)
                else:
                    self._assert_reply(client, second, "B", service)
                    self.assertEqual({("A", first): 1, ("B", second): 1}, calls)
                self.assertEqual((1, 2), stock["frontend_stats"](socket, listener))
        self.conf.config(group="api_settings", enabled_backend_h2c_providers=[])
        self.assertEqual([], self.get(discovery).json["gateway_capabilities"])
        self.put(pool_paths[1], {"pool": {"backend_h2c": False}})
        self.set_lb_status(lb)
        self.assertIs(False, self.get(pool_paths[1]).json["pool"]["backend_h2c"])
        config = self.directory / "cleared.cfg"
        self._configure_amphora(self.directory, lb, config)
        self.assertEqual(1, config.read_text().count(" proto h2"))
        with self._run_proxy(config, socket), stock["plaintext_channel"](port) as client:
            self._assert_reply(client, first, "A", service)
            with self.assertRaises(grpc.RpcError) as failed:
                client.unary_unary(methods[1])(b"", timeout=stock["TIMEOUT"])
            self.assertIn("502", failed.exception.details())
        print("A only: OK/502. Both pools: OK/OK with backend trailers. "
              "B cleared after regional opt-out: OK/502.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--haproxy", type=Path, required=True)
    BackendH2C.haproxy = parser.parse_args().haproxy.resolve(strict=True)
    tests = unittest.TestSuite(BackendH2C(name) for name in
                               ("test_pool_transport", "test_dedicated_http11_monitor"))
    result = unittest.TextTestRunner(verbosity=2).run(tests)
    raise SystemExit(0 if result.wasSuccessful() else 1)
