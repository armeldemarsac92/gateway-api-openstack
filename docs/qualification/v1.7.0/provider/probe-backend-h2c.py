#!/usr/bin/env python3
"""Check enrolled cleartext HTTP/2 pools through the public Octavia API."""

import argparse
from collections import Counter
from pathlib import Path
import runpy
import unittest

import grpc


stock = runpy.run_path(str(Path(__file__).with_name("stock-grpc.py")))


class BackendH2C(stock["StockGRPC"]):
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
    result = unittest.TextTestRunner(verbosity=2).run(BackendH2C("test_pool_transport"))
    raise SystemExit(0 if result.wasSuccessful() else 1)
