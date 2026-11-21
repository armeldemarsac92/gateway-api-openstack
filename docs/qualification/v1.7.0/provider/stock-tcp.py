#!/usr/bin/env python3
"""Run the stock Octavia Amphora TCP probe against local HAProxy."""

import argparse
from importlib.metadata import version
from pathlib import Path
import socket
import socketserver
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock
from uuid import uuid4

from octavia.common import data_models
from octavia.common.jinja.haproxy.combined_listeners import jinja_cfg
from octavia.common import rpc
from octavia.common import utils
from octavia.network import data_models as network_models
from octavia.tests.functional.api.v2 import base


LOOPBACK = "127.0.0.1"
PAYLOAD = bytes(range(256))


class Echo(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(3)
        payload = self.rfile.read(len(PAYLOAD))
        self.server.received.append(payload)
        self.wfile.write(payload)


class StockTCP(base.BaseAPITest):
    def _make_app(self):
        self.enterContext(mock.patch.object(rpc, "init"))
        self.rpc_client = self.enterContext(mock.patch.object(rpc, "get_client")).return_value
        return super()._make_app()

    def test_default_pool_lifecycle(self):
        subnet_id, network_id, port_id = (str(uuid4()) for _ in range(3))
        network = mock.Mock()
        network.get_subnet.return_value = network_models.Subnet(
            id=subnet_id, network_id=network_id, cidr="127.0.0.0/8", ip_version=4,
        )
        network.get_network.return_value = network_models.Network(
            id=network_id, subnets=[subnet_id], port_security_enabled=True,
        )
        network.allocate_vip.return_value = (
            data_models.Vip(ip_address=LOOPBACK, subnet_id=subnet_id,
                            network_id=network_id, port_id=port_id),
            [],
        )
        self.enterContext(mock.patch.object(utils, "get_network_driver", return_value=network))

        echo = self.enterContext(socketserver.TCPServer((LOOPBACK, 0), Echo))
        echo.received = []
        thread = threading.Thread(target=echo.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, timeout=3)
        self.addCleanup(echo.shutdown)
        with socket.socket() as reserve:
            reserve.bind((LOOPBACK, 0))
            listener_port = reserve.getsockname()[1]

        lb = self.create_load_balancer(subnet_id, provider="amphora", vip_address=LOOPBACK)["loadbalancer"]
        self.assertEqual(("amphora", LOOPBACK), (lb["provider"], lb["vip_address"]))
        lb_id = lb["id"]
        self.set_lb_status(lb_id)
        listener = self.create_listener("TCP", listener_port, lb_id, connection_limit=16)["listener"]
        listener_id = listener["id"]
        self.assertEqual(("TCP", listener_port, None), (
            listener["protocol"], listener["protocol_port"], listener["default_pool_id"],
        ))
        self.set_lb_status(lb_id)
        pool = self.create_pool(lb_id, "TCP", "ROUND_ROBIN")["pool"]
        pool_id = pool["id"]
        pool_path = self.POOL_PATH.format(pool_id=pool_id)
        self.assertEqual(("TCP", []), (pool["protocol"], pool["listeners"]))
        self.set_lb_status(lb_id)
        member_port = echo.server_address[1]
        member = self.create_member(pool_id, LOOPBACK, member_port)["member"]
        member_path = self.MEMBER_PATH.format(pool_id=pool_id, member_id=member["id"])
        self.set_lb_status(lb_id)
        member = self.get(member_path).json["member"]
        self.assertEqual((LOOPBACK, member_port), (member["address"], member["protocol_port"]))
        print("API created an unattached TCP listener, separate TCP pool, and member")

        listener_path = self.LISTENER_PATH.format(listener_id=listener_id)
        self.put(listener_path, {"listener": {"default_pool_id": pool_id}})
        self.set_lb_status(lb_id)
        self.assertEqual(pool_id, self.get_listener(listener_id)["listener"]["default_pool_id"])
        amphora = data_models.Amphora(id=str(uuid4()))
        self.assertEqual(PAYLOAD, self.exchange(listener_id, listener_port, amphora))
        self.assertEqual([PAYLOAD], echo.received)
        print(f"Attached pool echoed all {len(PAYLOAD)} binary bytes through HAProxy")

        self.put(listener_path, {"listener": {"default_pool_id": None}})
        self.set_lb_status(lb_id)
        self.assertIsNone(self.get_listener(listener_id)["listener"]["default_pool_id"])
        self.assertEqual(member["id"], self.get(member_path).json["member"]["id"])
        self.assertEqual(b"", self.exchange(listener_id, listener_port, amphora))
        self.assertEqual([PAYLOAD], echo.received)
        print("Detached listener accepted a TCP connection without forwarding any bytes")

        for path in (member_path, pool_path, listener_path):
            self.delete(path)
            self.set_lb_status(lb_id)
            self.get(path, status=404)
        self.assertEqual(
            ["create_load_balancer", "create_listener", "create_pool", "create_member",
             "update_listener", "update_listener", "delete_member", "delete_pool",
             "delete_listener"],
            [call.args[1] for call in self.rpc_client.cast.call_args_list],
        )
        print("API deleted the member, pool, and listener after simulated worker completion")

    def exchange(self, listener_id, port, amphora):
        with self.get_session() as session:
            listener = self.listener_repo.get(session, id=listener_id)
        with tempfile.TemporaryDirectory(prefix="octavia-tcp-", dir="/tmp") as directory:
            config = Path(directory) / "haproxy.cfg"
            stats_socket = Path(directory) / "stat.sock"
            templater = jinja_cfg.JinjaTemplater(base_amp_path=directory, connection_logging=False)
            config.write_text(templater.build_config(
                host_amphora=amphora, listeners=[listener], tls_certs={},
                haproxy_versions=["3", "0", "19"], amp_details={}, socket_path=str(stats_socket),
            ), encoding="utf-8")
            validated = subprocess.run(
                [self.haproxy, "-c", "-f", config], capture_output=True, text=True, timeout=5,
            )
            self.assertEqual(0, validated.returncode, validated.stdout + validated.stderr)
            with subprocess.Popen([self.haproxy, "-db", "-f", config]) as proxy:
                try:
                    deadline = time.monotonic() + 5
                    while not stats_socket.exists():
                        self.assertIsNone(proxy.poll(), "HAProxy exited before opening its socket")
                        self.assertLess(time.monotonic(), deadline, "HAProxy did not start")
                        time.sleep(0.02)
                    self.assertIsNone(proxy.poll(), "HAProxy exited during startup")
                    with socket.create_connection((LOOPBACK, port), timeout=3) as client:
                        try:
                            client.sendall(PAYLOAD)
                            with client.makefile("rb") as stream:
                                return stream.read(len(PAYLOAD))
                        except (BrokenPipeError, ConnectionResetError):
                            return b""
                finally:
                    proxy.terminate()
                    try:
                        proxy.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        proxy.kill()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--haproxy", required=True, type=Path)
    args = parser.parse_args()
    for dist, expected in (("octavia", "19.0.0"), ("octavia-lib", "3.12.0")):
        installed = version(dist)
        if installed != expected:
            parser.error(f"expected {dist} {expected}, found {installed}")
    StockTCP.haproxy = args.haproxy.resolve(strict=True)
    banner = subprocess.check_output([StockTCP.haproxy, "-v"], text=True).splitlines()[0]
    if banner.split()[2].split("-")[0] != "3.0.19":
        parser.error(f"expected HAProxy 3.0.19, found {banner}")
    print(f"Octavia 19.0.0, octavia-lib 3.12.0, {banner}", flush=True)
    unittest.main(argv=[__file__], verbosity=2)


if __name__ == "__main__":
    main()
