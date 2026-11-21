#!/usr/bin/env python3
"""Run the stock Octavia Amphora TCP or TLS SNI probe against local HAProxy."""

import argparse
from contextlib import contextmanager
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
import socket
import socketserver
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock
from uuid import uuid4

from octavia.amphorae.drivers.haproxy import rest_api_driver
from octavia.common import data_models
from octavia.common.jinja.haproxy.combined_listeners import jinja_cfg
from octavia.common import rpc
from octavia.common import utils
from octavia.network import data_models as network_models
from octavia.tests.functional.api.v2 import base


LOOPBACK = "127.0.0.1"
PAYLOAD = bytes(range(256))
SNI_PORT = 9443
SNI_NAMES = ("a.example.test", "b.example.test")


class Echo(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(3)
        payload = self.rfile.read(len(PAYLOAD))
        self.server.received.append(payload)
        self.wfile.write(payload)


class StockAmphoraAPI(base.BaseAPITest):
    def _make_app(self):
        self.enterContext(mock.patch.object(rpc, "init"))
        self.rpc_client = self.enterContext(mock.patch.object(rpc, "get_client")).return_value
        return super()._make_app()

    def create_loopback_lb(self):
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
        lb = self.create_load_balancer(subnet_id, provider="amphora", vip_address=LOOPBACK)["loadbalancer"]
        self.assertEqual(("amphora", LOOPBACK), (lb["provider"], lb["vip_address"]))
        self.set_lb_status(lb["id"])
        return lb["id"]

    @contextmanager
    def run_haproxy(self, config, stats_socket):
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
                yield
            finally:
                proxy.terminate()
                try:
                    proxy.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proxy.kill()
                    proxy.wait(timeout=3)
                stats_socket.unlink(missing_ok=True)


class StockTCP(StockAmphoraAPI):
    def test_default_pool_lifecycle(self):
        echo = self.enterContext(socketserver.TCPServer((LOOPBACK, 0), Echo))
        echo.received = []
        thread = threading.Thread(target=echo.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, timeout=3)
        self.addCleanup(echo.shutdown)
        with socket.socket() as reserve:
            reserve.bind((LOOPBACK, 0))
            listener_port = reserve.getsockname()[1]

        lb_id = self.create_loopback_lb()
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

    def test_tcp_listener_rejects_l7_policy(self):
        lb_id = self.create_loopback_lb()
        listener = self.create_listener("TCP", 8443, lb_id)["listener"]
        self.set_lb_status(lb_id)
        pool = self.create_pool(lb_id, "TCP", "ROUND_ROBIN")["pool"]
        self.set_lb_status(lb_id)
        refused = self.create_l7policy(listener["id"], "REDIRECT_TO_POOL", status=501, redirect_pool_id=pool["id"])
        self.assertIn("TCP protocol listeners do not support L7 policies", refused["faultstring"])
        self.assertNotIn("create_l7policy", [call.args[1] for call in self.rpc_client.cast.call_args_list])
        print("The Amphora driver rejected an L7 forwarding policy on TCP before dispatch to a worker")
        policies = self.get(self.L7POLICIES_PATH, params={"listener_id": listener["id"]}).json["l7policies"]
        print("SQLite policy states after HTTP 501:", [policy["provisioning_status"] for policy in policies])

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
            with self.run_haproxy(config, stats_socket), socket.create_connection((LOOPBACK, port), timeout=3) as client:
                try:
                    client.sendall(PAYLOAD)
                    with client.makefile("rb") as stream:
                        return stream.read(len(PAYLOAD))
                except (BrokenPipeError, ConnectionResetError):
                    return b""


def make_tls_test_ca(directory):
    def openssl(*args):
        subprocess.run(["openssl", *args], cwd=directory, check=True, capture_output=True, timeout=5)

    openssl(
        "req", "-x509", "-newkey", "rsa:2048", "-noenc", "-days", "1",
        "-subj", "/CN=Octavia passthrough test CA", "-addext", "basicConstraints=critical,CA:TRUE",
        "-keyout", "ca.key", "-out", "ca.crt",
    )
    san = ",".join(f"DNS:{name}" for name in SNI_NAMES)
    (directory / "leaf.ext").write_text(f"subjectAltName={san}\nextendedKeyUsage=serverAuth\n", encoding="ascii")
    for backend in ("A", "B"):
        openssl(
            "req", "-new", "-newkey", "rsa:2048", "-noenc", "-subj", f"/CN=Backend {backend}",
            "-keyout", f"{backend}.key", "-out", f"{backend}.csr",
        )
        openssl(
            "x509", "-req", "-in", f"{backend}.csr", "-CA", "ca.crt", "-CAkey", "ca.key",
            "-CAcreateserial", "-days", "1", "-extfile", "leaf.ext", "-out", f"{backend}.crt",
        )


class TLSEcho(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(3)
        with self.server.context.wrap_socket(self.request, server_side=True) as client:
            with client.makefile("rb") as stream:
                payload = stream.read(len(PAYLOAD))
            self.server.received.append((self.server.backend, client.sni, payload))
            client.sendall(self.server.backend.encode("ascii") + payload)


class StockTLSSNI(StockAmphoraAPI):
    def new_backend(self, directory, backend, received):
        def observe_sni(connection, name, _context):
            connection.sni = name

        cert = directory / f"{backend}.crt"
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, directory / f"{backend}.key")
        context.sni_callback = observe_sni
        server = self.enterContext(socketserver.TCPServer((LOOPBACK, 0), TLSEcho))
        server.context = context
        server.backend = backend
        server.received = received
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, timeout=3)
        self.addCleanup(server.shutdown)
        return server.server_address[1], sha256(ssl.PEM_cert_to_DER_cert(cert.read_text())).hexdigest()

    def check_exchange(self, phase, port, name, backend, fingerprint, received):
        before = len(received)
        with (
            socket.create_connection((LOOPBACK, port), timeout=3) as connection,
            self.client_tls.wrap_socket(connection, server_hostname=name) as client,
            client.makefile("rb") as stream,
        ):
            self.assertFalse(client.session_reused)
            leaf = sha256(client.getpeercert(binary_form=True)).hexdigest()
            client.sendall(PAYLOAD)
            reply = stream.read(len(PAYLOAD) + 1)
        self.assertEqual(fingerprint, leaf)
        self.assertEqual(backend.encode("ascii") + PAYLOAD, reply)
        self.assertEqual([(backend, name, PAYLOAD)], received[before:])
        print(f"{phase}: {LOOPBACK}:{port} SNI={name} backend={backend} leaf_sha256={leaf}")

    def render_worker_config(self, config, lb_id, amphora):
        driver = rest_api_driver.HaproxyAmphoraLoadBalancerDriver()
        agent = mock.Mock()
        agent.get_api_version.return_value = {"api_version": "1.0"}
        agent.get_info.return_value = {"haproxy_version": "3.0.19"}
        agent.get_details.return_value = {}
        driver.clients.update({"base": agent, "1.0": agent})
        with self.get_session() as session:
            lb = self.lb_repo.get(session, id=lb_id)
            driver.update_amphora_listeners(lb, amphora)
        agent.upload_config.assert_called_once()
        target, object_id, rendered = agent.upload_config.call_args.args
        self.assertEqual((amphora, lb_id), (target, object_id))
        agent.reload_listener.assert_called_once_with(amphora, lb_id, timeout_dict=None)
        agent.upload_cert_pem.assert_not_called()
        config.write_bytes(rendered.encode("utf-8"))
        return [line.strip() for line in rendered.splitlines()]

    def test_tcp_on_one_port(self):
        directory = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="octavia-sni-", dir="/tmp")))
        self.conf.config(group="haproxy_amphora", base_path=str(directory), connection_logging=False)
        make_tls_test_ca(directory)
        self.client_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.client_tls.load_verify_locations(directory / "ca.crt")
        received = []
        backends = {backend: self.new_backend(directory, backend, received) for backend in ("A", "B")}
        self.assertNotEqual(backends["A"][1], backends["B"][1])

        def check_direct(phase):
            for backend, (port, fingerprint) in backends.items():
                for name in SNI_NAMES:
                    self.check_exchange(phase, port, name, backend, fingerprint, received)

        check_direct("direct-before")
        lb_id = self.create_loopback_lb()
        listener = self.create_listener("TCP", SNI_PORT, lb_id, timeout_tcp_inspect=100)["listener"]
        listener_id = listener["id"]
        self.set_lb_status(lb_id)
        listener_path = self.LISTENER_PATH.format(listener_id=listener_id)
        pools = {}
        for backend, (port, _) in backends.items():
            pool = self.create_pool(lb_id, "TCP", "ROUND_ROBIN")["pool"]
            pool_id = pools[backend] = pool["id"]
            self.set_lb_status(lb_id)
            member = self.create_member(pool_id, LOOPBACK, port)["member"]
            self.set_lb_status(lb_id)
            path = self.MEMBER_PATH.format(pool_id=pool_id, member_id=member["id"])
            member = self.get(path).json["member"]
            self.assertEqual((LOOPBACK, port), (member["address"], member["protocol_port"]))

        amphora = data_models.Amphora(id=str(uuid4()))
        stats_socket = directory / f"{lb_id}.sock"
        for phase, backend in (("pool-A", "A"), ("detached", None), ("pool-B", "B")):
            pool_id = pools.get(backend)
            self.put(listener_path, {"listener": {"default_pool_id": pool_id}})
            self.set_lb_status(lb_id)
            stored = self.get_listener(listener_id)["listener"]
            self.assertEqual(("TCP", SNI_PORT, 100, pool_id), (
                stored["protocol"], stored["protocol_port"], stored["timeout_tcp_inspect"], stored["default_pool_id"],
            ))
            config = directory / f"{phase}.cfg"
            lines = self.render_worker_config(config, lb_id, amphora)
            self.assertIn(f"bind {LOOPBACK}:{SNI_PORT}", lines)
            self.assertIn("mode tcp", lines)
            self.assertEqual(["tcp-request inspect-delay 100"],
                             [line for line in lines if line.startswith("tcp-request ")])
            self.assertEqual([f"default_backend {pool_id}:{listener_id}"] if pool_id else [],
                             [line for line in lines if line.startswith("default_backend ")])
            self.assertFalse(any(line.startswith(("acl ", "use_backend ")) for line in lines))

            with self.run_haproxy(config, stats_socket):
                for name in SNI_NAMES:
                    if backend:
                        self.check_exchange(phase, SNI_PORT, name, backend, backends[backend][1], received)
                    else:
                        before = len(received)
                        with socket.create_connection((LOOPBACK, SNI_PORT), timeout=3) as connection:
                            with self.assertRaises((ssl.SSLEOFError, ConnectionResetError, BrokenPipeError)) as error:
                                self.client_tls.wrap_socket(connection, server_hostname=name).close()
                        self.assertEqual(before, len(received))
                        print(f"{phase}: {LOOPBACK}:{SNI_PORT} SNI={name} TCP connected, "
                              f"TLS {type(error.exception).__name__}, no backend request")

        check_direct("direct-after")

    def test_opaque_https_rejects_l7_policy(self):
        lb_id = self.create_loopback_lb()
        listener = self.create_listener("HTTPS", SNI_PORT, lb_id)["listener"]
        self.set_lb_status(lb_id)
        pool = self.create_pool(lb_id, "HTTPS", "ROUND_ROBIN")["pool"]
        self.set_lb_status(lb_id)
        refused = self.create_l7policy(
            listener["id"], "REDIRECT_TO_POOL", status=501, redirect_pool_id=pool["id"],
        )
        self.assertIn("HTTPS protocol listeners do not support L7 policies", refused["faultstring"])
        self.assertNotIn("create_l7policy", [call.args[1] for call in self.rpc_client.cast.call_args_list])
        print("Opaque HTTPS: HTTP 501 for REDIRECT_TO_POOL before worker dispatch")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--haproxy", required=True, type=Path)
    parser.add_argument("--tls-sni", action="store_true", help="run the same-port TLS passthrough probe")
    args = parser.parse_args()
    for dist, expected in (("octavia", "19.0.0"), ("octavia-lib", "3.12.0")):
        installed = version(dist)
        if installed != expected:
            parser.error(f"expected {dist} {expected}, found {installed}")
    test_case = StockTLSSNI if args.tls_sni else StockTCP
    test_case.haproxy = args.haproxy.resolve(strict=True)
    banner = subprocess.check_output([test_case.haproxy, "-v"], text=True).splitlines()[0]
    if banner.split()[2].split("-")[0] != "3.0.19":
        parser.error(f"expected HAProxy 3.0.19, found {banner}")
    print(f"Octavia 19.0.0, octavia-lib 3.12.0, {banner}", flush=True)
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(test_case)
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())


if __name__ == "__main__":
    main()
