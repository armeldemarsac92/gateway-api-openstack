#!/usr/bin/env python3
"""Probe exact gRPC paths over TLS with stock Octavia and local HAProxy."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import csv
from importlib.metadata import version
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest
from unittest import mock
from uuid import uuid4

import grpc
from octavia.amphorae.drivers.haproxy import rest_api_driver
from octavia.certificates.manager.local import LocalCertManager
from octavia.common import data_models
from octavia.common import rpc
from octavia.common import utils
from octavia.network import data_models as network_models
from octavia.tests.functional.api.v2 import base


LOOPBACK = "127.0.0.1"
HOST = "grpc.example.test"
SERVICE = "feasibility.v1.Echo"
METHODS = ("First", "Second")
PAYLOAD = b"grpc probe"
TRAILER = "echo-backend"
TIMEOUT = 5


def rpc_path(method):
    return f"/{SERVICE}/{method}"


def make_test_ca(directory):
    def openssl(*args):
        subprocess.run(
            ["openssl", *args], cwd=directory, check=True, capture_output=True,
            timeout=TIMEOUT,
        )

    openssl(
        "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-subj", "/CN=Octavia gRPC test CA", "-addext", "basicConstraints=critical,CA:TRUE",
        "-keyout", "ca.key", "-out", "ca.crt",
    )
    for name, dns, purpose in (
        ("frontend", HOST, "serverAuth"),
        ("client", "client.example.test", "clientAuth"),
        ("backend-a", HOST, "serverAuth"),
        ("backend-b", HOST, "serverAuth"),
    ):
        openssl(
            "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={dns}",
            "-keyout", f"{name}.key", "-out", f"{name}.csr",
        )
        (directory / "leaf.ext").write_text(
            f"subjectAltName=DNS:{dns}\nextendedKeyUsage={purpose}\n", encoding="ascii",
        )
        openssl(
            "x509", "-req", "-in", f"{name}.csr", "-CA", "ca.crt", "-CAkey", "ca.key",
            "-CAcreateserial", "-days", "1", "-extfile", "leaf.ext", "-out", f"{name}.crt",
        )
    return (directory / "ca.crt").read_bytes()


def tls_pair(directory, name):
    return (directory / f"{name}.key").read_bytes(), (directory / f"{name}.crt").read_bytes()


def frontend_stats(stats_socket, listener_id):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(TIMEOUT)
        client.connect(str(stats_socket))
        client.sendall(b"show stat\n")
        with client.makefile("r", encoding="utf-8") as stream:
            rows = csv.DictReader(stream.read().removeprefix("# ").splitlines())
            row = next(row for row in rows if row["pxname"] == listener_id
                       and row["svname"] == "FRONTEND")
    return int(row["conn_tot"]), int(row["req_tot"])


class StockGRPC(base.BaseAPITest):
    def _make_app(self):
        self.enterContext(mock.patch.object(rpc, "init"))
        self.enterContext(mock.patch.object(rpc, "get_client"))
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory(
            prefix="octavia-grpc-", dir="/tmp",
        )))
        self.conf.config(group="certificates", cert_manager="local_cert_manager",
                         storage_path=str(self.directory))
        self.conf.config(group="haproxy_amphora", base_path=str(self.directory),
                         base_cert_dir=str(self.directory), connection_logging=False)
        return super()._make_app()

    def _vip_subnet(self):
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
        return subnet_id

    def _backend(self, directory, backend_id, calls):
        def handler(method):
            def echo(payload, context):
                calls[backend_id, method] += 1
                context.set_trailing_metadata(((TRAILER, backend_id),))
                return f"{backend_id}/{method}:".encode() + payload
            return grpc.unary_unary_rpc_method_handler(echo)

        executor = self.enterContext(ThreadPoolExecutor(max_workers=1))
        server = grpc.server(executor)
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
            SERVICE, {method: handler(method) for method in METHODS},
        ),))
        creds = grpc.ssl_server_credentials((tls_pair(directory, f"backend-{backend_id.lower()}"),))
        port = server.add_secure_port(f"{LOOPBACK}:0", creds)
        self.assertGreater(port, 0)
        server.start()
        self.addCleanup(lambda: server.stop(0).wait(TIMEOUT))
        return port

    def _configure_amphora(self, directory, lb_id, haproxy_config):
        amp_directory = directory / lb_id
        amp_directory.mkdir(exist_ok=True)

        def upload_cert(amphora, obj_id, name, pem):
            self.assertEqual(lb_id, obj_id)
            (amp_directory / name).write_bytes(pem)

        def upload_config(amphora, obj_id, config, **kwargs):
            self.assertEqual(lb_id, obj_id)
            haproxy_config.write_bytes(config.encode("utf-8"))

        driver = rest_api_driver.HaproxyAmphoraLoadBalancerDriver()
        agent = mock.Mock()
        agent.get_api_version.return_value = {"api_version": "1.0"}
        agent.get_info.return_value = {"haproxy_version": "3.0.19"}
        agent.get_details.return_value = {}
        agent.upload_cert_pem.side_effect = upload_cert
        agent.upload_config.side_effect = upload_config
        driver.clients.update({"base": agent, "1.0": agent})
        amphora = data_models.Amphora(id=str(uuid4()))
        with self.get_session() as session:
            lb = self.lb_repo.get(session, id=lb_id)
        driver.update_amphora_listeners(lb, amphora)
        agent.reload_listener.assert_called_once_with(amphora, lb_id, timeout_dict=None)

    @contextmanager
    def _run_proxy(self, config, stats_socket):
        subprocess.run([self.haproxy, "-c", "-f", config], check=True, timeout=TIMEOUT)
        with subprocess.Popen([self.haproxy, "-db", "-f", config]) as proxy:
            try:
                deadline = time.monotonic() + TIMEOUT
                while not stats_socket.exists():
                    self.assertIsNone(proxy.poll(), "HAProxy exited before creating its socket")
                    self.assertLess(time.monotonic(), deadline, "HAProxy did not start")
                    time.sleep(0.02)
                yield
            finally:
                proxy.terminate()
                try:
                    proxy.wait(timeout=TIMEOUT)
                except subprocess.TimeoutExpired:
                    proxy.kill()
                    proxy.wait(timeout=TIMEOUT)
                stats_socket.unlink(missing_ok=True)

    def _exercise(self, config, stats_socket, listener_id, listener_port, credentials,
                  expected_backends, check_near_match=False):
        with (
            self._run_proxy(config, stats_socket),
            grpc.secure_channel(
                f"{LOOPBACK}:{listener_port}", credentials,
                options=(("grpc.ssl_target_name_override", HOST),
                         ("grpc.default_authority", HOST), ("grpc.enable_retries", 0)),
            ) as client,
        ):
            self.assertEqual((0, 0), frontend_stats(stats_socket, listener_id))
            for method, backend_id in zip(METHODS * 2, expected_backends, strict=True):
                result, call = client.unary_unary(rpc_path(method)).with_call(PAYLOAD, timeout=TIMEOUT)
                self.assertEqual(f"{backend_id}/{method}:".encode() + PAYLOAD, result)
                self.assertIs(grpc.StatusCode.OK, call.code())
                self.assertIn((TRAILER, backend_id), call.trailing_metadata())
            self.assertEqual((1, 4), frontend_stats(stats_socket, listener_id))
            print(f"{config.stem}: {' '.join(expected_backends)}, "
                  "4 OK replies with backend trailers over 1 frontend connection")
            if check_near_match:
                with self.assertRaises(grpc.RpcError) as caught:
                    client.unary_unary(rpc_path("second"))(PAYLOAD, timeout=TIMEOUT)
                self.assertIs(grpc.StatusCode.UNAVAILABLE, caught.exception.code())
                self.assertIn("http2 header with status: 503", caught.exception.details())
                self.assertEqual((1, 5), frontend_stats(stats_socket, listener_id))
                print("Exact-path control: case-variant /second missed both pools "
                      "(HAProxy HTTP 503 mapped by grpcio to UNAVAILABLE)")

    def test_exact_paths(self):
        directory = self.directory
        ca_cert = make_test_ca(directory)
        frontend_key, frontend_cert = tls_pair(directory, "frontend")
        frontend_ref = LocalCertManager.store_cert(None, frontend_cert, frontend_key)
        client_key, client_cert = tls_pair(directory, "client")
        credentials = grpc.ssl_channel_credentials(ca_cert, client_key, client_cert)
        calls = Counter()
        ports = [self._backend(directory, backend_id, calls) for backend_id in ("A", "B")]
        with socket.socket() as reserve:
            reserve.bind((LOOPBACK, 0))
            listener_port = reserve.getsockname()[1]

        lb = self.create_load_balancer(self._vip_subnet(), provider="amphora",
                                       vip_address=LOOPBACK)["loadbalancer"]
        self.assertEqual(("amphora", LOOPBACK), (lb["provider"], lb["vip_address"]))
        lb_id = lb["id"]
        self.set_lb_status(lb_id)
        listener = self.create_listener(
            "TERMINATED_HTTPS", listener_port, lb_id, connection_limit=16,
            default_tls_container_ref=frontend_ref, alpn_protocols=["h2"],
            client_ca_tls_container_ref="ca", client_authentication="MANDATORY",
        )["listener"]
        listener_id = listener["id"]
        listener_path = self.LISTENER_PATH.format(listener_id=listener_id)
        self.set_lb_status(lb_id)
        pool_ids = []
        for port in ports:
            pool = self.create_pool(
                lb_id, "HTTP", "ROUND_ROBIN", tls_enabled=True,
                ca_tls_container_ref="ca", alpn_protocols=["h2"],
            )["pool"]
            pool_id = pool["id"]
            pool_ids.append(pool_id)
            self.set_lb_status(lb_id)
            member = self.create_member(pool_id, LOOPBACK, port)["member"]
            self.set_lb_status(lb_id)
            member_path = self.MEMBER_PATH.format(pool_id=pool_id, member_id=member["id"])
            member = self.get(member_path).json["member"]
            self.assertEqual((LOOPBACK, port), (member["address"], member["protocol_port"]))
            pool = self.get(self.POOL_PATH.format(pool_id=pool_id)).json["pool"]
            self.assertEqual(("HTTP", True, ["h2"], "ca"), (
                pool["protocol"], pool["tls_enabled"], pool["alpn_protocols"],
                pool["ca_tls_container_ref"],
            ))

        stats_socket = directory / f"{lb_id}.sock"
        self.put(listener_path, {"listener": {"default_pool_id": pool_ids[0]}})
        self.set_lb_status(lb_id)
        control = directory / "default-pool.cfg"
        self._configure_amphora(directory, lb_id, control)
        self._exercise(control, stats_socket, listener_id, listener_port, credentials, "AAAA")
        self.assertEqual(Counter({("A", "First"): 2, ("A", "Second"): 2}), calls)

        for position, (method, pool_id) in enumerate(zip(METHODS, pool_ids, strict=True), 1):
            policy = self.create_l7policy(
                listener_id, "REDIRECT_TO_POOL", redirect_pool_id=pool_id, position=position,
            )["l7policy"]
            policy_id = policy["id"]
            self.set_lb_status(lb_id)
            rule = self.create_l7rule(policy_id, "PATH", "EQUAL_TO", rpc_path(method))["rule"]
            self.set_lb_status(lb_id)
            rule_path = self.L7RULE_PATH.format(l7policy_id=policy_id, l7rule_id=rule["id"])
            rule = self.get(rule_path).json["rule"]
            self.assertEqual(("PATH", "EQUAL_TO", rpc_path(method)), (
                rule["type"], rule["compare_type"], rule["value"],
            ))
            policy = self.get(self.L7POLICY_PATH.format(l7policy_id=policy_id)).json["l7policy"]
            self.assertEqual(("REDIRECT_TO_POOL", pool_id, position), (
                policy["action"], policy["redirect_pool_id"], policy["position"],
            ))
        self.put(listener_path, {"listener": {"default_pool_id": None}})
        self.set_lb_status(lb_id)
        listener = self.get_listener(listener_id)["listener"]
        self.assertEqual(("TERMINATED_HTTPS", ["h2"], "MANDATORY", None), (
            listener["protocol"], listener["alpn_protocols"], listener["client_authentication"],
            listener["default_pool_id"],
        ))

        routed = directory / "exact-path.cfg"
        self._configure_amphora(directory, lb_id, routed)
        calls.clear()
        self._exercise(routed, stats_socket, listener_id, listener_port, credentials, "ABAB",
                       check_near_match=True)
        self.assertEqual(Counter({("A", "First"): 2, ("B", "Second"): 2}), calls)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--haproxy", required=True, type=Path)
    args = parser.parse_args()
    pins = {"octavia": "19.0.0", "octavia-lib": "3.12.0", "grpcio": "1.83.0",
            "SQLAlchemy": "2.0.52"}
    for dist, expected in pins.items():
        installed = version(dist)
        if installed != expected:
            parser.error(f"expected {dist} {expected}, found {installed}")
    StockGRPC.haproxy = args.haproxy.resolve(strict=True)
    features = subprocess.check_output([StockGRPC.haproxy, "-vv"], text=True)
    banner = features.splitlines()[0]
    if banner.split()[2].split("-")[0] != "3.0.19" or "+OPENSSL" not in features.split():
        parser.error(f"expected HAProxy 3.0.19 with OpenSSL, found {banner}")
    print(", ".join(f"{dist} {pin}" for dist, pin in pins.items()), banner, sep=", ", flush=True)
    unittest.main(argv=[__file__], verbosity=2)


if __name__ == "__main__":
    main()
