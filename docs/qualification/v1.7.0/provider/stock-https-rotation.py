#!/usr/bin/env python3
"""Exercise public Barbican and Octavia APIs and stock HTTPS termination."""

import argparse
from contextlib import chdir
from datetime import UTC, datetime, timedelta
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, HTTPServer
from importlib.metadata import distribution, version
from importlib.util import module_from_spec, spec_from_file_location
import logging
import os
from pathlib import Path
import select
import socket
import ssl
import subprocess
import sys
import tempfile
from threading import Thread
import unittest
from unittest import mock
from urllib.parse import urlsplit
from uuid import UUID, uuid4
from wsgiref.simple_server import make_server, WSGIRequestHandler

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from webtest import TestApp


LOOPBACK = "127.0.0.1"
SECTIONS = (
    (None, "catchall.example.test"),
    ("second-example.org", "second-example.org"),
    ("third-example.wildcard.org", "third-example.wildcard.org"),
    ("fourth-example.wildcard.org", "fourth-example.wildcard.org"),
)
TIMEOUT = 8


class QuietWSGIHandler(WSGIRequestHandler):
    def log_request(self, *args):
        pass


def serve_barbican(directory):
    # Each Barbican plugin parses its own configuration on import.
    with mock.patch("oslo_config.cfg._get_config_dirs", return_value=[]):
        from barbican.common import config
        from barbican.plugin.crypto import simple_crypto
        from cryptography.fernet import Fernet
        from paste import deploy
        from paste.urlmap import URLMap

        config.CONF.set_override("connection", f"sqlite:///{directory / 'barbican.sqlite'}",
                                 group="database")
        config.CONF.set_override("db_auto_create", True)
        config.CONF.set_override("use_stderr", True)
        simple_crypto.CONF.set_override("kek", [Fernet.generate_key().decode("ascii")],
                                        group="simple_crypto_plugin")
        package = distribution("barbican")
        paste_file = next(file for file in package.files if file.name == "barbican-api-paste.ini")
        stock_paste = package.locate_file(paste_file).read_text(encoding="utf-8")
        local_paste = directory / "barbican-api-paste.ini"
        # Local clients supply fake confirmed identities in place of Keystone.
        local_paste.write_text(stock_paste.replace(" authtoken context ", " context "), encoding="utf-8")
        paste_uri = f"config:{local_paste}"
        public = URLMap()
        public["/"] = deploy.loadapp(paste_uri, name="barbican_version")
        public["/v1"] = deploy.loadapp(paste_uri, name="barbican-api-keystone")
        with make_server(LOOPBACK, 0, public, handler_class=QuietWSGIHandler) as server:
            print(server.server_port, flush=True)
            server.serve_forever()


def make_revisions():
    now = datetime.now(UTC)

    def certificate(name, key, issuer):
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
        return (x509.CertificateBuilder().subject_name(subject).issuer_name(issuer or subject)
                .public_key(key.public_key()).serial_number(x509.random_serial_number())
                .not_valid_before(now - timedelta(minutes=1))
                .not_valid_after(now + timedelta(days=1)))

    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca = (certificate("HTTPS fixture CA", ca_key, None)
          .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
          .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
          .add_extension(x509.KeyUsage(
              digital_signature=False, content_commitment=False, key_encipherment=False,
              data_encipherment=False, key_agreement=False, key_cert_sign=True,
              crl_sign=True, encipher_only=False, decipher_only=False,
          ), critical=True)
          .sign(ca_key, hashes.SHA256()))
    names = [x509.DNSName(name) for name in (
        "catchall.example.test", "second-example.org", "*.wildcard.org", "fourth-example.wildcard.org",
    )]
    revisions = []
    for label in ("A", "B"):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        leaf = (certificate(f"HTTPS fixture leaf {label}", key, ca.subject)
                .add_extension(x509.SubjectAlternativeName(names), critical=False)
                .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
                .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
                .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
                               critical=False)
                .sign(ca_key, hashes.SHA256()))
        bundle = pkcs12.serialize_key_and_certificates(
            label.encode("ascii"), key, leaf, None, serialization.NoEncryption(),
        )
        revisions.append((bundle, leaf.fingerprint(hashes.SHA256()).hex()))
    return ca.public_bytes(serialization.Encoding.PEM).decode("ascii"), revisions


class Backend(BaseHTTPRequestHandler):
    def do_GET(self):
        reply = f"fixture/{self.headers['Host']}{self.path}".encode("ascii")
        self.send_response(200)
        self.send_header("Content-Length", str(len(reply)))
        self.end_headers()
        self.wfile.write(reply)

    def log_message(self, *args):
        pass


def load_stock_fixture():
    spec = spec_from_file_location("stock_grpc", Path(__file__).with_name("stock-grpc.py"))
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.StockGRPC


# Octavia and Barbican initialize process-wide Oslo and Pecan configuration.
if __name__ == "__main__" and len(sys.argv) == 3 and sys.argv[1] == "--serve-barbican":
    serve_barbican(Path(sys.argv[2]))
    raise SystemExit


from barbicanclient import client as barbican_client
from barbicanclient import exceptions as barbican_exceptions
from keystoneauth1 import session as keystone_session, token_endpoint
from octavia.certificates.common.auth import barbican_acl
from octavia.certificates.manager import barbican as barbican_manager
from octavia.common import keystone, rpc
from octavia.tests.functional.api.v2.base import BaseAPITest
from oslo_config import cfg
import requests


REAL_CERT_MANAGER = barbican_manager.BarbicanCertManager


class StockHTTPS(load_stock_fixture()):
    def _make_app(self):
        self.enterContext(mock.patch.object(cfg, "_get_config_dirs", return_value=[]))
        self.enterContext(mock.patch.object(rpc, "init"))
        self.enterContext(mock.patch.object(rpc, "get_client"))
        # The upstream BaseAPITest replaces this entry point before _make_app.
        self.enterContext(mock.patch.object(barbican_manager, "BarbicanCertManager", REAL_CERT_MANAGER))
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="h-", dir=self.work_dir)))
        self.conf.config(group="certificates", cert_manager="barbican_cert_manager",
                         barbican_auth="barbican_acl_auth")
        # Give the stats socket a short absolute path into HAProxy's private working directory.
        self.conf.config(group="haproxy_amphora", base_path="/proc/self/cwd",
                         base_cert_dir=str(self.directory), connection_logging=False)
        return BaseAPITest._make_app(self)

    def _start_barbican(self):
        with (self.directory / "barbican.log").open("wb") as log:
            server = self.enterContext(subprocess.Popen(
                [sys.executable, Path(__file__).resolve(), "--serve-barbican", self.directory],
                cwd=self.directory, stdout=subprocess.PIPE, stderr=log,
                env={"PATH": os.defpath, "PYTHONDONTWRITEBYTECODE": "1",
                     "PYTHONPATH": os.environ.get("PYTHONPATH", "")},
            ))
        self.addCleanup(self._stop_barbican, server)
        ready, _, _ = select.select([server.stdout], [], [], TIMEOUT)
        self.assertTrue(ready, "Barbican did not start")
        port = server.stdout.readline().strip()
        self.assertTrue(port.isdigit(), "Barbican did not announce its loopback port")
        return f"http://{LOOPBACK}:{int(port)}"

    @staticmethod
    def _stop_barbican(server):
        if server.poll() is None:
            server.terminate()
        try:
            server.wait(timeout=TIMEOUT)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=TIMEOUT)

    def _configure_barbican_clients(self, endpoint):
        worker_id = uuid4().hex

        def local_session(user_id, role):
            transport = self.enterContext(requests.Session())
            transport.trust_env = False
            return keystone_session.Session(
                auth=token_endpoint.Token(f"{endpoint}/v1", uuid4().hex), session=transport, timeout=TIMEOUT,
                additional_headers={"X-Project-Id": self.project_id, "X-User-Id": user_id,
                                    "X-Identity-Status": "Confirmed", "X-Roles": role},
                redirect=False,
            )

        worker_session = local_session(worker_id, "reader")
        operator = barbican_client.Client(session=local_session(uuid4().hex, "member"), endpoint=endpoint)
        self.enterContext(mock.patch.object(keystone.KeystoneSession, "get_session", return_value=worker_session))
        self.enterContext(mock.patch.object(keystone.KeystoneSession, "get_service_user_id", return_value=worker_id))
        self.enterContext(mock.patch.object(barbican_acl.BarbicanACLAuth, "_barbican_client", None))
        self.enterContext(mock.patch.object(
            barbican_acl.BarbicanACLAuth, "get_barbican_client_user_auth", return_value=operator,
        ))
        return operator

    def _store_revision(self, operator, endpoint, revision):
        bundle, _ = revision
        expires = (datetime.now(UTC) + timedelta(days=1)).isoformat()
        ref = operator.secrets.create(name="HTTPS fixture revision", payload=bundle, expiration=expires).store()
        ref_parts = urlsplit(ref)
        expected = urlsplit(endpoint)
        self.assertTrue((ref_parts.scheme, ref_parts.netloc) == (expected.scheme, expected.netloc)
                        and ref_parts.path.startswith("/v1/secrets/")
                        and not (ref_parts.query or ref_parts.fragment), "unexpected public secret reference")
        self.assertTrue(UUID(ref_parts.path.removeprefix("/v1/secrets/")).version == 4,
                        "expected a version 4 identifier")
        self.addCleanup(self._delete_revision, operator, ref)
        return ref

    def _assert_stored_revision(self, operator, ref, bundle):
        self.assertTrue(operator.secrets.get(ref).payload == bundle, "stored PKCS#12 did not match the original")

    def _delete_revision(self, operator, ref):
        operator.secrets.delete(ref)
        with self.assertRaises(barbican_exceptions.HTTPClientError) as missing:
            operator.secrets.get(ref).payload
        self.assertEqual(404, missing.exception.status_code)

    def _start_backend(self):
        server = self.enterContext(HTTPServer((LOOPBACK, 0), Backend))
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, TIMEOUT)
        self.addCleanup(server.shutdown)
        return server.server_port

    def _delete_load_balancer(self, lb_id, listener_id):
        path = self.LB_PATH.format(lb_id=lb_id)
        self.set_lb_status(lb_id)
        self.delete(path, params={"cascade": "true"})
        # Simulate the worker completing the cascade before checking public visibility.
        self.set_lb_status(lb_id, status="DELETED")
        self.get(path, status=404)
        self.get(self.LISTENER_PATH.format(listener_id=listener_id), status=404)

    def _lose_update_response(self, path, ref):
        def drop(environ, _):
            def complete(status, headers, exc_info=None):
                self.assertEqual("200", status.split()[0], "stock listener update did not succeed")
                return lambda data: None

            response = self.app.app(environ, complete)
            try:
                for _ in response:
                    pass
            finally:
                if hasattr(response, "close"):
                    response.close()
            raise ConnectionResetError("fixture discarded response after stock WSGI completed")

        with self.assertRaisesRegex(ConnectionResetError, "fixture discarded response"):
            TestApp(drop).put_json(self._get_full_path(path), {"listener": {"default_tls_container_ref": ref}})

    def _assert_traffic(self, port, fingerprint, ca):
        for sni, hostname in SECTIONS:
            trust = ssl.create_default_context(cadata=ca)
            trust.check_hostname = sni is not None
            with (
                socket.create_connection((LOOPBACK, port), timeout=TIMEOUT) as connection,
                trust.wrap_socket(connection, server_hostname=sni) as tls,
            ):
                leaf = tls.getpeercert(binary_form=True)
                self.assertEqual(fingerprint, hashlib.sha256(leaf).hexdigest())
                tls.sendall(f"GET /probe HTTP/1.1\r\nHost: {hostname}\r\n"
                            "Connection: close\r\n\r\n".encode("ascii"))
                with http.client.HTTPResponse(tls) as response:
                    response.begin()
                    self.assertEqual(200, response.status)
                    self.assertEqual(f"fixture/{hostname}/probe".encode("ascii"), response.read())

    def test_rotation(self):
        endpoint = self._start_barbican()
        operator = self._configure_barbican_clients(endpoint)
        ca, (old, new) = make_revisions()
        old_ref = self._store_revision(operator, endpoint, old)
        new_ref = self._store_revision(operator, endpoint, new)
        self.assertTrue(old_ref != new_ref, "expected distinct Barbican references")
        self.assertNotEqual(old[1], new[1], "expected distinct leaf fingerprints")

        backend_port = self._start_backend()
        lb_id, listener_id, vip_port = self._listener(
            "TERMINATED_HTTPS", default_tls_container_ref=old_ref, alpn_protocols=["http/1.1"],
        )
        self.addCleanup(self._delete_load_balancer, lb_id, listener_id)
        listener_path = self.LISTENER_PATH.format(listener_id=listener_id)
        pool_id, = self._pools(lb_id, [backend_port], tls_enabled=False)
        self.put(listener_path, {"listener": {"default_pool_id": pool_id}})
        self.set_lb_status(lb_id)
        self._assert_listener(self.get_listener(listener_id)["listener"], listener_id,
                              vip_port, pool_id, old_ref)

        stats_socket = self.directory / f"{lb_id}.sock"
        before = self.directory / "revision-a.cfg"
        self._configure_amphora(self.directory, lb_id, before)
        with chdir(self.directory), self._run_proxy(before, stats_socket):
            self._assert_traffic(vip_port, old[1], ca)
        print(f"Revision A: four fresh HTTPS 200 replies, verified leaf SHA-256 {old[1]}")

        self._lose_update_response(listener_path, new_ref)
        fresh = TestApp(self.app.app)
        observed = fresh.get(self._get_full_path(listener_path), status=200).json["listener"]
        self._assert_listener(observed, listener_id, vip_port, pool_id, new_ref)
        self.set_lb_status(lb_id)
        print("The fixture dropped the listener PUT's HTTP 200. A fresh in-process WSGI client "
              "read revision B using the listener ID returned at creation.")

        after = self.directory / "revision-b.cfg"
        self._configure_amphora(self.directory, lb_id, after)
        with chdir(self.directory), self._run_proxy(after, stats_socket):
            self._assert_traffic(vip_port, new[1], ca)
        print(f"Revision B: four fresh HTTPS 200 replies, verified leaf SHA-256 {new[1]}")

        for ref, (expected_bundle, _) in ((old_ref, old), (new_ref, new)):
            self._assert_stored_revision(operator, ref, expected_bundle)
        print("Public Barbican GET returned both original PKCS#12 revisions unchanged")

    def _assert_listener(self, listener, listener_id, port, pool_id, ref):
        self.assertEqual((listener_id, "TERMINATED_HTTPS", port, pool_id, []), (
            listener["id"], listener["protocol"], listener["protocol_port"],
            listener["default_pool_id"], listener["sni_container_refs"],
        ))
        self.assertTrue(listener["default_tls_container_ref"] == ref,
                        "listener certificate reference differed from expected revision")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--haproxy", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    args = parser.parse_args()
    pins = {"barbican": "23.0.0", "octavia": "19.0.0", "octavia-lib": "3.12.0",
            "python-barbicanclient": "7.6.0", "SQLAlchemy": "2.0.52", "cryptography": "43.0.3"}
    for dist, expected in pins.items():
        installed = version(dist)
        if installed != expected:
            parser.error(f"expected {dist} {expected}, found {installed}")
    StockHTTPS.haproxy = args.haproxy.resolve(strict=True)
    StockHTTPS.work_dir = args.work_dir.resolve(strict=True)
    features = subprocess.check_output([StockHTTPS.haproxy, "-vv"], text=True)
    banner = features.splitlines()[0]
    if banner.split()[2].split("-")[0] != "3.0.19" or "+OPENSSL" not in features.split():
        parser.error(f"expected HAProxy 3.0.19 with OpenSSL, found {banner}")
    print(", ".join(f"{dist} {pin}" for dist, pin in pins.items()), banner, sep=", ", flush=True)
    logging.disable(logging.CRITICAL)  # Upstream log messages include secret reference URLs.
    suite = unittest.TestSuite([StockHTTPS("test_rotation")])
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():
        return 1
    print("Octavia cleanup sent a public cascade request. Load balancer and listener GETs returned "
          "404 after the fixture set DELETED.")
    print("Barbican cleanup deleted both secrets through the public API. Both GETs returned 404.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
