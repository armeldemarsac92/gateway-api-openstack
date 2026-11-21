import base64
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.ec import SECP256R1, generate_private_key
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import NameOID

from bindings import Ledger, Rejected, creation_receipt


SCOPE = {"cluster": "cluster", "gatewayClass": "octavia", "project": "project", "region": "RegionOne"}
ENDPOINT = "https://barbican.example/v1/"
FIRST = "11111111-1111-4111-8111-111111111111"
SECOND = "22222222-2222-4222-8222-222222222222"
REVISION = "opaque/revision-a"


def certificate(name, key, issuer_name, issuer_key, expiry):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    return (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
        .issuer_name(issuer_name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + expiry)
        .sign(issuer_key, hashes.SHA256())
    )


def materials():
    issuer_key = generate_private_key(SECP256R1())
    parent_key = generate_private_key(SECP256R1())
    key = generate_private_key(SECP256R1())
    root_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "root")])
    parent = certificate("parent", parent_key, root_name, parent_key, timedelta(days=30))
    issuer = certificate("issuer", issuer_key, parent.subject, parent_key, timedelta(days=2))
    leaf = certificate("web", key, issuer.subject, issuer_key, timedelta(days=10))
    data = {
        "tls.crt": base64.b64encode(b"".join(cert.public_bytes(serialization.Encoding.PEM) for cert in (leaf, issuer, parent))).decode(),
        "tls.key": base64.b64encode(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())).decode(),
    }
    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "kubernetes.io/tls",
        "metadata": {"namespace": "certs", "name": "web", "uid": "opaque-uid", "resourceVersion": REVISION},
        "data": data,
    }
    bundle = pkcs12.serialize_key_and_certificates(b"web", key, leaf, [issuer, parent], serialization.NoEncryption())
    wrong_order_bundle = pkcs12.serialize_key_and_certificates(b"web", key, leaf, [parent, issuer], serialization.NoEncryption())
    return secret, bundle, wrong_order_bundle, issuer.not_valid_after_utc


def receipt(bundle, identifier=FIRST):
    return creation_receipt(SCOPE, ENDPOINT, {"secret_ref": ENDPOINT + "secrets/" + identifier}, bundle)


class BindingsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.secret, cls.bundle, cls.wrong_order, cls.earliest_expiry = materials()
        cls.other_secret, cls.other_bundle, _, _ = materials()

    def setUp(self):
        directory = self.enterContext(tempfile.TemporaryDirectory())
        self.path = Path(directory) / "revisions.json"
        self.ledger = Ledger(self.path, SCOPE)

    def test_create_receipt_for_catalog_service_path(self):
        ref = {"secret_ref": "https://cloud.example/key-manager/v1/secrets/" + FIRST}
        for endpoint in ("https://cloud.example/key-manager", "https://cloud.example/key-manager/v1/"):
            with self.subTest(endpoint=endpoint):
                creation = creation_receipt(SCOPE, endpoint, ref, self.bundle)
                self.assertEqual(creation["barbicanSecretID"], FIRST)

    def test_requires_scoped_creation_receipt_and_both_native_inputs(self):
        wrong_key = deepcopy(self.secret)
        wrong_key["data"]["tls.key"] = self.other_secret["data"]["tls.key"]
        wrong_type = {**self.secret, "type": "Opaque"}
        wrong_project = {**receipt(self.bundle), "scope": {**SCOPE, "project": "other"}}
        for secret, creation, payload in (
            (wrong_key, receipt(self.bundle), self.bundle),
            (wrong_type, receipt(self.bundle), self.bundle),
            (self.secret, receipt(self.bundle), self.other_bundle),
            (self.secret, receipt(self.other_bundle), self.other_bundle),
            (self.secret, receipt(self.wrong_order), self.wrong_order),
            (self.secret, wrong_project, self.bundle),
        ):
            with self.subTest(case=(secret["type"], creation["scope"]["project"])):
                with self.assertRaises(Rejected):
                    self.ledger.enroll(secret, creation, payload)
                self.assertFalse(self.path.exists())
        with self.assertRaises(Rejected):
            creation_receipt(SCOPE, ENDPOINT, {"secret_ref": "https://other.example/v1/secrets/" + FIRST}, self.bundle)

        row = self.ledger.enroll(self.secret, receipt(self.bundle), self.bundle)
        self.assertEqual(row["notAfter"], self.earliest_expiry.strftime("%Y-%m-%dT%H:%M:%SZ"))
        self.assertEqual(row["resourceVersion"], REVISION)
        self.assertEqual(len(row["chainSHA256"]), 3)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    def test_rotation_and_revocation_require_the_exact_active_row(self):
        original = self.ledger.enroll(self.secret, receipt(self.bundle), self.bundle)
        self.assertEqual(self.ledger.enroll(self.secret, receipt(self.bundle), self.bundle), original)
        rotated = deepcopy(self.secret)
        rotated["metadata"]["resourceVersion"] = "opaque/revision-b"
        with self.assertRaises(Rejected):
            self.ledger.enroll(rotated, receipt(self.bundle, SECOND), self.bundle)
        new = self.ledger.enroll(rotated, receipt(self.bundle, SECOND), self.bundle, previous=original)
        with self.assertRaises(Rejected):
            self.ledger.revoke(original)
        self.ledger.revoke(new)
        self.assertEqual(self.ledger.enroll(rotated, receipt(self.bundle, SECOND), self.bundle), new)

    def test_competing_issuers_cannot_overwrite_the_active_revision(self):
        def enroll(identifier):
            try:
                return self.ledger.enroll(self.secret, receipt(self.bundle, identifier), self.bundle)
            except Rejected:
                return None

        with ThreadPoolExecutor(max_workers=2) as workers:
            outcomes = list(workers.map(enroll, (FIRST, SECOND)))
        accepted = [row for row in outcomes if row is not None]
        self.assertEqual(len(accepted), 1)
        self.assertEqual(self.ledger.enroll(self.secret, receipt(self.bundle, accepted[0]["barbicanSecretID"]), self.bundle), accepted[0])


if __name__ == "__main__":
    unittest.main()
