"""Issue and withdraw trusted HTTPS enrollment from local operator evidence."""

import argparse
import base64
import fcntl
import hashlib
import ipaddress
import json
import os
import re
import stat
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

from cryptography import x509
from cryptography.exceptions import InvalidSignature, UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.serialization import pkcs12


MAX_MAP_BYTES = 1 << 20
MAX_INPUT_BYTES = 4 << 20
SCOPE = {"cluster", "gatewayClass", "project", "region"}
IDENTITY = ("namespace", "name", "uid", "resourceVersion", "barbicanSecretID")
ROW_FIELDS = set(IDENTITY) | {"chainSHA256", "notBefore", "notAfter"}
DNS_LABEL = r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?"
PEM_CERTIFICATE = re.compile(rb"\s*-----BEGIN CERTIFICATE-----[\r\n]+[A-Za-z0-9+/=\r\n]+-----END CERTIFICATE-----")


class Rejected(ValueError):
    pass


def checked(condition, message):
    if not condition:
        raise Rejected(message)


def fields(value, expected):
    return isinstance(value, dict) and value.keys() == expected


def nonempty(value):
    return isinstance(value, str) and bool(value)


def dns_name(value):
    return nonempty(value) and len(value) <= 253 and re.fullmatch(rf"{DNS_LABEL}(?:\.{DNS_LABEL})*", value)


def secret_key(value):
    namespace, name = value.get("namespace"), value.get("name")
    checked(
        nonempty(namespace) and len(namespace) <= 63 and re.fullmatch(DNS_LABEL, namespace) and dns_name(name),
        "invalid Secret namespace or name",
    )
    return namespace, name


def check_scope(scope):
    checked(
        fields(scope, SCOPE)
        and all(nonempty(value) and value == value.strip() for value in scope.values())
        and dns_name(scope["gatewayClass"]),
        "invalid enrollment scope",
    )


def uuid4(value):
    try:
        parsed = UUID(value)
        checked(parsed.version == 4 and str(parsed) == value, "invalid Barbican Secret UUID")
    except (ValueError, TypeError, AttributeError):
        raise Rejected("invalid Barbican Secret UUID") from None
    return value


def fingerprint(value):
    return nonempty(value) and re.fullmatch(r"[0-9a-f]{64}", value)


def barbican_base(endpoint):
    try:
        checked(not re.search(r"[\x00-\x20\x7f?#]", endpoint), "invalid Barbican catalog endpoint")
        parsed = urlsplit(endpoint)
        hostname = parsed.hostname
        checked(
            parsed.scheme == "https" and hostname and "@" not in parsed.netloc
            and "%" not in hostname and not parsed.netloc.endswith(":")
            and (parsed.port is None or parsed.port > 0),
            "invalid Barbican catalog endpoint",
        )
        if parsed.netloc.startswith("["):
            ipaddress.IPv6Address(hostname)
        else:
            checked(dns_name(hostname.removesuffix(".")), "invalid Barbican catalog endpoint")
        mount = parsed.path.removesuffix("/").removesuffix("/v1")
        if mount:
            segments = mount.removeprefix("/").split("/")
            checked(all(re.fullmatch(r"[A-Za-z0-9._~-]+", part) and part not in (".", "..", "v1") for part in segments),
                    "invalid Barbican service path")
        return f"https://{parsed.netloc}{mount}/v1/secrets/"
    except (ValueError, TypeError):
        raise Rejected("invalid Barbican catalog endpoint") from None


def creation_receipt(scope, endpoint, response, uploaded_payload):
    check_scope(scope)
    base = barbican_base(endpoint)
    checked(fields(response, {"secret_ref"}) and nonempty(response["secret_ref"]) and response["secret_ref"].startswith(base),
            "Barbican creation response does not match the catalog endpoint")
    identifier = uuid4(response["secret_ref"].removeprefix(base))
    checked(bool(uploaded_payload), "empty PKCS#12 creation payload")
    return {"version": 1, "scope": scope, "barbicanSecretID": identifier, "payloadSHA256": hashlib.sha256(uploaded_payload).hexdigest()}


def public_key(key):
    return key.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)


def certificates(pem):
    position = 0
    while block := PEM_CERTIFICATE.match(pem, position):
        position = block.end()
    checked(position > 0 and not pem[position:].strip(), "invalid PEM certificate chain")
    return x509.load_pem_x509_certificates(pem)


def match_material(secret, bundle):
    checked(
        isinstance(secret, dict) and secret.get("apiVersion") == "v1" and secret.get("kind") == "Secret"
        and secret.get("type") == "kubernetes.io/tls",
        "expected a native Kubernetes TLS Secret",
    )
    metadata = secret.get("metadata")
    checked(isinstance(metadata, dict) and metadata.get("deletionTimestamp") is None, "missing or deleting Secret metadata")
    namespace, name = secret_key(metadata)
    uid, revision = metadata.get("uid"), metadata.get("resourceVersion")
    checked(nonempty(uid) and nonempty(revision), "missing Secret UID or resourceVersion")
    try:
        data = secret["data"]
        chain = certificates(base64.b64decode(data["tls.crt"], validate=True))
        key = serialization.load_pem_private_key(base64.b64decode(data["tls.key"], validate=True), None)
        stored_key, stored_leaf, stored_chain = pkcs12.load_key_and_certificates(bundle, None)
        checked(stored_key is not None and stored_leaf is not None and chain, "PKCS#12 or Secret lacks a key or leaf certificate")
        leaf_key = public_key(chain[0].public_key())
        checked(leaf_key == public_key(key.public_key()) == public_key(stored_key.public_key()),
                "the Secret and PKCS#12 private keys must match the leaf certificate")
        expected = [cert.fingerprint(hashes.SHA256()).hex() for cert in chain]
        observed = [cert.fingerprint(hashes.SHA256()).hex() for cert in (stored_leaf, *stored_chain)]
        checked(expected == observed and len(set(expected)) == len(expected), "the ordered certificate chains differ")
        for certificate, issuer in zip(chain, chain[1:]):
            certificate.verify_directly_issued_by(issuer)
    except Rejected:
        raise
    except (ValueError, TypeError, KeyError, InvalidSignature, UnsupportedAlgorithm):
        raise Rejected("invalid Secret or PKCS#12 certificate material") from None
    start = max(cert.not_valid_before_utc for cert in chain)
    end = min(cert.not_valid_after_utc for cert in chain)
    checked(start <= datetime.now(timezone.utc) < end, "certificate chain is outside its validity period")
    return {
        "namespace": namespace, "name": name, "uid": uid, "resourceVersion": revision,
        "chainSHA256": expected, "notBefore": timestamp(start), "notAfter": timestamp(end),
    }


def timestamp(value):
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def check_row(row):
    checked(fields(row, ROW_FIELDS), "invalid enrollment row")
    secret_key(row)
    checked(nonempty(row["uid"]) and nonempty(row["resourceVersion"]), "invalid Secret revision")
    uuid4(row["barbicanSecretID"])
    chain = row["chainSHA256"]
    checked(
        isinstance(chain, list) and chain and all(fingerprint(value) for value in chain) and len(set(chain)) == len(chain),
        "invalid certificate chain fingerprints",
    )
    try:
        start, end = (datetime.fromisoformat(row[field]) for field in ("notBefore", "notAfter"))
        checked(start.tzinfo is not None and end.tzinfo is not None and start < end, "invalid certificate validity period")
    except (ValueError, TypeError):
        raise Rejected("invalid certificate validity period") from None


def same_revision(left, right):
    return left is not None and right is not None and all(left.get(field) == right.get(field) for field in IDENTITY)


class Ledger:
    def __init__(self, path, scope):
        check_scope(scope)
        self.path = path
        self.scope = scope

    @contextmanager
    def locked(self):
        # Lock a separate inode that survives each atomic map replacement.
        lock_path = self.path.with_name(self.path.name + ".lock")
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "rb") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    def read(self):
        try:
            checked(stat.S_ISREG(self.path.lstat().st_mode), "working map must be a regular file")
            snapshot = read_json(self.path, MAX_MAP_BYTES)
        except FileNotFoundError:
            return {}
        checked(
            fields(snapshot, {"version", "scope", "revisions"}) and type(snapshot["version"]) is int
            and snapshot["version"] == 1 and snapshot["scope"] == self.scope and isinstance(snapshot["revisions"], list),
            "invalid map or scope mismatch",
        )
        rows = {}
        for row in snapshot["revisions"]:
            check_row(row)
            target = secret_key(row)
            checked(target not in rows, "ambiguous active row in map")
            rows[target] = row
        return rows

    def write(self, rows):
        data = json_bytes({"version": 1, "scope": self.scope, "revisions": list(rows.values())})
        checked(len(data) <= MAX_MAP_BYTES, "enrollment map exceeds 1 MiB")
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(dir=self.path.parent, delete=False) as temporary:
                temporary_path = Path(temporary.name)
                temporary.write(data)
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_path, self.path)
            dir_fd = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def enroll(self, secret, receipt, payload, previous=None):
        checked(
            fields(receipt, {"version", "scope", "barbicanSecretID", "payloadSHA256"})
            and type(receipt["version"]) is int and receipt["version"] == 1 and receipt["scope"] == self.scope,
            "invalid Barbican creation receipt or scope mismatch",
        )
        identifier = uuid4(receipt["barbicanSecretID"])
        checked(receipt["payloadSHA256"] == hashlib.sha256(payload).hexdigest(), "PKCS#12 bytes differ from the creation receipt")
        row = {**match_material(secret, payload), "barbicanSecretID": identifier}
        target = secret_key(row)
        with self.locked():
            rows = self.read()
            current = rows.get(target)
            if current == row:
                return row
            if current is not None or previous is not None:
                checked(same_revision(current, previous), "active row differs from the expected previous revision")
                checked(
                    (current["uid"], current["resourceVersion"]) != (row["uid"], row["resourceVersion"]),
                    "withdraw this revision before enrolling it with a different receipt",
                )
            rows[target] = row
            self.write(rows)
        return row

    def revoke(self, expected):
        target = secret_key(expected)
        uuid4(expected.get("barbicanSecretID"))
        with self.locked():
            rows = self.read()
            checked(same_revision(rows.get(target), expected), "active row differs from the revision to withdraw")
            del rows[target]
            self.write(rows)


def read_bytes(path, limit=MAX_INPUT_BYTES):
    with path.open("rb") as handle:
        data = handle.read(limit + 1)
    checked(len(data) <= limit, "input exceeds the size limit")
    return data


def read_json(path, limit=MAX_INPUT_BYTES):
    def unique(pairs):
        value = dict(pairs)
        checked(len(pairs) == len(value), "duplicate JSON member")
        return value

    def invalid_constant(_):
        raise Rejected("invalid JSON input")

    try:
        return json.loads(
            read_bytes(path, limit).decode("utf-8"), object_pairs_hook=unique, parse_constant=invalid_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        raise Rejected("invalid JSON input") from None


def json_bytes(value):
    return (json.dumps(value, indent=2, allow_nan=False) + "\n").encode("utf-8")


def revision_arguments(parser, prefix, required=False):
    parser.add_argument(f"--{prefix}uid", required=required)
    parser.add_argument(f"--{prefix}resource-version", required=required)
    parser.add_argument(f"--{prefix}barbican-secret-id", required=required)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--gateway-class", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    actions = parser.add_subparsers(dest="action", required=True)
    capture = actions.add_parser("capture-receipt", help="record the trusted Barbican create response and exact submitted bytes")
    capture.add_argument("--endpoint", required=True)
    capture.add_argument("--creation-response", type=Path, required=True)
    capture.add_argument("--pkcs12", type=Path, required=True)
    capture.add_argument("--output", type=Path, required=True)
    enroll = actions.add_parser("enroll", help="verify both native inputs and authorize a Secret revision")
    enroll.add_argument("--map", type=Path, required=True)
    enroll.add_argument("--secret-json", type=Path, required=True)
    enroll.add_argument("--receipt", type=Path, required=True)
    enroll.add_argument("--pkcs12", type=Path, required=True)
    revision_arguments(enroll, "previous-")
    revoke = actions.add_parser("revoke", help="withdraw one exact active revision")
    revoke.add_argument("--map", type=Path, required=True)
    revoke.add_argument("--namespace", required=True)
    revoke.add_argument("--name", required=True)
    revision_arguments(revoke, "", required=True)
    args = parser.parse_args()
    scope = {"cluster": args.cluster, "gatewayClass": args.gateway_class, "project": args.project, "region": args.region}
    os.umask(0o077)
    try:
        if args.action == "capture-receipt":
            receipt = creation_receipt(scope, args.endpoint, read_json(args.creation_response), read_bytes(args.pkcs12))
            with args.output.open("xb") as output:
                output.write(json_bytes(receipt))
                output.flush()
                os.fsync(output.fileno())
        elif args.action == "enroll":
            secret = read_json(args.secret_json)
            previous = None
            previous_revision = (args.previous_uid, args.previous_resource_version, args.previous_barbican_secret_id)
            if any(value is not None for value in previous_revision):
                checked(all(previous_revision), "provide the complete previous revision")
                checked(isinstance(secret, dict) and isinstance(secret.get("metadata"), dict), "missing Secret metadata")
                previous = dict(zip(IDENTITY, (*secret_key(secret["metadata"]), *previous_revision), strict=True))
            Ledger(args.map, scope).enroll(secret, read_json(args.receipt), read_bytes(args.pkcs12), previous=previous)
        else:
            expected = dict(zip(
                IDENTITY, (args.namespace, args.name, args.uid, args.resource_version, args.barbican_secret_id), strict=True,
            ))
            Ledger(args.map, scope).revoke(expected)
    except Rejected as error:
        parser.exit(1, f"{error}\n")
    except OSError:
        parser.exit(1, "Could not read an input or write the protected operator file.\n")


if __name__ == "__main__":
    main()
