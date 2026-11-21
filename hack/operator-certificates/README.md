# Enrolling a certificate for HTTPS

An operator can authorize one revision of a Kubernetes TLS Secret for a terminating HTTPS listener. Each enrollment pairs the exact Secret UID and resourceVersion with an immutable Barbican PKCS#12 payload. Use the authenticated OpenStack project ID, the key-manager catalog region, the GatewayClass name and the controller's configured cluster ID.

## Collect the evidence

Run the helper on a protected Linux operator host with Python 3.11 or newer and `cryptography>=42`. Keep the inputs in a local directory writable only by the operator, with mode `0700` and a process umask of `077`. The TLS Secret JSON and PKCS#12 both contain private key material.

1. Use the authenticated provisioning session for the configured OpenStack project and region. Retain the successful Barbican create response containing `secret_ref` and the exact unencrypted PKCS#12 bytes submitted to that request. If the create response is missing or belongs to a different project, obtain a valid creation receipt through the approved provisioning process. A successful read through a Barbican ACL establishes access, so the creation session supplies the project proof.
2. Under that trusted workflow, retrieve the binary payload of the resulting Barbican Secret by its ID. Keep the retrieval separate from the submitted PKCS#12 so the helper can compare both. Octavia also needs permission to retrieve the stored certificate through its own service access.
3. Read the actual Secret from the intended cluster with an operator identity allowed to read its contents. Use the returned API object, including its UID and resourceVersion. For example, `kubectl -n certs get secret web -o json > "$evidence/secret.json"` writes the Secret JSON directly into the protected directory.

Use the following commands in Bash after setting the scope from the authorized environments. The helper processes local evidence. Its output files contain only identifiers, fingerprints and certificate dates.

```bash
binding=(python3 hack/operator-certificates/bindings.py
  --cluster "$CLUSTER_ID" --gateway-class "$GATEWAY_CLASS"
  --project "$PROJECT_ID" --region "$REGION")

"${binding[@]}" capture-receipt \
  --endpoint "$BARBICAN_CATALOG_ENDPOINT" \
  --creation-response "$evidence/create-response.json" \
  --pkcs12 "$evidence/submitted.p12" \
  --output "$evidence/creation-receipt.json"

"${binding[@]}" enroll \
  --map "$evidence/certificate-map.json" \
  --secret-json "$evidence/secret.json" \
  --receipt "$evidence/creation-receipt.json" \
  --pkcs12 "$evidence/retrieved.p12"
```

The creation receipt is an operator attestation from trusted provisioning artifacts. Run the CLI using inputs collected from the authenticated provider and cluster. The `capture-receipt` command checks the returned resource URI against the configured HTTPS catalog endpoint and records the SHA-256 of the exact submitted bytes. Enrollment checks the receipt scope and payload digest, then checks the native Secret type and the private keys against the leaf. It compares the leaf and each intermediate by SHA-256 of their DER encoding in sequence, verifies the issuer signatures between neighboring certificates and checks the validity of the whole chain.

## Publish and withdraw enrollments

Publish the generated map through the controller's protected, read-only volume. Restrict who can replace the source of the mount. A ConfigMap volume should expose the entire mounted directory so the kubelet can propagate updates to a running process. The controller accepts a readable projected file such as mode `0444` and opens the current path on each resolution. On Kubernetes, publication takes effect after the volume update reaches the controller.

Provide the optional Secret `get` permission for each enrolled Secret name in its own namespace using `resourceNames`. The controller requests only metadata. Kubernetes authorizes this as the ordinary Secret `get` verb, so that RBAC permission also grants full `get` access to the named Secret. An HTTPS Listener in another namespace also requires a matching ReferenceGrant from its Gateway namespace. The controller does not need Secret list or watch permission for enrollments.

The helper serializes writers using a stable sidecar lock and atomically replaces the working map. Use the same working path on one operator host. For different hosts, serialize publication through your deployment workflow. The working map supports one active row per namespace and Secret name.

To enroll a rotation when an active row exists, pass its previous UID, resourceVersion and Barbican ID to `enroll` using `--previous-uid`, `--previous-resource-version` and `--previous-barbican-secret-id`. The helper compares them to the active row before replacing it. Repeating an identical enrollment is idempotent. To bind a different Barbican payload to the same Kubernetes revision, first withdraw the existing row.

```bash
"${binding[@]}" revoke \
  --map "$evidence/certificate-map.json" --namespace certs --name web \
  --uid="$ACTIVE_UID" --resource-version="$ACTIVE_RESOURCE_VERSION" \
  --barbican-secret-id="$ACTIVE_BARBICAN_ID"
```

Republish the updated map after each change. A changed Kubernetes resourceVersion or recreated Secret UID requires enrollment before the new revision can serve HTTPS. Maintain Octavia access and retire old Barbican payloads through the approved provider workflow.

## Map format

The resolver accepts version 1 snapshots up to 1 MiB. The timestamps represent the latest `notBefore` and earliest `notAfter` in the ordered chain. Enrollment is valid at `notBefore` and expires at `notAfter`. The resourceVersion is an opaque value.

```json
{
  "version": 1,
  "scope": {
    "cluster": "local",
    "gatewayClass": "octavia",
    "project": "project",
    "region": "RegionOne"
  },
  "revisions": [
    {
      "namespace": "certs",
      "name": "web",
      "uid": "secret-uid",
      "resourceVersion": "opaque/revision-a",
      "barbicanSecretID": "11111111-1111-4111-8111-111111111111",
      "chainSHA256": [
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      ],
      "notBefore": "2026-01-01T00:00:00Z",
      "notAfter": "2027-01-01T00:00:00Z"
    }
  ]
}
```

The receipt uses the same scope and version with top-level `barbicanSecretID` and `payloadSHA256` fields. The controller builds the Barbican reference from the HTTPS catalog endpoint, and the mapped UUID. It preserves a catalog mount such as `/key-manager` and appends the v1 Secret collection once. [Barbican 23 service registration](https://github.com/openstack/barbican/blob/1efe16cd73b6e7d7c827aa8090665f2564de632e/devstack/lib/barbican#L236-L246)

```bash
python3 -B -m unittest discover -s hack/operator-certificates
```
