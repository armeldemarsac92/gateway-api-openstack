# Stock Hibiscus shared HTTPS certificate rotation

One stock Amphora `TERMINATED_HTTPS` listener served eight successful requests across two certificate revisions on the same loopback VIP and port. An operator provisioned the two PKCS#12 bundles through public Barbican 23.0.0. Octavia 19.0.0 took the replacement through the public listener `PUT`, and its stock worker produced the configuration and PEM uploads used by HAProxy. [Probe](stock-https-rotation.py) · [Hibiscus Barbican release](https://releases.openstack.org/hibiscus/index.html#barbican) · [Hibiscus Octavia release](https://releases.openstack.org/hibiscus/index.html#octavia)

The provider graph models the common certificate in `same-namespace-with-https-listener`. The upstream Gateway has four HTTPS sections on port 443, all referring to `gateway-conformance-infra/tls-validity-checks-certificate`. The probe used an ephemeral port, two generated one-day leaves with identical SANs, one default TLS reference and one HTTP pool. Each request opened a new TLS connection and verified the chain, exact leaf SHA-256 and the backend's HTTP 200 response with the original Host and path. Named connections also verified the hostname. [Pinned upstream Gateway](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/base/manifests.yaml#L30-L89)

| Upstream section hostname | Client SNI | Before update | After update |
|---|---|---|---|
| Unspecified | None | Leaf A, HTTP 200 | Leaf B, HTTP 200 |
| `second-example.org` | `second-example.org` | Leaf A, HTTP 200 | Leaf B, HTTP 200 |
| `*.wildcard.org` | `third-example.wildcard.org` | Leaf A, HTTP 200 | Leaf B, HTTP 200 |
| `fourth-example.wildcard.org` | `fourth-example.wildcard.org` | Leaf A, HTTP 200 | Leaf B, HTTP 200 |

The recorded leaf digests were `270d8db7889da0dcb9d3546cd97c349d52f1dc8741f4649314959d149492e60c` for A and `8ab71c7043d1895ab423406700712ca647fec65df777ad0d0130e8463f0d918b` for B. [Execution receipt](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-0b512ec28ae055d6e1eb1460dc2a43e7b87c441f/stock-https-rotation-2b432ee4cb23.txt)

After the stock WSGI application completed `PUT /v2/lbaas/listeners/{id}` with HTTP 200, the probe discarded the response and reported a simulated loss to the caller. A fresh in-process WSGI client made a `GET` with the listener ID returned at creation. The listener port, protocol and pool matched the original, and `default_tls_container_ref` matched the expected replacement exactly. The sequence used one certificate update and a readback in the same process. Resource discovery, ownership validation, a second update and restart recovery remain outside this evidence. The tagged Octavia handler returns 200. The public API reference documents 202 and directs clients to poll `PENDING_UPDATE` until the provisioning status resolves. [Tagged listener update handler](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/controllers/listener.py#L625-L691) · [Octavia listener update API](https://docs.openstack.org/api-ref/load-balancer/v2/index.html#update-a-listener)

Public Barbican reads returned both original bundles byte for byte after the switch. The probe sent a public Octavia cascade delete request and simulated worker completion by setting `DELETED` directly in the fixture. Public GETs then returned HTTP 404 for the load balancer and listener. The operator deleted both Barbican secrets through the real public API and verified HTTP 404 for each one.

An operator-assisted mapping for Kubernetes Secret revisions is under study. It covers certificate identity, key ownership, grant withdrawal and the four same-port listener sections. [Proposed mapping and acceptance criteria](https-certificate-mapping.md)

## What the probe exercised

Barbican ran as a separate loopback WSGI process with SQLite and its `store_crypto` adapter and `simple_crypto` plugin. The real discovery, secret and ACL APIs ran with their stock policy. The fixture omitted Keystone's token-validation filter and supplied synthetic confirmed project identities. The operator had the `member` role and the Octavia certificate reader had the `reader` role. Octavia's Barbican ACL handler used the real ACL and secret APIs through python-barbicanclient. The test substituted the Keystone service session, service user ID and delegated user client. Both services used fixture configuration with host configuration discovery disabled. Generated private keys and secret references stayed in the private fixture.

Octavia used the upstream API test fixture with no-auth, SQLite, simulated Neutron and messaging, and manually advanced provisioning states. The probe restored the real `BarbicanCertManager` in place of the test fixture's mock. The worker's real Amphora HAProxy driver rendered from the persisted Octavia graph. A simulated Amphora REST agent received PEM and configuration uploads and recorded the reload call. HAProxy validated and ran the uploaded bytes in two successive local processes. The run covers public API state and fresh connections under each rendition. Live Octavia worker convergence, in-place Amphora reload, existing connection continuity, Gateway reconciliation and upstream Core qualification remain open. [Barbican secret API](https://docs.openstack.org/api-guide/key-manager/secrets.html) · [Tagged Barbican certificate manager](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/certificates/manager/barbican.py) · [Tagged Amphora driver](https://github.com/openstack/octavia/blob/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/amphorae/drivers/haproxy/rest_api_driver.py)

## Transfer to Gateway qualification

For this common-certificate shape, a controller trial can resolve the Kubernetes Secret used by the four sections, associate each revision with an operator-provisioned Barbican reference, and update one terminating listener. Preserve each section's attachment and status rules when building the HTTP graph. Validate listener and load balancer ownership independently and, on an ambiguous response, fetch the listener by its recorded ID and compare the readback with the intended reference. Before retiring any certificate, require the matching reference, `ACTIVE` listener provisioning status and a fresh TLS handshake that serves the expected replacement leaf. Separate evidence is needed for hostname routing precedence, multiple distinct certificates, real Keystone roles and ACLs, and the full upstream setup and case bodies on each channel.

The run leaves certificate creation and deletion with the fixture operator. Controller-managed uploads and deletion still need a defined project scope, role and ACL contract, revision ownership, cleanup rules for interrupted operations and evidence that a revision has no remaining consumers. Keep any trusted provider references private and use placeholders in checked-in examples.

## Reproduce

Use Linux with unprivileged user namespaces and Python 3.14. Build HAProxy 3.0.19 with OpenSSL using the [stock gRPC build instructions](stock-grpc.md#reproduce) and set `HAPROXY_BIN` to its absolute path. Run from the repository root with an existing private, writable scratch directory in `HTTPS_WORK_DIR`. The probe opens only ephemeral loopback ports. [Pinned Hibiscus constraints](https://github.com/openstack/requirements/blob/bbeadfcdd6cd8ac9abf888661d3fc65fa45c7480/upper-constraints.txt)

```sh
curl -fsSLo "$HTTPS_WORK_DIR/constraints.txt" \
  https://raw.githubusercontent.com/openstack/requirements/bbeadfcdd6cd8ac9abf888661d3fc65fa45c7480/upper-constraints.txt
python3.14 -m venv "$HTTPS_WORK_DIR/venv"
"$HTTPS_WORK_DIR/venv/bin/python" -m pip install -c "$HTTPS_WORK_DIR/constraints.txt" \
  'barbican==23.0.0' 'octavia==19.0.0' 'octavia-lib==3.12.0' \
  'python-barbicanclient==7.6.0' grpcio pyroute2 fixtures oslotest WebTest
unshare --user --map-root-user --net sh -c '
  ip link set lo up &&
  unshare --user --map-user=65534 --map-group=65534 "$1" -Bu \
    docs/qualification/v1.7.0/provider/stock-https-rotation.py --haproxy "$2" --work-dir "$3"
' sh "$HTTPS_WORK_DIR/venv/bin/python" "$HAPROXY_BIN" "$HTTPS_WORK_DIR"
```

The passing command reports one test, two distinct leaf SHA-256 digests, readback by the listener ID returned at creation, unchanged original revisions and the Octavia and Barbican cleanup checks. The tested environment used python-barbicanclient 7.6.0, SQLAlchemy 2.0.52, cryptography 43.0.3 and HAProxy `3.0.19-ade9c1262` with OpenSSL 3.5.8.
