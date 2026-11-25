# ClusterTrustBundle frontend and backend TLS

A named Kubernetes `ClusterTrustBundle` supplied trust for a Gateway HTTPS listener
and a `BackendTLSPolicy` targeting the TLS Service used by an HTTPRoute. All six
steps passed on both Gateway API v1.7.0 CRD channels. The upstream registry classifies
`ClusterTrustBundle` as Experimental Extended and has no registered case for it.
This supplemental result leaves the recorded totals at 167 of 170 registered
cases per channel and 55 of 64 named features with all their registered cases
passing on both channels. [Local receipt and digests](cluster-trust-bundle-evidence/receipt.json) ·
[Named contracts](../matrix/named-contracts.md)

| Bundle state | Standard | Experimental |
|---|---|---|
| Absent | Listener and policy report `InvalidCACertificateRef` | Same result |
| Create with client CA A and the server CA | Client A negotiates TLS 1.3 and receives 200 from `tls-backend`, which echoes SNI `abc.example.com`. Client B fails TLS. | Same result |
| Replace client CA A with B | Client B negotiates TLS 1.3 and receives 200 from the same workload with the expected SNI. Client A fails TLS. | Same result |
| Remove the server CA | Client B negotiates TLS 1.3, then receives 503. Client A still fails TLS. | Same result |
| Restore the server CA | Client B receives 200 from `tls-backend` with SNI `abc.example.com`. Client A still fails TLS. | Same result |
| Delete the bundle | Listener and policy report `InvalidCACertificateRef`. A fresh connection from client B closes. | Same result |

With a valid reference, the listener and policy both reported `Accepted=True`
and `ResolvedRefs=True`, and Octavia reported `MANDATORY` client authentication.
At each of the four states with a published bundle, the test read the native
Octavia listener and two TLS pools, followed their CA references through Barbican
over TLS and compared the SHA-256 fingerprints of the DER certificates to the
bundle. The receipt preserves the observed listener `Programmed` condition at
each sample. Successful traffic and CA readback could precede a status update
from `False/Pending`.

The test used a disposable Kind cluster per channel, native Octavia REST and
database, native Barbican, the Amphora renderer and the pinned HAProxy executable.
It sent traffic to the upstream TLS echo workload. Keystone, Neutron, worker and
agent transport, and Pod network relays used local fixtures. The traffic result
is limited to the exercised Gateway HTTPS and Service BackendTLSPolicy over
HTTPRoute paths. Live Hibiscus and the other Gateway profiles remain outside this
run. [Operator configuration](../../../operator-cluster-trust-bundle.md)

## Repeat the traffic check

Use the signed source and `linux/amd64` controller archive named in the receipt.
The normal and independent no-cache builds produced the same OCI manifest and
controller binary. To verify the release archive and extract the exact binary,
use the accompanying `oci.py` and `image.json` assets. Match the Gateway API
bundle, provider packages and HAProxy binary to the receipt for the local replay.

```sh
python3 oci.py controller-linux-amd64.oci.tar ./controller --expect image.json
```

1. Create separate Kubernetes 1.36.4 Kind clusters for Standard and Experimental.
   Use the corresponding v1.7.0 installation YAML at the pinned Gateway API
   revision. Enable the native bundle resource in each Kind configuration.

   ```yaml
   featureGates:
     ClusterTrustBundle: true
   runtimeConfig:
     certificates.k8s.io/v1beta1/clustertrustbundles: "true"
   ```

   Confirm that the controller ServiceAccount can `get`, `list` and `watch`
   cluster-scoped `clustertrustbundles.certificates.k8s.io`. The Experimental
   overlay additionally enables the Experimental APIs used by the controller.

2. Use Amphora with `enabled_https_listener_isolation_providers = amphora`,
   `enabled_backend_tls_hostname_providers = amphora` and
   `enabled_gateway_action_providers = amphora` in Octavia's `[api_settings]`.
   Use a build that supports strict backend DNS validation. Configure the
   operator HTTPS certificate map with the approved frontend Secret revision.
   The controller can read the map via `GATEWAY_OPENSTACK_TLS_CERTIFICATE_MAP`.
   Start the manager with the selected channel and these settings from the
   local scenario.

   ```sh
   ./controller --gateway-api-channel="$channel" \
     --gateway-api-cluster-trust-bundle --octavia-frontend-client-validation \
     --octavia-https-listener-isolation --octavia-backend-tls-policy \
     --octavia-fixed-http-responses --octavia-microversion=2.8 \
     --leader-elect=false --metrics-bind-address=0 --health-probe-bind-address=0 \
     --openstack-resync-interval=2s --openstack-poll-interval=50ms \
     --openstack-api-qps=120 --openstack-api-burst=160 --openstack-operation-timeout=30s
   ```

   [Frontend client validation and certificate enrollment](../../../operator-https.md#require-a-client-certificate) ·
   [Service BackendTLSPolicy](../../../operator-octavia-backend-tls-policy.md)

3. Install the `tls-backend` Deployment and Service from the pinned Gateway API
   `conformance/base/manifests.yaml`. Enroll a CA-signed backend leaf with DNS SAN
   `abc.example.com` as the TLS echo workload's serving certificate. Issue two
   client leaves with the client-auth extended key usage and authority key
   identifiers from independent roots. Use distinct subjects for both client
   roots and the backend root, and distinct serial numbers. The corrected fixture
   kept the upstream backend CA and generated two independent client CAs. The
   original fixture gave unrelated roots the same subject and serial, so
   OpenSSL could reject a valid leaf depending on bundle order. Keep signing
   keys outside the cluster-scoped object.

4. Use Endpoint member mode. Create a Gateway with an HTTP listener on port 80
   and an HTTPS listener on port 443 for `example.org` using the approved serving
   Secret. Set `spec.tls.frontend.default.validation.caCertificateRefs` to a
   named native bundle. Attach an HTTPRoute to the Gateway and forward to
   `tls-backend:443`. Set a Service-targeted BackendTLSPolicy with validation
   hostname `abc.example.com` and the same CA reference. Omit `namespace` on both
   cluster-scoped references. [Reference example](../../../operator-cluster-trust-bundle.md#use-a-named-clustertrustbundle-for-gateway-tls)

5. Start with the bundle absent and wait for the invalid-reference conditions.
   Create the bundle with the two public roots for the first state, then replace
   `spec.trustBundle` and delete the object in the order above. At each step,
   wait for Kubernetes conditions and Octavia CA references to converge. Use fresh
   client connections, verify the frontend certificate, and send `GET /trust`
   with Host and SNI `example.org`. Probe once with each client certificate.
   Check the HTTP code, the backend echo and the two independent client outcomes.
   Use authorized Barbican reads to fingerprint the published CA payloads.
   Compare the sets of SHA-256 certificate fingerprints without depending on PEM order.
