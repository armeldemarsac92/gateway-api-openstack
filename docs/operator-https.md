# Operator-provisioned HTTPS

A tenant can use an HTTPS listener and an HTTPRoute on a private Amphora Gateway. The operator provisions a certificate in Barbican and approves the exact revision of a Kubernetes TLS Secret. With stock Octavia, use one `TERMINATED_HTTPS` listener per port and one certificate reference per listener. A region that supports HTTPS listener isolation can serve multiple hostnames on the same port. The Gateway can also serve plain HTTP on a different port.

In the stock configuration, two HTTPS listeners claiming the same port report `Conflicted=True`, `Accepted=False` and `Programmed=False`. Check `ResolvedRefs` on each listener for its certificate authorization. A missing cross-namespace grant reports `RefNotPermitted` even during the conflict. Move one listener to a free port and verify each backend with a new TLS connection.

## Share an HTTPS port

Upgrade the regional Octavia API, database, provider library, workers and Amphora agent with HTTPS listener isolation support. Enable `enabled_https_listener_isolation_providers = amphora` in Octavia's `[api_settings]`. Verify that the authenticated regional `GET /v2/lbaas/providers/amphora/gateway_capabilities` includes `https_listener_isolation_v1`. Use the standalone or Magnum shared-listener overlay to set `--octavia-https-listener-isolation` along with the operator certificate map. [Standalone overlay](../config/octavia-https-listener-isolation/kustomization.yaml) · [Magnum workload overlay](../integrations/magnum-capi-helm/workload-octavia-https-listener-isolation/kustomization.yaml)

Give each HTTPS section on the port a distinct hostname. Use an exact DNS name, a leading wildcard such as `*.example.test`, or one section with no hostname. All sections must reference the same authorized Secret in the same namespace and use its current enrolled certificate revision. The certificate must cover the frontend hostnames that clients will verify.

Amphora selects the most specific section for the TLS SNI and for each HTTP request authority. An exact name wins over a wildcard, the longest matching wildcard wins over a broader one, and the section with no hostname matches the remainder. If the two selections differ, that request receives HTTP 421. A selected section with no matching route returns HTTP 404. Each HTTPRoute retains its own hostname, path and namespace restrictions within the sections to which it is attached.

Check a successful request for each application and its `attachedRoutes` count. On one HTTP/2 connection, send a request for the TLS section, then for a different section, then for the first section again. Expect the application's response, HTTP 421 and the application's response respectively. Revoking the common certificate grant withdraws this entire port. During a disjoint hostname grant change, the controller can keep the port and its discovery EndpointSlice active after confirming the surviving certificate, route actions and native selectors. Overlapping hostnames, changed TLS credentials or incomplete evidence can require a temporary port shutdown. [Shared-port grant transitions](qualification/v1.7.0/provider/frontend-client-validation.md) Other independently authorized listener ports continue serving. [Local results on both channels](qualification/v1.7.0/provider/shared-https.md)

Removing sections keeps the last HTTPS section isolated on its original port. Removing the last section retires the closed Octavia listener after its owned policies are gone. If the controller cannot verify which section owns a policy, it closes the affected port until it can read the route and its policy again. The attachment count still reflects the routes attached to each section.

For rollback, first drain and remove the enrolled Gateway with the enabled controller and regional capability. Check the Octavia listener and policy inventory before turning off the option. A previously enrolled frontend remains closed if the operator disables isolation or a readback cannot confirm its section set.

### Delegate an HTTPS section to a ListenerSet

Delegated HTTPS is available on the Gateway API v1.7.0 Standard and Experimental bundles when the operator HTTPS and regional Amphora listener-isolation options are enabled. Set the Gateway's `allowedListeners` to admit the ListenerSet namespace. Use one core Secret reference per terminating listener. On a shared port, the Gateway and the ListenerSets must resolve to the same enrolled Barbican certificate. HTTPRoutes select a delegated section by setting their parent kind to `ListenerSet` and using that ListenerSet's name, namespace and section name.

The default certificate namespace is the ListenerSet namespace. For a cross-namespace Secret, create the ReferenceGrant in the Secret's namespace and set `spec.from.kind` to `ListenerSet` and `spec.from.namespace` to the ListenerSet namespace. Enrollment still uses the parent GatewayClass. A grant for a Gateway allows only the Gateway's own references.

Removing a ListenerSet's grant reports `ResolvedRefs=False` with `RefNotPermitted` and withdraws that section and its HTTPRoutes from the HTTPS frontend. Independently authorized sections keep their authorization. A shared port can temporarily close while Octavia retires obsolete policies. Restore the grant to enroll the section again using the existing certificate map entry.

## Enable the mode

The default controller has no Secret permissions. The optional component sets `GATEWAY_OPENSTACK_TLS_CERTIFICATE_MAP` and mounts a protected directory at `/etc/gateway-operator/certificates`. For a standalone installation, start your pinned-image overlay from [the HTTPS installation](../config/operator-https/kustomization.yaml). For Magnum, render [the five-object workload overlay](../integrations/magnum-capi-helm/workload-operator-https/kustomization.yaml) into the operator artifact and record its SHA-256 in the conductor configuration. The mapping ConfigMap and the per-Secret Role and RoleBinding belong to the workload operator. [Magnum installation](qualification/v1.7.0/magnum-operator.md)

```sh
kubectl kustomize config/operator-https
kubectl kustomize integrations/magnum-capi-helm/workload-operator-https > gateway-controller.yaml
sha256sum gateway-controller.yaml
```

Configure an authenticated region and a project-scoped OpenStack controller credential. The operator enrollment file names the same cluster ID, GatewayClass, region and project. The controller obtains the Barbican endpoint from the authenticated service catalog. The platform must provide a TLS-verified HTTPS key-manager endpoint and grant the Octavia service access to the immutable PKCS#12 payload. Protect write access to the operator namespace, enrollment files and controller workload. [Enrollment helper and input contract](../hack/operator-certificates/README.md) · [Barbican Secrets](https://docs.openstack.org/api-guide/key-manager/secrets.html)

Grant the workload ServiceAccount `get` for each approved Secret name. Create the following Role and RoleBinding in the namespace that owns the Secret. For a Magnum workload, change the ServiceAccount name to `magnum-openstack-gateway-controller`. Kubernetes RBAC allows payload reads with this verb. The resolver requests only object metadata and checks the UID and resourceVersion. [Kubernetes resource-name RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/#referring-to-resources)

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: gateway-web-certificate
  namespace: certificates
rules:
  - apiGroups: [""]
    resources: [secrets]
    resourceNames: [web-tls]
    verbs: [get]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: gateway-web-certificate
  namespace: certificates
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: gateway-web-certificate
subjects:
  - kind: ServiceAccount
    namespace: openstack-gateway-system
    name: openstack-gateway-controller
```

The operator enrollment helper verifies the Secret's leaf and private key against a project-scoped creation receipt and the actual Barbican payload, including the ordered intermediate chain. Keep the private inputs on the operator host. Publish the generated map from that host to the workload cluster.

```sh
kubectl -n openstack-gateway-system create configmap openstack-gateway-tls-revisions \
  --from-file=map.json="$OPERATOR_MAP" --dry-run=client -o yaml |
  kubectl apply --server-side --field-manager=gateway-certificate-operator -f -
```

The full ConfigMap directory is mounted read-only. After the kubelet projects a new map into the Pod, a subsequent HTTPS reconciliation reads it. Healthy and settled listeners check enrollment at least once per five-second polling interval, subject to work-queue and API latency. Kubelet projection, provider retries and listener provisioning also affect the time until a new connection sees the change. With multiple controller replicas, verify the expected map is available to each replica before a planned leadership change. [Kubernetes projected ConfigMap updates](https://kubernetes.io/docs/concepts/configuration/configmap/#mounted-configmaps-are-updated-automatically)

## Require a client certificate

Use the standalone or Magnum frontend validation overlay. Both include operator-provisioned HTTPS, add `--octavia-frontend-client-validation`, select Octavia API microversion 2.8 and grant read and watch access to core ConfigMaps. Custom overlays can select a later microversion. Continue using the regional Barbican project and the existing server certificate enrollment procedure. [Standalone overlay](../config/octavia-frontend-client-validation/kustomization.yaml) · [Magnum workload overlay](../integrations/magnum-capi-helm/workload-octavia-frontend-client-validation/kustomization.yaml)

To authenticate both clients and upstreams, add the backend TLS policy component to the frontend validation overlay. Both components share one Octavia microversion argument. Set a higher regional microversion in the final deployment overlay if required. [Backend TLS policy component](../config/components/octavia-backend-tls-policy/kustomization.yaml)

The controller watches ConfigMap metadata and reads CA contents directly after checking the Gateway reference. It publishes the CA as an immutable Barbican revision scoped to the Gateway. Octavia must have access to those CA revisions.

Gateway API applies `spec.tls.frontend` to listeners with `protocol: HTTPS`. A `TLSRoute` attached to a `protocol: TLS` listener in `Terminate` mode uses the server certificate in the listener's `tls.certificateRefs`. [Gateway API v1.7.0 listener targeting](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/geps/gep-91/index.md#L49-L61)

Put one PEM-encoded CA certificate in the ConfigMap's `ca.crt` key. Set the following fields under `Gateway.spec` to require the client certificate on every HTTPS port. Port 8443 uses its own CA.

```yaml
tls:
  frontend:
    default:
      validation:
        mode: AllowValidOnly
        caCertificateRefs:
          - group: ""
            kind: ConfigMap
            name: workforce-ca
    perPort:
      - port: 8443
        tls:
          validation:
            caCertificateRefs:
              - group: ""
                kind: ConfigMap
                name: partner-ca
                namespace: trust
```

The default mode is `AllowValidOnly`. A port override replaces the default, and every HTTPS section on the same port uses the same client trust. Plain HTTP ports are independent. For a cross-namespace CA, create a ReferenceGrant in the ConfigMap's namespace. In this example, the Gateway lives in `apps`.

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: ReferenceGrant
metadata:
  namespace: trust
  name: apps-partner-ca
spec:
  from:
    - group: gateway.networking.k8s.io
      kind: Gateway
      namespace: apps
  to:
    - group: ""
      kind: ConfigMap
      name: partner-ca
```

For an invalid CA reference, look for `ResolvedRefs=False`, `Accepted=False` with reason `NoValidCACertificate`, and `Programmed=False` on the affected listener. For a missing grant, `ResolvedRefs` reports `RefNotPermitted`.

`AllowInsecureFallback` lets clients connect without a certificate or with a certificate the listener cannot verify. The mode requires the same authorized CA as `AllowValidOnly` and the regional Octavia provider must advertise `frontend_client_auth_insecure_fallback_v1`. A Gateway configured with this mode reports `InsecureFrontendValidationMode=True` with reason `ConfigurationChanged`. If regional support is unavailable, affected listeners report `Accepted=False` with reason `UnsupportedValue` and stay closed. When changing a port back to `AllowValidOnly`, the controller confirms that Octavia has disabled fallback before reopening the port.

Rotating a CA closes the affected port while Octavia changes and verifies its native client authentication settings. Removing the ConfigMap, revoking its ReferenceGrant, or disabling the controller option while validation is still requested closes the port. If Octavia rejects the administrative shutdown because the attached CA cannot be read, the controller removes the HTTPS listener after verifying its policy and rule ownership. The port remains withdrawn until authorization returns. `Programmed=False` with reason `Pending` indicates that the controller is still waiting for Octavia to confirm closure. Restore the authorized configuration to rebuild or reopen the affected listener. To verify strict validation, open new HTTPS connections with a trusted client certificate, with no client certificate, and with a client certificate issued by a different CA. Only the first connection should complete. If you intentionally remove validation from a Gateway, Octavia clears the Gateway-owned CA on the closed listener before restoring ordinary HTTPS.

## Attach a tenant route

This example serves `app.example.test` on port 443. The certificate owner grants the Gateways in namespace `apps` access to the named Secret. A Secret in the Gateway namespace uses the same enrollment procedure and requires no ReferenceGrant.

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: ReferenceGrant
metadata:
  namespace: certificates
  name: apps-web-tls
spec:
  from:
    - group: gateway.networking.k8s.io
      kind: Gateway
      namespace: apps
  to:
    - group: ""
      kind: Secret
      name: web-tls
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  namespace: apps
  name: edge
spec:
  gatewayClassName: openstack
  listeners:
    - name: web
      protocol: HTTPS
      hostname: app.example.test
      port: 443
      tls:
        mode: Terminate
        certificateRefs:
          - name: web-tls
            namespace: certificates
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  namespace: apps
  name: web
spec:
  parentRefs:
    - name: edge
      sectionName: web
  rules:
    - backendRefs:
        - name: web
          port: 80
```

Use a supported Service and member configuration for the backend. Check the listener's current `Accepted`, `ResolvedRefs` and `Programmed` conditions, the route's parent status and the expected application response through the VIP. Use a new TLS connection with the expected SNI, hostname and trust root to check the certificate. [Supported HTTP backends](getting-started.md) · [Measured status, rotation and traffic](qualification/v1.7.0/provider/https-operator-ports.md)

## Rotate, revoke and roll back

1. Upload the new PKCS#12 through the trusted project-scoped workflow and preserve its creation receipt. Keep the old payload and Octavia's read permission while any listener still references it. Stock Amphora may need the object while rendering other listeners.
2. Write the new TLS Secret and enroll its exact UID and new resourceVersion. A metadata update can change the resourceVersion even when the certificate bytes stay the same. Use the operator helper to replace the old enrollment and publish the new map. The transition can interrupt fresh connections on that HTTPS port.
3. Confirm Octavia has completed provisioning and its enabled listener references the newly approved object. Verify a fresh application request, leaf fingerprint and chain on the wire. Retire the old Barbican object only after verifying it is unused by every consuming listener and operator.
4. To revoke a cross-namespace authorization, remove the matching ReferenceGrant or narrow its target. To revoke an enrollment, publish a map with that approved row removed. Check `ResolvedRefs=False` with `RefNotPermitted` for a missing grant or `InvalidCertificateRef` for a missing or stale enrollment. Check the affected port refuses a fresh connection and the other active ports still reach their expected backends. Track existing client connections separately during drainage.
5. If OpenStack is unavailable or its load balancer is stuck in `ERROR`, the controller cannot confirm withdrawal until the provider accepts it. Use the public listener, its provisioning state and a fresh TLS check as evidence. A Kubernetes status alone cannot establish when the data plane stopped serving.
6. Before disabling the mode or rolling back the image, migrate or drain HTTPS traffic while the current controller is running. Remove the HTTPS routes and listeners and verify withdrawal and ownership in Octavia. For an enrolled HTTP-family Gateway, follow the full distinct-port rollback procedure before installing an older controller. Keep certificate material available until that procedure finishes. [Gateway rollback](operator-recovery.md#roll-back-a-gateway-with-distinct-http-ports)

Automatic import of tenant keys, TLSRoute termination and client-certificate validation have their own qualification records. [Gateway API feature matrix](qualification/v1.7.0/matrix/named-contracts.md) · [Gateway API TLS types](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/gateway_types.go)

A dedicated Amphora Gateway can route TLS connections by exact SNI and let the applications terminate TLS. [TLS passthrough setup](operator-octavia-tls-sni-passthrough.md)
