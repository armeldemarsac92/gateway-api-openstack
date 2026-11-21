# Operator-provisioned HTTPS

A tenant can use an HTTPS listener and an HTTPRoute on a private Amphora Gateway. The operator provisions a certificate in Barbican and approves the exact revision of a Kubernetes TLS Secret. The controller creates a stock Octavia `TERMINATED_HTTPS` listener with that certificate. Use one HTTPS listener per port and one certificate reference per listener. The Gateway can also serve plain HTTP on a different port.

When two HTTPS listeners claim the same port, both report `Conflicted=True`, `Accepted=False` and `Programmed=False`. Check `ResolvedRefs` on each listener for its certificate authorization. A missing cross-namespace grant reports `RefNotPermitted` even during the conflict. Move one listener to a free port and verify each backend with a new TLS connection.

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

Several HTTPS sections on the same physical port, automatic import of tenant keys, TLSRoute termination, client-certificate validation and shared-frontend SNI/authority isolation require separate implementation or qualification. [Gateway API TLS types](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/gateway_types.go) · [Octavia public listener API](https://docs.openstack.org/api-ref/load-balancer/v2/index.html)

A dedicated Amphora Gateway can route TLS connections by exact SNI and let the applications terminate TLS. [TLS passthrough setup](operator-octavia-tls-sni-passthrough.md)
