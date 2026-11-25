# Export Gateway request spans to OpenTelemetry

An operator can attach an Experimental `XTelemetryPolicy` to a Gateway and send its HTTP request spans to a Kubernetes Service. The controller configures the Gateway’s HTTP and terminated HTTPS listeners in Amphora. The collector receives OTLP/HTTP on its Service port. [Gateway API v1.7.0 tracing contract](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apisx/v1alpha1/xtelemetrypolicy_types.go)

Install the Gateway API v1.7.0 Experimental bundle and the controller’s Experimental workload overlay. Enable `--octavia-telemetry-policy` on the controller. The region must advertise `listener_otlp_tracing_v1`. To enable the capability, install the corresponding Octavia API, migration, worker, driver library and Amphora image, then set `[api_settings] enabled_listener_otlp_tracing_providers = amphora`. The collector Service needs a ready backend reachable from Amphora. [Octavia changes](https://github.com/armeldemarsac92/octavia/pull/29) · [Driver library](https://github.com/armeldemarsac92/octavia-lib/pull/19)

The following example targets the Gateway named `public` in the `apps` namespace. The owner of `observability` grants access to the collector Service named `otelcol`.

```yaml
apiVersion: gateway.networking.x-k8s.io/v1alpha1
kind: XTelemetryPolicy
metadata:
  name: gateway-traces
  namespace: apps
spec:
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: Gateway
      name: public
  tracing:
    mode: Enabled
    serviceName: apps/public
    spanName: Gateway HTTP request
    provider:
      backendRef:
        group: ""
        kind: Service
        namespace: observability
        name: otelcol
        port: 4318
---
apiVersion: gateway.networking.k8s.io/v1
kind: ReferenceGrant
metadata:
  name: allow-apps-traces
  namespace: observability
spec:
  from:
    - group: gateway.networking.x-k8s.io
      kind: XTelemetryPolicy
      namespace: apps
  to:
    - group: ""
      kind: Service
      name: otelcol
```

Inspect the policy’s `status.ancestors` entry for the target Gateway. It reports acceptance and whether the collector reference is permitted and resolved. The Octavia listener exposes its own `otlp_tracing_status`. `PENDING_UPDATE` means Amphora has yet to finish programming the exporter. `ACTIVE` records successful configuration publication and TLS preparation. To check delivery, look for the request’s trace ID in the collector. A collector TLS preparation failure sets the exporter to `ERROR` while the application listener continues serving. [Provider API](https://github.com/armeldemarsac92/octavia/blob/5edd5b1402fd22de33a13fc84d6cc0ab9f38b593/api-ref/source/parameters.yaml#L2074)

This mode uses HTTP/1.1 to the collector and full sampling. It supports a service name, span name and W3C trace context. A collector with an applicable `BackendTLSPolicy` also requires the controller’s backend TLS option and the corresponding regional support. Custom exporter headers, partial sampling, explicit parent-based sampling rules and custom span attributes remain open. [Backend TLS setup](operator-octavia-backend-tls-policy.md)

The local qualification exercises an HTTP collector on kind. Its report records the application response, Gateway and policy status, listener publication state and matching collector span before, during and after withdrawal of the cross-namespace grant. [Local evidence](qualification/v1.7.0/provider/gateway-telemetry-evidence/receipt.json)
