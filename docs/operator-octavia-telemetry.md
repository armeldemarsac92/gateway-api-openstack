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

The collector uses HTTP/1.1. The default sampling rate is 100%. It supports a service name, span name and W3C trace context. A collector with an applicable `BackendTLSPolicy` also requires the controller’s backend TLS option and the corresponding regional support. Custom exporter headers remain open. [Backend TLS setup](operator-octavia-backend-tls-policy.md)

## Add attributes to request spans

Enable `[api_settings] enabled_listener_otlp_span_attributes_providers = amphora` after upgrading the API, workers and Amphora agents with the corresponding tracing Lua script. Check that the region advertises `listener_otlp_tracing_v1` and `listener_otlp_span_attributes_v1`. Add attributes under `spec.tracing` on an Experimental policy.

```yaml
attributes:
  - name: deployment.environment.name
    sourceType: Literal
    literalValue: staging
  - name: release.channel
    sourceType: Header
    headerName: X-Release-Channel
  - name: upstream.result
    sourceType: Attribute
    attributeKey: http.response.status_code
```

For `Header`, Amphora reads the first field value from the request. If the request omits that header, Amphora uses the first response field of the same name. A missing field leaves the attribute absent. `Attribute` can copy `http.request.method`, `http.response.status_code`, `url.path` or `url.scheme`. The status code is an integer, the path excludes the query, and the scheme reflects the frontend connection. The output name `http.response.status_code` is reserved for the actual response code. Removing `attributes` restores the default span fields. [Gateway API attribute definitions](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apisx/v1alpha1/xtelemetrypolicy_types.go#L340)

Before rolling back the controller or Amphora image to a version without span attributes, remove `spec.tracing.attributes` from each affected policy while the compatible controller is running. Check every managed HTTP and terminated HTTPS listener in Octavia. Its `otlp_tracing` profile must omit `attributes` and its `otlp_tracing_status` must report `ACTIVE`. An intentionally disabled exporter reports `otlp_tracing: null` with the same acknowledgement. Then roll back the binaries.

If an older controller is already running with an attribute profile still present, restore the compatible controller to complete withdrawal. An Octavia operator can also clear `otlp_tracing` on the affected listeners and verify the acknowledgement on each one. Keep the collector reserved until every serving listener has acknowledged its update.

## Set base and parent sampling rates

Upgrade the Octavia database, API, workers and eligible Amphora agents, including the matching tracing Lua script. Enable `listener_otlp_sampling_v1` alongside `listener_otlp_tracing_v1` in the region. Run the controller release listed in the build record with `--octavia-telemetry-policy` to apply sampling to an Experimental Gateway. [Controller build](qualification/v1.7.0/image-build.md) · [Octavia packages and image requirements](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-otlp-sampling-dc6b95f72e4a)

To sample one in four new traces and half of the requests with a sampled upstream parent, add the following under `spec.tracing` on your policy.

```yaml
samplingRate:
  numerator: 1
  denominator: 4
parentBasedSampling:
  mode: Enabled
  samplingRate:
    numerator: 1
    denominator: 2
```

With parent-based sampling enabled, a valid unsampled parent produces no exported span. A sampled parent uses the parent rate, which defaults to 100%. Requests without a valid parent use the base rate. With parent-based sampling disabled, all requests use the base rate. A zero numerator disables sampling for that group. Fractions express a probability, so the count can vary from one batch to the next. [Gateway API sampling fields](https://github.com/kubernetes-sigs/gateway-api/blob/v1.7.0/apisx/v1alpha1/xtelemetrypolicy_types.go)

After removing the policy or its collector grant, a previously managed listener reports `otlp_tracing: null`. A status of `PENDING_UPDATE` or `ERROR` keeps the old collector reserved. A status of `ACTIVE` confirms the active HAProxy configuration on all current Amphora peers. The controller can then retire the collector. [Octavia listener contract](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-otlp-sampling-dc6b95f72e4a)

On Experimental, the collector received 68 spans for 128 requests at a base rate of one half. With a sampled parent and a parent rate of one half, it received 62 spans for 128 requests. Across the Experimental scenarios, all 304 requests returned HTTP 200 and the collector received 145 spans matching their downstream trace context. The random trace ID flag survived propagation. After the collector grant was removed, Octavia acknowledged the cleared exporter and the controller retired its pool. All 6 application probes during withdrawal returned HTTP 200. The first measured request after grant restoration produced a matching span. On Standard, both requests returned HTTP 200 with the original trace context and the policy CRD was absent. The first Experimental run stopped when the simulated Amphora reload transport rejected the new publication identifier. The successful retry used the same signed binaries and acknowledged the identifier after HAProxy activated the configuration. These observations use local kind, HAProxy and an OpenTelemetry Collector with simulated cloud infrastructure. [Conditions, propagated flags and collected spans](qualification/v1.7.0/provider/xtelemetry-sampling-evidence/receipt.json)

The local qualification exercises an HTTP collector on kind. Its report records the application response, Gateway and policy status, listener publication state and matching collector span before, during and after withdrawal of the cross-namespace grant. [Local evidence](qualification/v1.7.0/provider/gateway-telemetry-evidence/receipt.json)
