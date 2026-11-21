# Cleartext HTTP/2 to Amphora pool members

An operator can enroll an Amphora provider for HTTP/2 prior knowledge on individual HTTP pools. The client-facing listener can continue to accept ordinary HTTP and gRPC over HTTP/2. The Service port declares `appProtocol: kubernetes.io/h2c`. [Octavia pool API and discovery](https://github.com/armeldemarsac92/octavia/blob/hibiscus/grpc-backend-h2c/api-ref/source/v2/provider.inc) · [HAProxy server-side HTTP/2](https://www.haproxy.com/documentation/haproxy-configuration-tutorials/protocol-support/http/#http2-over-http-h2c-to-the-server)

## Operator setup

1. Install the matching Octavia and octavia-lib builds. Apply database migration `61dc89ad2fb7` and upgrade the API servers, workers and Amphora fleet.
2. Add the provider name to `[api_settings] enabled_backend_h2c_providers` in the region. Check that its authenticated `GET /v2/lbaas/providers/{provider}/gateway_capabilities` response contains `backend_h2c_v1`.
3. For a standalone installation, use `config/octavia-backend-h2c` as the Kustomize base. For a Magnum workload artifact, render `integrations/magnum-capi-helm/workload-backend-h2c` and pin its checksum and the image digest in the conductor. Each overlay adds `--octavia-backend-h2c`. A native GRPCRoute with exact service and method matching also uses the operator's `--octavia-fixed-http-responses` flag and provider enrollment for fixed responses. [Magnum procedure](../magnum-operator.md) · [Native Routes and conditions](grpc-h2c-controller.md) · [Fixed responses](../../../operator-octavia-http-actions.md)

The h2c-only workload overlay rendered five resources through the Magnum 23.0.0 and magnum-capi-helm 1.4.0 qualification fixture, using parent chart 0.10.1 and Helm 3.19.4. The enrolled chart carried `--octavia-backend-h2c`; the unenrolled chart omitted the workload addon. [Driver integration](../magnum-driver.md)

## Provider traffic and rollback

Two local plaintext gRPC servers served both methods. The fixture used the public Octavia API, the stored graph, the Amphora worker renderer and HAProxy 3.0.19. With h2c enabled on pool A, the first method reached A and returned gRPC `OK` with its backend trailer. The second method returned HTTP 502 on pool B. Once B also selected h2c, the two calls reached their respective servers over one frontend connection, both with gRPC `OK` and the correct trailers. [Replay probe](probe-backend-h2c.py) · [Stock-provider control](stock-grpc.md#cleartext-backend-transport)

Removing a provider from enrollment stops advertising the capability. Existing pools keep their transport. In the local trial, clearing B with `backend_h2c: false` restored its original HTTP 502 on the gRPC-only server while A continued serving gRPC. Reconcile affected workloads and drain or clear these pools before rolling a fleet back. On MariaDB 11.4.13 a pre-existing HTTP pool retained its protocol and gained the default value `false`. A locked update read saw the latest committed transport after a prior transaction snapshot. [Live Hibiscus ledger](../live-runs.md)
