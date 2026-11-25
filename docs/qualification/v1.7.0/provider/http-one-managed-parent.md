# HTTPRoute with one managed Gateway parent

An HTTPRoute can attach to one supported Gateway managed by this controller while also referencing a Gateway that is absent, belongs to another controller, or has an unknown GatewayClass. The controller uses the managed reference's namespace, listener section and port wherever it appears in `spec.parentRefs`. It preserves parent statuses written by other controllers, including the conditions' generation and transition time. It writes status for Gateways whose class selects this controller and removes its own obsolete entries. [Gateway API status contract](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/apis/v1/shared_types.go)

In the single-parent implementation tested here, a second managed Gateway or two references to different sections of that Gateway received `Accepted=False/UnsupportedValue`. A later controller added per-Gateway bindings. On both channels the original HTTPRoute and GRPCRoute multiple-Gateway cases passed all six checks, including traffic through each Gateway. [Multiple-Gateway qualification](multiple-gateways.md)

A rollback to controller source `455798411c13fd42f5c1568357f2c35cec2b2169` restores the requirement for exactly one item in `spec.parentRefs`. The predecessor rejects mixed-parent routes and can remove their existing Octavia fragments. Before rollback, identify which additional parents serve traffic. Migrate any live attachment and verify both paths before reducing the original spec to one reference. [Operator rollback procedure](../../../operator-recovery.md#roll-back-httproutes-with-additional-parents)

## Local qualification

Kubernetes 1.36.2 ran the existing controller status suite on both genuine Gateway API v1.7.0 release bundles and their admission policies. The runner uses Endpoint member mode because the unmodified upstream `infra-backend-v1` Service is ClusterIP. It uses the upstream test bodies and manifests and a recording provider. [Envtest runner](../../../../internal/controller/envtest_upstream_status_test.go)

| Original upstream case | Predecessor Standard | Predecessor Experimental | Standard | Experimental |
| --- | --- | --- | --- | --- |
| `GatewayNameMaximumLength` | PASS | PASS | PASS | PASS |
| `HTTPRoutePreserveForeignStatus` | FAIL | FAIL | PASS | PASS |

The predecessor used the same status runner and failed the initial managed-parent acceptance assertion with `Accepted=False/UnsupportedValue`. The passing source accepted the managed parent, let the upstream case seed a foreign status, and reconciled to the new Route generation while retaining that entry exactly. These runs exercise Kubernetes status and controller reconciliation. Live OpenStack, packet forwarding and complete conformance profiles remain unqualified by this run. [Original HTTPRoute case](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/httproute-preserve-foreign-status.go)

The controller regression exercises both reference orders, including an existing foreign Gateway, and checks the exact managed reference, foreign status equality and a single provider graph bound to the managed Gateway UID. It also covers two managed Gateways, two sections of the same managed Gateway, and a previously unknown GatewayClass appearing before a graph mutation. [HTTPRoute regressions](../../../../internal/controller/httproute/httproute_managed_parent_test.go)

To repeat the status run, set `KUBEBUILDER_ASSETS` to the Kubernetes 1.36.2 envtest binary directory and `GATEWAY_API_SOURCE` to the v1.7.0 Gateway API checkout.

```sh
for channel in standard experimental; do
  rtk env USE_EXISTING_CLUSTER=false KUBECONFIG=/dev/null \
    KUBEBUILDER_ASSETS="$KUBEBUILDER_ASSETS" \
    GATEWAY_API_CHANNEL="$channel" GATEWAY_API_CRD_PATH="$GATEWAY_API_SOURCE/config/crd/$channel" \
    go test -json -tags=envtest -count=1 -timeout=5m \
    -run '^TestControllerEnvtest$/^(genuine_Gateway_API_release_bundle|upstream_Gateway_status_cases)$' \
    ./internal/controller > "$channel.jsonl" 2>&1
done
```

## Forwarding through the managed parent

The two captured routes referred to a missing Gateway on opposite sides of their managed reference. The controller produced one graph for Gateway `edge`. The test sent that graph through Gophercloud's loopback REST fixture and Octavia 19's stock Amphora renderer. HAProxy 3.0.19 returned 200 for both `/api` and `/api/extra` on `API.Example.COM:1234`. The echo backend received the original Host including the port. Four unbound host or path combinations returned the fixture's 503. This exercise used Kubernetes objects in memory and a loopback backend. The two-channel status tests used real Kubernetes API servers. [Observed traffic and status](http-one-managed-parent-results.json) · [Reproduction patch and recorded graph](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-f85a33968895b1c14a6eb5b5d911435c34b0f79e/hibiscus-http-parent-qualification-f85a33968895.tar.gz)
