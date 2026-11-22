# ListenerSet admission on an owned Gateway

When `spec.allowedListeners` is absent or explicitly selects `None`, the Gateway continues to serve its own listeners and reports zero attached ListenerSets. A ListenerSet referring to this Gateway receives `Accepted=False` and `Programmed=False`, both with reason `NotAllowed`. The controller watches the parent and publishes the current generation when its references change.

| Registered v1.7.0 case | Standard | Experimental |
| --- | --- | --- |
| `ListenerSetDefaultNotAllowed` | PASS | PASS |
| `ListenerSetAllowedNamespaceNone` | PASS | PASS |

The original tests ran against a Kubernetes 1.36.2 API server and etcd with the corresponding v1.7.0 CRDs. The fixture supplies a ready Pod status and a recording OpenStack provider. Separate checks exercised parent deletion and replacement, and verified that a different controller keeps ownership of its ListenerSet status. These checks cover Kubernetes admission status. Traffic from accepted ListenerSets and peer conflicts require additional cases.

The v1 ListenerSet API is discovered when the manager starts. Restart the manager if that API is installed later. The [release archive](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-3c8dd9883713c57ac152ebfb12210ec9eecbe6ca) contains the exact controller image, checksums, recorded outcomes and replay command. [Machine-readable outcomes](listenerset-rejection-results.json)
