# Recover from provisioning and cleanup failures

This guide covers network prerequisites and a Gateway, HTTPRoute, TCPRoute or UDPRoute that cannot finish cleanup.
The controller stops finalization when an OpenStack request fails or ownership cannot be proved.
That behavior protects resources owned by another controller or cluster.

Keep the controller and its original OpenStack access available while you investigate.
Do not remove its finalizers, edit identity annotations, or delete resources by a matching name.
Those actions bypass the checks that make cleanup safe.

## Check the current failure

Start with the Kubernetes object that is still deleting.
Review its conditions, Events, and controller logs.
The public messages are deliberately brief and do not include OpenStack response bodies.

```sh
kubectl -n <namespace> describe gateway <name>
kubectl -n <namespace> describe httproute <name>
kubectl -n <namespace> describe tcproute <name>
kubectl -n <namespace> describe udproute <name>
kubectl -n openstack-gateway-system logs \
  deployment/openstack-gateway-controller --all-containers
```

Confirm these settings before changing anything:

- The controller name is the same value that created the resources.
- The cluster ID has not changed.
- The credential is scoped to the original OpenStack project.
- The configured region still selects the same Octavia and Neutron endpoints.
- The Gateway API v1.7.0 CRDs come from the channel selected by the controller. The default is Standard.
  A bundle mismatch blocks provisioning and repair. Finalization can continue while the required Kubernetes APIs are served.
- Octavia and Neutron are reachable, the project has enough quota, and the controller credential can read and delete the resources it owns.

Authentication, authorization, quota, rate limiting, and service failures are different conditions.
Restore the original access or service health and let the controller try again.
A new credential is suitable only when it is scoped to the same project and has the required permissions.

For TCP or UDP, compare the listener's `default_pool_id`, the load balancer's listener and pool IDs, and the listed child resources. A mismatch yields `DependencyNotReady` and retains the binding for another observation. An `ERROR` listener on an `ACTIVE` load balancer allows withdrawal of a proven default. If the listener stays in `ERROR` after its default clears, route cleanup holds the pool until the listener recovers. A normal deletion of the whole Gateway can remove that verified listener before finishing cleanup. Keep the controller running and check Octavia's provisioning status. [Provider behavior and regressions](../internal/cloud/openstack/graph/tcp_graph_test.go)

A Gateway enrolled in multi-listener UDP retains its original listener port in the frozen binding, including after it shrinks to one listener. A controller from before this feature rejects the new binding and leaves the resources in place. To roll back to that controller, clean up the enrolled Gateway with a version that understands the binding, wait for deletion to finish, then recreate the Gateway with a single UDP listener. Before enrollment the controller seals the frozen port on each bound UDPRoute. Older controllers also hold those route bindings, including if enrollment is interrupted. Keep the newer controller running until cleanup finishes. Editing the binding annotations by hand leaves the checkpoints invalid.

## Roll back a fixed private Gateway address

Keep the newer controller running while preparing each Gateway. Record its observed private VIP, remove `spec.addresses`, and wait for `Programmed=True` at the new generation with the same VIP. Verify that its existing routes still serve traffic. Then roll back the controller. The previous controller treats an explicit fixed request as an invalid Gateway and can withdraw its owned graph. Clearing the request leaves future replacement load balancers eligible for automatic allocation.

A local two-version reader test stored the newer controller’s bound Gateway, cleared the address, and reconciled that exact serialized object with the previous controller. Its binding and observed VIP survived. A deployed rollback and live traffic remain outstanding. [Reader test and results](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-455798411c13fd42f5c1568357f2c35cec2b2169/hibiscus-private-ipv4-455798411c13.tar.gz)

## Roll back a Gateway with several HTTP sections

Use this procedure for Gateways that have only used one HTTP port. The earlier controller rejects a Gateway with two HTTP sections on that port and can start deleting its owned Octavia graph. Keep the newer controller running while migrating. Record the VIP, all hostnames, their accepted routes, their allowed namespaces and an identifiable backend response for each active host. Choose which single section and port will stay on the original Gateway.

Move traffic for each additional hostname to a separately supported Gateway or retire the hostname with its owner. Preserve its namespace authorization and verify its backend through the destination that clients will use. When the additional paths have been migrated or retired, reduce the original Gateway to its surviving section. Wait for Accepted and Programmed at the new generation, the expected attached route count and the original VIP. Verify the surviving and migrated hostnames before reverting the controller image. If the deployment cannot preserve the required reachability and authorization with one section per Gateway, retain the newer version.

The two-version readback kept the surviving VIP, route and member port after this migration. [Tested singleton downgrade](qualification/v1.7.0/provider/http-disjoint-sections.md#verified-singleton-downgrade)

## Rotate or withdraw a terminating HTTPS certificate

Keep the current controller running during withdrawal. Check the certificate on a fresh TLS connection, the stock listener's enabled and provisioning state, and the sibling ports. Leave old immutable Barbican objects available until every listener has released them. [Recovery, certificate rotation and downgrade](operator-https.md#rotate-revoke-and-roll-back)

## Roll back a Gateway with distinct HTTP ports

Complete the controller rollout before adding a second HTTP port. Once an HTTP Gateway enrolls in per-port bindings, those bindings persist even after its spec returns to a single port. The previous controller rejects the enrolled Gateway and route bindings. Its Gateway status can lose the reported address while the old provider graph still exists. [Two-version readback](qualification/v1.7.0/provider/http-distinct-ports.md#rollback)

1. Keep the current controller and its original cloud access available. Record the affected Gateway and route UIDs, VIP, ports, authorization, expected backend responses and the owned cloud resource IDs. Record the identities of your unrelated control resources for comparison.
2. Move traffic to a supported destination or drain it. Verify the migrated host and path through the destination clients use. Delete or detach the scoped routes and wait for the current controller to clean up their forwarding and owned resources.
3. Delete the enrolled Gateway and wait for Kubernetes finalization. Use the [ownership audit](#run-the-ownership-audit) and cloud readback to verify that the recorded Octavia and Neutron resources owned by the old graph are gone and that unrelated control resources are still present. Keep controller-managed annotations and finalizers intact.
4. Revert the controller and recreate the Gateway with one supported HTTP port and its corresponding routes. Check the new address, current-generation status and application responses. Plan for a possible endpoint or VIP change. Reuse of the old VIP remains unverified.

## Roll back HTTPRoutes with additional parents

The controller at `455798411c13fd42f5c1568357f2c35cec2b2169` requires one item in `spec.parentRefs`. Before rolling back, list routes that use this controller alongside other parents. Record which parents actually serve traffic and check the hostname, path and backend through each active entry point. If an extra reference is unused, remove it while the newer controller is running and verify the managed path. If another controller serves through an extra reference, migrate that attachment to a separately named HTTPRoute using its supported configuration and verify both paths before removing it from the original. Account for age-based selection where routes overlap. Keep the current image if the attachments cannot be migrated while preserving traffic. The previous image can withdraw this controller's Octavia fragment as soon as it encounters multiple references. [Controller and qualification](qualification/v1.7.0/provider/http-one-managed-parent.md)

## HTTPRoute hostname rollback

Before restoring a controller that accepts at most one declared hostname, inspect HTTPRoutes with multiple declarations. Reduce each affected route to the single hostname that preserves its effective match and its precedence on this listener. Verify the intended backend after the change. Rolling back with several declarations still present causes the older reconciler to withdraw that route. [Hostname selection and local traffic](qualification/v1.7.0/provider/http-host-intersection.md)

## Multiport TCP upgrades

Complete the controller rollout before adding a second TCP listener. Verify that the old controller pods have terminated and all cloud operations from that rollout have settled. The new controller saves the original listener port in the Gateway and in bound TCPRoutes before it starts managing individual ports. A Gateway keeps that record when it returns to one TCP listener. An identity-only Octavia read can discover the untagged original listener but cannot verify its port without that record. It reports `Progressing` and leaves the graph untouched.

In a separate replay of four enrolled TCPRoute records and their two Gateways per channel, the current controller validated the exact captured bindings. The previous weighted controller reported an ownership conflict for both Gateway and route records. [Binding replay](qualification/v1.7.0/provider/tcp-upstream.md#multiple-tcp-listeners-on-one-gateway)

A local compatibility test used the previous weighted-TCP controller `ce6f51797fb7`. It accepted the former singleton binding and refused to send Gateway updates, Gateway deletion or route cleanup to the provider once presented with an enrolled Gateway. A separately injected cleanup request with an old route record reached the recording provider on the much earlier controller `4aca0f993323`. Keep the current controller until cleanup completes. [Prior weighted controller](https://github.com/armeldemarsac92/gateway-api-openstack/commit/ce6f51797fb7c31a63c1f63577cb6938ecaff618)

A TCPRoute selector edit can retain a listener that the route still wins if the current Services and destinations are authorized and Octavia confirms the existing pool and members. Ports removed from the selection are withdrawn. New associations wait for the current observed generation and forwarding checkpoint. Before a graph write the controller requires explicit pool and member collections on every Octavia page. A missing or null collection suspends mutation. Immediately after the provider read and planning, it rechecks the Gateway and route inputs before applying the next mutation. [Provider graph](https://github.com/armeldemarsac92/gateway-api-openstack/blob/00a9799650bc03d4e96dc80afa4b5437573be183/internal/cloud/openstack/graph/graph.go) · [Route observation](https://github.com/armeldemarsac92/gateway-api-openstack/blob/00a9799650bc03d4e96dc80afa4b5437573be183/internal/controller/l4route/graph.go)

Before downgrading an enrolled TCP Gateway, drain its routes and delete the Gateway using a controller that recognizes its port records. Wait for Kubernetes finalization and compare Octavia's listener and pool inventory in both directions. Recreate it as a single-listener Gateway on the older version. Leave the frozen annotations and finalizers under controller ownership. Grant or namespace revocation can withdraw forwarding before deletion. The old and new listener endpoints should be tested with fresh application connections. [Operator procedure](getting-started.md#forward-tcp-to-a-service)

## Network prerequisites

Before rolling a weighted TCPRoute back to a single-backend controller, reduce its rule to exactly one valid Service reference while the newer controller is running. A second reference with weight zero still counts as a second reference. Wait for the current generation to report the selected Service as programmed and verify it answers a fresh TCP connection. In a local reconciliation check, the previous controller read a bound 70/30 Endpoint-mode TCPRoute, reported `ResolvedRefs=False/UnsupportedValue`, and marked it for pool withdrawal. [Previous controller source](https://github.com/armeldemarsac92/gateway-api-openstack/blob/00eabae770f40014af8cb6cb53a225f1450ac52a/internal/controller/l4route/backend.go)

For TCPRoute and UDPRoute, the `<controller-domain>/ForwardingTarget` parent condition records a digest of the bound Service UID, its selected port and the Octavia destinations. For an enrolled UDP or TCP Gateway, it also records the listener ports assigned to the route. `Pending` means the controller is preparing a target change. `Observed` accompanies provider-confirmed forwarding. During a Gateway API bundle mismatch, the controller can retain an existing target while its binding, authorization and current destinations still match the observed digest. A released NodePort, replacement Service, changed members or missing checkpoint triggers withdrawal. Restoring the selected Gateway API bundle lets reconciliation resume. [L4 target checks](../internal/controller/l4route/forwarding.go)

For provisioning blocked by referenced security or cloud capabilities, use the [read-only diagnostic workflow](getting-started.md#inspect-network-prerequisites).
Compare its current class template with the Gateway's frozen configuration before changing operator-managed groups or worker associations.
The controller retries failed external prerequisites slowly and does not repair referenced rules.

Detached owned Floating IPs recover automatically only when the full graph is valid, the exact VIP association is known, and Neutron supports revision-conditional updates.
A foreign association, inconsistent fixed IP, or missing load balancer stops repair.
Keep the original binding and investigate with the ownership audit.

## Run the ownership audit

`openstack-gateway-audit` compares durable Gateway, HTTPRoute, TCPRoute and UDPRoute bindings with resources found in the authenticated OpenStack project.
It only lists and gets resources.
It does not patch Kubernetes, adopt OpenStack resources, delete anything, or remove a finalizer.

Build the tool from the same reviewed source tree as the controller when a release bundle is not available:

```sh
make build-audit VERSION="$(git describe --always --dirty)"
```

Use the controller name, cluster ID, cloud entry, region, and Octavia microversion from the affected installation.
The command follows the usual kubeconfig loading rules.
Use `--kubeconfig` and `--context` when the default context is not the affected cluster.

```sh
umask 077
bin/openstack-gateway-audit \
  --controller-name=example.net/gateway-api-openstack \
  --cluster-id=<stable-cluster-id> \
  --kubeconfig=<path-to-kubeconfig> \
  --context=<context-name> \
  --clouds-yaml=<path-to-clouds.yaml> \
  --openstack-cloud=<cloud-name> \
  --openstack-region=<region> \
  --octavia-microversion=<microversion> \
  > ownership-audit.json
```

When `--clouds-yaml` is omitted, the command uses `OS_CLIENT_CONFIG_FILE` when it is set.
Otherwise it reads the standard OpenStack authentication environment variables.
`--openstack-cloud` defaults to `OS_CLOUD` and is required whenever a clouds.yaml path is selected.
`--openstack-region` defaults to `OS_REGION_NAME`.
The Kubernetes identity needs list access to Gateways, HTTPRoutes, TCPRoutes and UDPRoutes.
The OpenStack credential needs read access to the relevant Octavia and Neutron APIs.

The command writes only one JSON document to standard output.
Errors go to standard error.
It exits with status 0 after writing a report, even when the report has findings.
Pass `--fail-on-findings` to receive status 2 after a valid report with findings.
Do not use this status to trigger cleanup.
The JSON schema is experimental.
Configuration, authentication, or read failures return status 1 without a report.

The report contains OpenStack resource IDs and Kubernetes namespaces, names, and UIDs for local investigation.
It also contains the cluster ID and controller name.
Keep the file private and remove those identifiers before sharing it outside the operations team.

## Read the report

The `assessment` field describes the Kubernetes evidence used for the comparison:

- `complete` means every durable binding detected by the command was valid.
- `incomplete` means at least one object had missing or conflicting binding metadata.
  OpenStack orphan candidates cannot be evaluated safely in this state.

A complete assessment does not prove that every desired OpenStack resource exists, that no resource leaked, or that deletion is safe.
Kubernetes and OpenStack are listed separately, so state may change between observations.
Let reconciliation settle and run the command again before drawing a conclusion.

Each OpenStack resource has one disposition:

| Disposition | Operator meaning |
| --- | --- |
| `matched` | The resource identity and observed parent match a current binding. This says nothing about the completeness of the rest of the graph. |
| `orphanCandidate` | The resource has this controller's complete scope identity, but no current valid binding matches it. Investigate its history. This is not permission to delete it. |
| `staleUID` | Object names match a current binding, but an immutable Kubernetes UID differs. Check whether an object was deleted and recreated. |
| `ownershipConflict` | Identity, project, parent, or cardinality evidence conflicts. Stop manual mutation and resolve the source of the conflict. |
| `unresolved` | The available observations do not support a safe conclusion. Gather the missing parent or identity evidence. |

The `kubernetesIssues` list identifies objects whose stored binding could not be used.
Restore binding metadata only from an authoritative backup for the same Kubernetes object UID.
If installation configuration changed, restore the exact controller name, cluster ID, region, and project scope used by that installation.
Never construct a binding from resource names or copy annotations from another object.

## Let the controller resume

After correcting credentials, permissions, quota, service health, or the original binding configuration, leave the object and its finalizers in place.
The controller will observe the graph again and continue cleanup in dependency order.
Run the audit again after the Kubernetes object finishes deletion or the failure condition changes.

This project does not provide a supported manual cleanup command.
If normal reconciliation cannot be restored, preserve the report and controller logs and open a focused recovery issue with identifiers removed.
Any manual OpenStack change needs its own review and ownership proof immediately before mutation.
A saved audit report is not sufficient.

## Discovery limits

Review the audit's [discovery limits](design/architecture.md#discovery-limits) before interpreting an empty report.
Keep recovery scoped to resources whose ownership can be proved, and never use the report for broad project cleanup.
