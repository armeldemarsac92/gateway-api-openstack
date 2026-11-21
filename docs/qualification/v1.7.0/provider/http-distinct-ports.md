# Distinct HTTP ports on one stock Amphora Gateway

A Gateway can serve plain HTTP on several ports of the same VIP. For example, use listener sections named `http` on port 80 and `http-alt` on port 8080. Set each HTTPRoute's `parentRefs[].sectionName` to the intended section. You can also specify `parentRefs[].port`. If you set both fields, they must select the same section. The route must resolve to one eligible section after applying the listener's namespace and hostname rules.

Octavia uses one HTTP listener per port. Routes on separate ports can match the same hostname and path and forward to different backends. Each route owns its forwarding policies and backend pool. An unmatched request has no default pool. Multiple sections can share a port when they have distinct exact DNS hostnames; they share one Octavia listener and keep their own Gateway status and attachment counts.

When you add another port to a bound HTTP Gateway, the controller adopts the existing listener in place. The load balancer, VIP, and established listener and route resources retain their IDs. Removing a port withdraws its policies and cleans up its owned resources after Octavia confirms the relationships. Other ports keep their policies and backend pools. The original binding port stays recorded so reconciliation can safely resume after a restart, even when that port has been removed.

The controller tests cover the explicit section selector from `GatewayWithAttachedRoutesWithPort8080`. The provider test drives local Octavia API responses and can render the resulting inventory with pinned Octavia templates and probe both ports using HAProxy. Stock-provider and upstream conformance results are recorded separately.
