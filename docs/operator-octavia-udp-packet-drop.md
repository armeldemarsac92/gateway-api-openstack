# Proportional UDP packet discard

Enable proportional discard on the Gateway controller in a region running the Amphora API, worker and data plane that implement `udp_packet_drop_v1`. The two flags are independent, default to off, and work on both Gateway API v1.7.0 channels.

| GatewayClass member mode | Controller flag |
| --- | --- |
| `Endpoint` | `--octavia-udp-packet-drop` |
| `NodePort` (default) | `--octavia-udp-nodeport-packet-drop` |

An eligible UDPRoute wins on exactly one listener of a dedicated UDP Gateway with an IPv4 VIP. Separate routes on different listeners can use the feature independently. Routes sharing one pool across several listeners and IPv6 Gateways use ordinary UDP forwarding.

The extension uses one dedicated round-robin pool per selected listener and requires an empty default before the first managed member batch. The controller omits the class's UDP-CONNECT monitor on these pools and uses EndpointSlice readiness. In NodePort mode it also selects ready IPv4 workers according to the Service's `externalTrafficPolicy` and verifies ingress rules and worker port associations when the class uses `backendSecurity.mode: Referenced`. Enabling either option can briefly interrupt an existing eligible listener while the controller replaces its stock pool and UDP-CONNECT monitor. The route's `Programmed` condition stays pending until Octavia reports the new pool active. The listener and Gateway keep their identities.

For backend weights 3 and 1, removing the ReferenceGrant for the second Service reserves one quarter of the traffic for discard. The valid Service continues to get three quarters and the unauthorized Service receives no new traffic once Octavia applies the batch. Missing Services and Services with no eligible endpoints also contribute their positive weights. A zero-weight invalid reference contributes only diagnostics. An all-unavailable pool has no real members and discards all traffic.

Check the route's `Accepted`, `ResolvedRefs` and `<controller-domain>/Programmed` conditions independently. A revoked reference yields `ResolvedRefs=False` while `Programmed=True` can confirm the valid members and packet discard share. Endpoint loss can leave `ResolvedRefs=True`. The route reports programmed only after the active pool reports the exact fraction and member set. To qualify a deployment, send fresh UDP flows before revoking a grant, after provisioning the new fraction and after restoring the grant. Count traffic at both Services and verify a separate listener during each phase.

If Octavia is still provisioning a batch, the controller can replace it with the most recent authorized members or remove the affected pool. It retries when Amphora is applying a change. A temporary capability discovery failure leaves a healthy pool serving while its status is pending. If that pool would still serve a revoked destination, the controller withdraws it.

When Neutron temporarily fails a NodePort security read, the controller defers the update and keeps the pool if its serving destinations are still authorized and its discard share already covers the requested share. It withdraws the pool when a serving destination has been removed, the requested discard share increases, or the security check establishes a failed prerequisite.

## Local traffic observations

On Standard and Experimental, a UDPRoute sent weight 3 to a local Service and weight 1 to a Service in another namespace. After the ReferenceGrant was withdrawn and the pool became active, 48 of 64 fresh flows reached the local Service. The kernel dropped the other 16 datagrams, and the cross-namespace Service received zero. Restoring the grant delivered 48 and 16 replies from the two Services on Standard, and 48 and 16 on Experimental. Removing the foreign Service's endpoints also dropped 16 of 64 datagrams. The second listener answered all 16 control probes in each stabilized phase, including after the first route was deleted.

The packet trial used a dedicated pool for each listener. A separate controller test returned HTTP 503 on every capability lookup, removed one of an ordinary pool's two listeners, and checked that its other listener stayed ready. The dedicated route waited for discovery and recovered without rewriting its pool. [Controller regression test](https://github.com/armeldemarsac92/gateway-api-openstack/blob/4418c52adff866a90d05fa8061c5405d5569bda3/internal/cloud/openstack/graph/multi_udp_status_test.go#L336) [Route status, backend counts and transition samples](qualification/v1.7.0/provider/udproute-packet-drop-evidence/receipt.json)

The experiment used Endpoint mode and ran one IPv4 Amphora agent with systemd, Keepalived, IPVS and nftables in a disposable VM. The Kubernetes API and echo Pods ran in kind. For a regional installation, install the matching Octavia API, worker and guard-capable Amphora image before advertising `udp_packet_drop_v1` for `amphora` in `[api_settings] enabled_udp_packet_drop_providers`. Keep `DIB_OCTAVIA_AMP_USE_NFTABLES` enabled on those images. Starting the guard sets `net.ipv4.vs.expire_nodest_conn=1` inside the Amphora network namespace. All IPVS services in that namespace share the setting, which remains enabled after removing a guarded listener. This allows a socket previously mapped to a removed IPVS destination to reach a remaining member. A production Amphora image, deployed Neutron rules and VRRP failover remain to be qualified. [Octavia packages and configuration](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-udp-packet-drop-f7c184099225)

## Disable the feature

Remove the flag for the class's member mode while the patched API and worker are available. The controller deletes the owned guarded pool and its default edge. A route with all active destinations available gets a stock pool with its class monitor. A route with an unavailable positive-weight backend stays withdrawn until its destinations are usable. A lost regional capability follows the same fallback. Verify the affected listener defaults and confirm that the guarded pools are gone before downgrading Octavia.
