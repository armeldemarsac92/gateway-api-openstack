# Proportional UDP packet discard

Enable proportional discard on the Gateway controller in a region running the Amphora API, worker and data plane that implement `udp_packet_drop_v1`. The two flags are independent, default to off, and work on both Gateway API v1.7.0 channels.

| GatewayClass member mode | Controller flag |
| --- | --- |
| `Endpoint` | `--octavia-udp-packet-drop` |
| `NodePort` (default) | `--octavia-udp-nodeport-packet-drop` |

An eligible UDPRoute can win on one or more listeners of a dedicated UDP Gateway with an IPv4 VIP. Each winning port applies the same backend weights and discard fraction in its own pool. Other UDP routes on the Gateway can use the feature independently. IPv6 Gateways use ordinary UDP forwarding.

The extension uses one dedicated round-robin pool per selected listener and requires an empty default before the first managed member batch. The controller omits the class's UDP-CONNECT monitor on these pools and uses EndpointSlice readiness. In NodePort mode it also selects ready IPv4 workers according to the Service's `externalTrafficPolicy` and verifies ingress rules and worker port associations when the class uses `backendSecurity.mode: Referenced`. Enabling either option can briefly interrupt existing eligible ports while the controller replaces a stock pool and UDP-CONNECT monitor. A stock pool shared by multiple listeners is detached before the controller replaces it. A guarded pool from the previous controller stays in place when Octavia confirms its single listener association and the route still wins that port. Listeners and the Gateway keep their identities during pool replacement.

For backend weights 3 and 1, removing the ReferenceGrant for the second Service reserves one quarter of the traffic for discard. The valid Service continues to get three quarters and the unauthorized Service receives no new traffic once Octavia applies the batch. Missing Services and Services with no eligible endpoints also contribute their positive weights. A zero-weight invalid reference contributes only diagnostics. An all-unavailable pool has no real members and discards all traffic.

The discard fraction still uses the configured backend reference weights. The pool weights for the available destinations can use [UDP weight rounding](getting-started.md#forward-udp-to-a-service), with a tolerance of 0.390625 percentage points for any group of those destinations as a share of the available pool weight.

Check the route's `Accepted`, `ResolvedRefs` and `<controller-domain>/Programmed` conditions independently. A revoked reference yields `ResolvedRefs=False` while `Programmed=True` can confirm the valid members and packet discard share. Endpoint loss can leave `ResolvedRefs=True`. Each pool updates its fraction and member set together. Octavia applies changes across ports in separate operations, and the route reports programmed after every winning port confirms the exact active configuration. To qualify a deployment, send fresh UDP flows to every winning port before revoking a grant, after provisioning the new fraction and after restoring the grant. Count traffic at both Services and verify a separate listener during each phase.

If Octavia is still provisioning a batch, the controller can replace it with the most recent authorized members or remove the affected pool. It retries when Amphora is applying a change. A temporary capability discovery failure leaves a healthy pool serving while its status is pending. If that pool would still serve a revoked destination, the controller withdraws it.

When Neutron temporarily fails a NodePort security read, the controller defers the update and keeps the pool if its serving destinations are still authorized and its discard share already covers the requested share. It withdraws the pool when a serving destination has been removed, the requested discard share increases, or the security check establishes a failed prerequisite.

## Local traffic observations

On Standard and Experimental, a UDPRoute sent weight 3 to a local Service and weight 1 to a Service in another namespace. After the ReferenceGrant was withdrawn and the pool became active, 48 of 64 fresh flows reached the local Service. The kernel dropped the other 16 datagrams, and the cross-namespace Service received zero. Restoring the grant delivered 48 and 16 replies from the two Services on Standard, and 48 and 16 on Experimental. Removing the foreign Service's endpoints also dropped 16 of 64 datagrams. The second listener answered all 16 control probes in each stabilized phase, including after the first route was deleted.

The packet trial used separate routes with one listener each. Controller tests cover one route on two guarded ports and a separate listener through grant withdrawal and restoration, pool replacement and listener election changes. Traffic through a multiport route still needs deployment qualification. [Route status, backend counts and transition samples](qualification/v1.7.0/provider/udproute-packet-drop-evidence/receipt.json)

The experiment used Endpoint mode and ran one IPv4 Amphora agent with systemd, Keepalived, IPVS and nftables in a disposable VM. The Kubernetes API and echo Pods ran in kind. For a regional installation, install the matching Octavia API, worker and guard-capable Amphora image before advertising `udp_packet_drop_v1` for `amphora` in `[api_settings] enabled_udp_packet_drop_providers`. Keep `DIB_OCTAVIA_AMP_USE_NFTABLES` enabled on those images. Starting the guard sets `net.ipv4.vs.expire_nodest_conn=1` inside the Amphora network namespace. All IPVS services in that namespace share the setting, which remains enabled after removing a guarded listener. This allows a socket previously mapped to a removed IPVS destination to reach a remaining member. A production Amphora image, deployed Neutron rules and VRRP failover remain to be qualified. [Octavia packages and configuration](https://github.com/armeldemarsac92/octavia/releases/tag/hibiscus-udp-packet-drop-f7c184099225)

In NodePort mode, the two local Gateway API channels exercised grant revocation, restoration and weight rounding with the NodePort option enabled. [NodePort traffic and fixture scope](qualification/v1.7.0/provider/udp-nodeport-packet-drop/README.md)

## Disable the feature

Remove the flag for the class's member mode while the patched API and worker are available. The controller deletes the owned guarded pools and their default edges. A route with all active destinations available gets a stock pool with its class monitor, shared across its winning ports. A route with an unavailable positive-weight backend stays withdrawn until its destinations are usable. A lost regional capability follows the same fallback. Verify the affected listener defaults and confirm that the guarded pools are gone before downgrading Octavia.
