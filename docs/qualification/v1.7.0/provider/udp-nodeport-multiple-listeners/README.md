# Weighted UDP NodePort traffic on two listeners in local kind

Gateway API v1.7.0 Standard and Experimental passed this local kind replay. The replay used the executable from signed controller `f233af8aa653` with `--octavia-udp-nodeport-packet-drop`, Octavia `460570fb9060` and Octavia Lib `735564bb8701`. The normal and no-cache linux/amd64 image builds yielded the same manifest. [Portable receipt](receipt.json)

A Gateway-wide UDPRoute with backend weights 3 and 1 won ports 9310 and 9312. Each port had a dedicated Octavia pool. An older, section-scoped UDPRoute won port 9311 and served as the witness.

| Channel | Converged phase | 9310 probe outcomes | 9312 probe outcomes | 9311 witness replies |
|---|---|---|---|---:|
| Standard | Baseline | 48 primary, 16 foreign | 48 primary, 16 foreign | 16 |
| Standard | Grant withdrawn | 48 primary, 16 timeout | 48 primary, 16 timeout | 16 |
| Standard | Grant restored | 48 primary, 16 foreign | 48 primary, 16 foreign | 16 |
| Standard | 9312 removed | 48 primary, 16 foreign | 64 timeout | 16 |
| Standard | 9312 restored | 48 primary, 16 foreign | 48 primary, 16 foreign | 16 |
| Experimental | Baseline | 48 primary, 16 foreign | 48 primary, 16 foreign | 16 |
| Experimental | Grant withdrawn | 48 primary, 16 timeout | 48 primary, 16 timeout | 16 |
| Experimental | Grant restored | 48 primary, 16 foreign | 48 primary, 16 foreign | 16 |
| Experimental | 9312 removed | 48 primary, 16 foreign | 64 timeout | 16 |
| Experimental | 9312 restored | 48 primary, 16 foreign | 48 primary, 16 foreign | 16 |

After grant withdrawal, each weighted pool reported a discard fraction of 1/4 with only the primary member active. On each port the nftables weighted-drop counter rose by 16 and the foreign Pod received zero datagrams. The direct foreign NodePort probe still reached that Pod. The route retained its UID, generation and backendRefs and reported `Accepted=True`, `ResolvedRefs=False/RefNotPermitted` and the controller's `Programmed=True` condition. Restoring the grant restored the 3:1 member weights and `ResolvedRefs=True`. Pod markers and counter deltas matched the converged probe outcomes.

Removing 9312 cleared its listener, pool, IPVS service and committed guard. Its 64 probes reached the guest's native input path, with zero Pod deliveries and zero weighted drops. Restoring 9312 created a new listener and pool. The Gateway and load balancer kept their identities, as did the 9310 and 9311 listeners, pools and members across the listener edits. Both UDPRoute specs stayed at generation 1. Pod and Service UIDs stayed the same, and the ready EndpointSlice address sets stayed the same.

| Channel | Transition | Ports sampled | Rounds from probe start through convergence signal | Rounds after convergence |
|---|---|---|---:|---:|
| Standard | Grant withdrawn | 9311 | 161 | 8 |
| Standard | Grant restored | 9311 | 155 | 8 |
| Standard | 9312 removed | 9310, 9311 | 120 | 8 |
| Standard | 9312 restored | 9310, 9311 | 124 | 8 |
| Experimental | Grant withdrawn | 9311 | 146 | 8 |
| Experimental | Grant restored | 9311 | 143 | 8 |
| Experimental | 9312 removed | 9310, 9311 | 119 | 8 |
| Experimental | 9312 restored | 9310, 9311 | 109 | 8 |

Each round sent one fresh IPv4 probe per listed port. Every sampled port replied in every round. The first round preceded the mutation. The probe signaled convergence after Kubernetes conditions, the active Octavia graph and guest IPVS and guard state agreed. The eight rounds after that signal also matched Pod markers. The transition probe sampled only the 9311 witness for changes to the grant. Both weighted ports were sampled in separate batches after grant convergence.

Kind ran a control plane and worker, ready EndpointSlices, kube-proxy and three UDP Services with `externalTrafficPolicy: Local`. Local Octavia WSGI, SQLite and TaskFlow used the pinned wheels and the real Amphora REST driver. The guest ran the Flask agent, systemd, Keepalived, IPVS and nftables on a preattached network using `backendSecurity.mode: Unmanaged`. Keystone, Neutron, RPC transport, Amphora inventory and ordinary CRUD completion were fixtures.

Standard used 3072 MiB of RAM and an 18 GiB virtual disk. Experimental used 2688 MiB of RAM and an 18 GiB virtual disk. Guest setup, scenario, launcher and campaign exited 0 on both channels.

The first run on each channel stopped before sending any probes because the fixture had skipped ReferenceGrant creation. Both runners exited 1. The completed replays used the same repaired fixture and signed binaries.

The host-memory preflight returned exit 75 before guest startup on Standard at 4992 MiB and on Experimental at 2816 MiB and 3072 MiB (2 attempts). The campaign skipped the runner in all four attempts.

This replay covers fresh IPv4 flows in the local fixture. Regional qualification, deployed Neutron enforcement, production Amphora images, mTLS, VRRP, IPv6, long-lived flows, load and upstream conformance remain outside this campaign. [Regional runs](../../live-runs.md) · [Operator configuration](../../../../operator-octavia-udp-packet-drop.md)
