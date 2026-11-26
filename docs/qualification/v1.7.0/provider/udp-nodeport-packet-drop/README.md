# Weighted UDP NodePort traffic in local kind

Both Gateway API v1.7.0 channels ran signed controller `a088a5f949d0` with `--octavia-udp-nodeport-packet-drop`. They used Octavia `460570fb9060` and Octavia Lib `735564bb8701`. The normal and no-cache linux/amd64 builds yielded the same manifest. Each replay used the manager extracted from the signed OCI archive. [Portable receipt](receipt.json) · [Image build](../../image.json)

| Channel | Phase | Target replies and timeouts | Weighted drops | Witness replies |
|---|---|---|---:|---:|
| Standard | baseline | 16 foreign, 48 primary | 0 | 16 |
| Standard | withdrawal | 48 primary, 16 timeout | 16 | 16 |
| Standard | restored | 16 foreign, 48 primary | 0 | 16 |
| Standard | weight-precision | 2 foreign, 512 primary | 0 | 16 |
| Experimental | baseline | 16 foreign, 48 primary | 0 | 16 |
| Experimental | withdrawal | 48 primary, 16 timeout | 16 | 16 |
| Experimental | restored | 16 foreign, 48 primary | 0 | 16 |
| Experimental | weight-precision | 2 foreign, 512 primary | 0 | 16 |

The same affected UDPRoute UID, generation and backendRefs persisted through grant removal and restoration. The active Octavia pool reported `1/4` discard during revocation while the route reported `ResolvedRefs=False/RefNotPermitted` and `Programmed=True`. Direct traffic to the foreign NodePort succeeded. The restored pool forwarded both configured shares.

| Channel | Grant change | First post-convergence sequence | Target replies and timeouts | Foreign Pod markers | Witness replies |
|---|---|---:|---|---:|---:|
| Standard | withdrawal | 44 | 12 primary, 4 timeout | 0 | 16 |
| Standard | restored | 45 | 4 foreign, 12 primary | 4 | 16 |
| Experimental | withdrawal | 44 | 12 primary, 4 timeout | 0 | 16 |
| Experimental | restored | 59 | 12 primary, 4 foreign | 4 | 16 |

The transition probe kept sending pairs during programming. Each window started after Kubernetes conditions, the active Octavia graph and the guest IPVS and guard state had converged. The receipt records the boundary, the number of programming pairs and the following 16 pairs.

With the option still enabled, changing only the primary route weight from 3 to 257 advanced the same route by one generation. Octavia and IPVS reported 256:1 on the same two worker InternalIP and NodePort pairs. The load balancer, both listeners, the witness pool and its member retained their IDs. On both channels the target pool and both members were replaced. Their worker address and NodePorts stayed the same. The precision sample began after convergence and used 514 distinct target IPv4 tuples and 16 witness tuples on each channel. Both echoed replies and markers from real Pods matched the Pod and nftables counter deltas.

The replay used a real kind control plane and worker, ready EndpointSlices, kube-proxy, and three UDP Services with `externalTrafficPolicy: Local`. Local Octavia WSGI and SQLite used the installed wheels. The TaskFlow worker called the real Amphora REST driver over tunneled HTTP. The guest ran the Flask agent, systemd, Keepalived, IPVS and nftables. Keystone, Neutron, RPC transport, Amphora inventory and ordinary CRUD completion were fixtures. The guest used a preattached network and `backendSecurity.mode: Unmanaged`.

This local qualification covers fresh IPv4 flows. Regional qualification remains at 0 of 14 profile/channel combinations. Deployed OpenStack, production Amphora images, Neutron security enforcement, mTLS, VRRP, IPv6, long-lived flows, load and upstream conformance remain outside this campaign. [Regional runs](../../live-runs.md) · [Operator configuration](../../../../operator-octavia-udp-packet-drop.md)
