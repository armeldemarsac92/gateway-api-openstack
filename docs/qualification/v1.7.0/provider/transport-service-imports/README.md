# TCPRoute and UDPRoute traffic to a Headless ServiceImport

The route sent three shares to a local Service and one share to a same-named Headless ServiceImport. The two backends had independent grants. Each sample opened 256 fresh TCP connections or sent 64 UDP datagrams. A separate route received 16 control probes in each phase.

| Controller | Channel | Protocol | Import grant | Local replies | Imported replies | Closed or dropped | Control replies | ResolvedRefs |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| Earlier build | Standard | TCP | Present | 185 | 0 | 71 | 16 | False/InvalidKind |
| Earlier build | Standard | UDP | Present | 48 | 0 | 16 | 16 | False/InvalidKind |
| Earlier build | Experimental | TCP | Present | 203 | 0 | 53 | 16 | False/InvalidKind |
| Earlier build | Experimental | UDP | Present | 48 | 0 | 16 | 16 | False/InvalidKind |
| Transport build | Standard | TCP | Present | 192 | 64 | 0 | 16 | True/ResolvedRefs |
| Transport build | Standard | TCP | Withdrawn | 188 | 0 | 68 | 16 | False/RefNotPermitted |
| Transport build | Standard | TCP | Restored | 192 | 64 | 0 | 16 | True/ResolvedRefs |
| Transport build | Standard | UDP | Present | 48 | 16 | 0 | 16 | True/ResolvedRefs |
| Transport build | Standard | UDP | Withdrawn | 48 | 0 | 16 | 16 | False/RefNotPermitted |
| Transport build | Standard | UDP | Restored | 48 | 16 | 0 | 16 | True/ResolvedRefs |
| Transport build | Experimental | TCP | Present | 192 | 64 | 0 | 16 | True/ResolvedRefs |
| Transport build | Experimental | TCP | Withdrawn | 184 | 0 | 72 | 16 | False/RefNotPermitted |
| Transport build | Experimental | TCP | Restored | 192 | 64 | 0 | 16 | True/ResolvedRefs |
| Transport build | Experimental | UDP | Present | 48 | 16 | 0 | 16 | True/ResolvedRefs |
| Transport build | Experimental | UDP | Withdrawn | 48 | 0 | 16 | 16 | False/RefNotPermitted |
| Transport build | Experimental | UDP | Restored | 48 | 16 | 0 | 16 | True/ResolvedRefs |

The earlier signed mirror build reported InvalidKind for the import. The transport build accepted the reference on both channels. Removing the import grant kept the route accepted and programmed, changed ResolvedRefs to False/RefNotPermitted and preserved the import’s configured share as closed TCP connections or dropped UDP datagrams. Recreating the grant restored replies from the import without editing either route.

The TCP trial used a Kubernetes API server, the signed controller, the installed Octavia and Amphora packages, HAProxy and separate TCP echo applications. The UDP trial used a local kind cluster with echo Pods, Keepalived/IPVS and nftables. Kernel drop counters and Pod counters were checked against the datagrams sent. The receipt identifies each signed build and both Gateway API bundles. Reachability across a real Hibiscus ClusterSet still requires a regional trial. [Route conditions, counters and artifact hashes](receipt.json) · [Operator configuration](../../../../operator-multicluster-service-imports.md#forward-tcproute-connections-and-udproute-packets)
