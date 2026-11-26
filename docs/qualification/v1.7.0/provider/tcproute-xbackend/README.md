# Raw TCP to an EndpointSelector XBackend

An Experimental TCPRoute sent three shares of traffic to a Service and one to a same-named XBackend. The two origins returned distinct markers and echoed the original binary payload. A second listener sent all its connections to the Service.

| Pod state | Service echoes | XBackend echoes | Connections closed | Total |
| --- | ---: | ---: | ---: | ---: |
| Pod selected and ready | 192 | 64 | 0 | 256 |
| Selector label removed | 184 | 0 | 72 | 256 |
| Selector label restored | 192 | 64 | 0 | 256 |
| Pod unready | 194 | 0 | 62 | 256 |
| Pod ready again | 192 | 64 | 0 | 256 |

When the Pod was selected and ready, Octavia reported two members with weights 3 and 1. Removing its matching label or marking it unready removed its address from the pool and set TCP connection rejection to one in four. After each withdrawal converged, the origin accepted zero Gateway connections during the probes and answered a separate direct probe. Restoring eligibility restored the two members and their weights.

The routes kept their original UID and spec. The mixed TCPRoute continued reporting `Accepted=True`, `ResolvedRefs=True` and `Programmed=True` across the Pod changes. The XBackend reported `Accepted=True` for the Gateway. On both release channels the ordinary TCPRoute echoed all 16 control connections in every phase. The Standard API server omitted the XBackend CRD.

The earlier signed controller reported `ResolvedRefs=False/InvalidKind` for the Experimental reference. Its mixed listener sent 182 connections to the Service and closed 74. The XBackend origin received zero Gateway connections.

The run used the Kubernetes API server, the manager extracted from its signed-source OCI image, installed Octavia REST and SQLAlchemy, its TCP member-batch worker, the Amphora agent and a live HAProxy. Local fixtures supplied cloud identity, network reachability, remaining worker completions, agent transport and service control. The probes exercised an IPv4 EndpointSelector over raw TCP. ExternalHostname, backend TLS on TCPRoute and deployed Amphora networking require separate work. [Route conditions, pool weights and per-origin TCP echoes](receipt.json) · [Operator configuration](../../../../getting-started.md#forward-tcp-to-an-experimental-xbackend)
