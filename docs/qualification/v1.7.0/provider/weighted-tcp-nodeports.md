# Weighted TCP NodePorts on stock Octavia

The manager extracted from the linux/amd64 OCI archive at source revision `41c3b27c85c459b55b017203b51328a61b5c7485` sent traffic for a TCPRoute with a 3:1 Service weight ratio to three simulated worker NodePorts. Independently initialized Gateway API v1.7.0 Standard and Experimental control planes gave the same observed distribution. [Standard receipt](weighted-tcp-nodeports-evidence/standard.json) · [Experimental receipt](weighted-tcp-nodeports-evidence/experimental.json)

The route referenced a primary Service with a ready endpoint on each of two workers and a secondary Service with a ready endpoint only on worker-a. Both Services used `externalTrafficPolicy: Local`. The actual Octavia v2 HTTP API returned one TCP listener with a single TCP `ROUND_ROBIN` default pool, no session persistence, and the three weighted members below. The current-generation TCPRoute parent conditions were `Accepted=True`, `ResolvedRefs=True`, `Programmed=True` and `ForwardingTarget=True`. HAProxy reported each member `UP` with `L4OK` and reported its configured and effective weight at the beginning and end of each series.

| Echo identity | Octavia member | Weight | Standard, 400 connections | Experimental, 400 connections |
| --- | --- | ---: | ---: | ---: |
| primary@worker-a | 198.51.100.11:32000 | 3 | 152 | 152 |
| primary@worker-b | 198.51.100.12:32000 | 3 | 151 | 151 |
| secondary@worker-a | 198.51.100.11:32001 | 2 | 97 | 97 |
| **Service totals** | **primary vs secondary** | **6:2** | **303:97** | **303:97** |

Each of the 400 connections had a distinct open/close cycle and sent a 256-byte payload. The echo returned the worker identity and the same 256 bytes. HAProxy recorded an unchanged process, matching load-balancer selection and session counts, and zero connection errors, retries or redispatches during each measurement. The observed Service shares were 75.75% and 24.25%. The initial Standard series used the earlier local manager at `84cdbd889c3b260f293aa41b750fae462998bba1` and recorded 152/151/97 before the health gate was added. The subsequent gated Standard check also saw 152/151/97. Both failed the first script's exact 150/150/100 sample assertion. The final replay requires exact Octavia and HAProxy member weights, verifies every payload and accepts up to eight connections of deviation per identity from the nominal 150/150/100 counts. The tolerance is a qualification criterion for this sample.

After deleting the TCPRoute and Gateway, each run verified an empty Octavia load-balancer, listener, pool and health-monitor inventory and a closed TCP VIP. The two Services and their EndpointSlices were still readable.

The control plane was Kubernetes v1.36.2 via envtest. Octavia 19.0.0 (`93689cb7701da65b5aad387bb824c477c64b0f2b`) and octavia-lib 3.12.0 (`bc03e1856b60517e1b75db4fdf9b3edf55f3a019`) used their tagged source trees. The stock Octavia WSGI API ran over loopback HTTP with SQLite and the Amphora renderer supplied the configuration consumed by HAProxy 3.0.19. The fixture supplied Keystone, Neutron, provisioning completion and the Amphora transport. It represented the workers with local TCP echo servers bound to the worker IP and NodePort pairs in an isolated network namespace. The evidence covers this single TCPRoute through reconciliation, the Octavia API and real HAProxy TCP traffic. This first run exercised simulated worker NodePorts. The follow-up below exercised kube-proxy on kind workers.

Both receipts record the OCI manifest and manager digests and the revision logged at startup. [OCI build record](https://github.com/armeldemarsac92/gateway-api-openstack/blob/1cefeb4523f9a5a5564663a40f89303405095c47/docs/qualification/v1.7.0/image.json) · [Reproduce the OCI build](https://github.com/armeldemarsac92/gateway-api-openstack/blob/1cefeb4523f9a5a5564663a40f89303405095c47/docs/qualification/v1.7.0/image-build.md)

The source replay asset `hibiscus-weighted-tcp-nodeport-41c3b27c85c4.tar.gz` contains the runner, extraction and replay instructions, and both receipts.

## TCP through kube-proxy on two kind workers

A separate run used the published controller `04bb56309d8a` on two Kubernetes v1.36.4 kind workers. Both Services used `externalTrafficPolicy: Local`. In each cluster the primary Service had one ready pod on each worker. The secondary Service had one ready pod on worker A. Local Octavia and native HAProxy forwarded connections through kube-proxy iptables to the echo pods. [Image and publisher](https://github.com/armeldemarsac92/gateway-api-openstack/actions/runs/45656381435)

| Channel | Primary on A | Primary on B | Secondary on A | Total |
| --- | ---: | ---: | ---: | ---: |
| Standard | 152 | 151 | 97 | 400 |
| Experimental | 151 | 152 | 97 | 400 |

The three members had weights 3, 3 and 2. Each new TCP connection verified the responding pod and its 256-byte payload. HAProxy counted the same connections with zero retries, redispatches or connection errors. After deletion, the VIP refused connections, all four Octavia resource collections were empty and the Services and their ready EndpointSlices still served the expected NodePorts. Keystone, Neutron, worker completion and Amphora transport were supplied by local fixtures. [Results for both channels](weighted-tcp-kind-nodeports/receipt.json)
