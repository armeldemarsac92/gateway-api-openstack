# TCP, UDP and TLS routes attached to two explicit Gateways

A TCPRoute and a UDPRoute each sent traffic from two VIPs to the Service named in the route. Removing one parent stopped that VIP from reaching the backend. The retained Gateway continued serving and kept its Octavia listener and pool. Each phase includes three consecutive TCP and UDP probes after the forwarding state converged. [Standard](tcp-udp-standard.json) · [Experimental](tcp-udp-experimental.json)

A passthrough TLSRoute on two VIPs reached the same echo Pod for `abc.example.com`. The Pod saw that SNI, and the client verified the backend certificate. After removal, TLS connections on the retired parent failed before a certificate was presented. The remaining VIP served three more TLS requests. Both listeners excluded an unmatched name under `*.example.com` and a name outside that suffix. [Standard](tls-standard.json) · [Experimental](tls-experimental.json)

Each route initially listed two accepted, resolved and programmed parents. After editing the route, it listed the retained parent. The retained listener continued reporting one attachment, and the withdrawn listener reported zero. Default Gateway discovery was disabled.

The run used the signed controller image, the upstream v1.7.0 CRDs, local Kubernetes clusters and local Octavia APIs. The JSON files include the observed data plane, load-balancer state and controller digest. Regional qualification is still open. [Build and component revisions](receipt.json)
