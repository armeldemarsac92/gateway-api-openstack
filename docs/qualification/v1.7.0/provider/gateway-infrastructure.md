# Gateway infrastructure metadata and DNS

With infrastructure discovery enabled, a Gateway publishes its observed private Octavia VIP through a selectorless headless Service and matching EndpointSlice. Gateway infrastructure labels and annotations are applied to both. EndpointSlice ports follow the listeners the controller observes as serving. [Operator configuration](../../../operator-gateway-infrastructure-metadata.md)

The registered `GatewayInfrastructureMetadata` test passed on Standard and Experimental. On each channel a client Pod resolved the Service through CoreDNS, opened a new connection to the simulated Octavia VIP on port 8080 and received HTTP 200 from an `infra-backend-v1` Pod through native HAProxy. After the load balancer was removed via Octavia REST and its replacement was held pending, the EndpointSlice disappeared, DNS stopped returning the VIP and the Service retained its UID. Both reports use the executable from the signed reproducible image. [Standard report](gateway-infrastructure-evidence/standard-report.yaml) · [Experimental report](gateway-infrastructure-evidence/experimental-report.yaml) · [DNS, HTTP and source versions](gateway-infrastructure-evidence/receipt.json)

The fixture uses a real kind control plane, CoreDNS, echo Pods, Octavia REST and HAProxy. The test supplies the network allocation, cloud worker scheduling and transport to the local VIP.

## Experimental ClusterIP reachability

With cluster address and infrastructure discovery enabled, a client Pod reached the Gateway's assigned Kubernetes ClusterIP through kube-proxy. Native HAProxy counted the request and the upstream echo Pod returned HTTP 200. After the Cluster address request was removed, the controller withdrew the Service and EndpointSlice. Three new connections failed and HAProxy's frontend counter did not increase. The headless Service still reached the same Octavia VIP and the simulated Floating IP association was retained. [Experimental traffic receipt](gateway-infrastructure-evidence/experimental-cluster-ip.json)

Gateway API v1.7.0 has no registered test for `GatewayAddressRoutabilityCluster`. The additional Experimental test exercises Pod traffic through a local native provider fixture. Separate API-server tests cover Standard field pruning and, on Experimental, loss and recovery of the last Cluster address when an EndpointSlice is owned elsewhere. [Operator configuration](../../../operator-gateway-infrastructure-metadata.md#experimental-clusterip-addresses) · [API tests](../../../../internal/controller/envtest_address_routability_test.go)
