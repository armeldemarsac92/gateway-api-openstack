# Local Hibiscus HTTP contract

Run the adapter scenario locally.

```sh
go test ./internal/cloud/openstack/graph -run '^TestHibiscusHTTPContractRecoversAndDeletesOnlyOwnedGraph$' -count=1
```

The [test](../../../internal/cloud/openstack/graph/hibiscus_contract_test.go) drives `ReconcileGatewayGraph` and Gophercloud against the existing in-memory HTTP transport. One project contains a Gateway with an Amphora load balancer, an HTTP listener, one HTTPRoute graph with a NodePort member, and a Floating IP. Two other load balancers represent the Magnum API server and an OCCM Service.

The first reconciliation observes the complete Gateway graph. The fixture applies the initial DELETE on the L7 rule and drops its HTTP response at the transport. A fresh provider re-reads the same inventory using the project, ownership tags and parent filters, then deletes one verified resource per call. The test requires a non-cascade load balancer DELETE, no creation or unrelated mutations, and a final inventory with the two original controls.

The fixture uses Octavia 19.0.0 at `93689cb7701da65b5aad387bb824c477c64b0f2b` for the public paths, list filters, response shapes and HTTP 204 deletion. Octavia accepts a conjunction of discovery tags as repeated or comma-separated query values. Dedicated child lists supply the full child records and their tags. Neutron supplies the independent Floating IP inventory. [Octavia load balancer controller][loadbalancer] · [Octavia list filtering][filtering] · [Octavia response types][types] · [Neutron Networking API][neutron]

Evidence level is local simulation. The fixture provides settled inventory between reconcile calls. Live Magnum and OCCM lifecycle, authentication and authorization, Octavia provisioning, traffic and Gateway API conformance require a designated Hibiscus deployment.

[loadbalancer]: https://opendev.org/openstack/octavia/src/commit/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/controllers/load_balancer.py
[filtering]: https://opendev.org/openstack/octavia/src/commit/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/common/pagination.py
[types]: https://opendev.org/openstack/octavia/src/commit/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/types
[neutron]: https://docs.openstack.org/api-ref/network/v2/index.html#floating-ips-floatingips
