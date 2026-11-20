# Local Hibiscus HTTP contract

Run the adapter scenario locally.

```sh
go test ./internal/cloud/openstack/graph -run '^TestHibiscusHTTPContractRecoversAndDeletesOnlyOwnedGraph$' -count=1
```

The [test](../../../internal/cloud/openstack/graph/hibiscus_contract_test.go) drives `ReconcileGatewayGraph` and Gophercloud against the existing in-memory HTTP transport. One project contains a Gateway with an Amphora load balancer, an HTTP listener, one HTTPRoute graph with a NodePort member, and a Floating IP. Two other load balancers represent the Magnum API server and an OCCM Service.

The first reconciliation observes the complete Gateway graph with an enabled hostname-and-path policy. Cleanup validates the inventory, then disables the owned load balancer through its public API. The fixture accepts the update and drops its HTTP response at the transport. A fresh provider re-reads the inventory and makes zero mutations while Octavia reports `PENDING_UPDATE`. After observing the load balancer disabled and `ACTIVE`, it deletes one verified resource per call. The test rejects rule deletion before that observation because removing a predicate could otherwise widen the set of requests the enabled policy matches. It also requires a non-cascade load balancer DELETE and a final inventory with the two original controls. [Amphora update flow][amphora-update] · [Amphora proxy template][amphora-template]

The fixture uses Octavia 19.0.0 at `93689cb7701da65b5aad387bb824c477c64b0f2b` for the public paths, list filters, response shapes and HTTP 204 deletion. Octavia accepts a conjunction of discovery tags as repeated or comma-separated query values. Dedicated child lists supply the full child records and their tags. Neutron supplies the independent Floating IP inventory. [Octavia load balancer controller][loadbalancer] · [Octavia list filtering][filtering] · [Octavia response types][types] · [Neutron Networking API][neutron]

Evidence level is local simulation. The fixture models the administrative update as pending until the load balancer configuration has converged. Live Magnum and OCCM lifecycle, authentication and authorization, Octavia provisioning, traffic and Gateway API conformance require a designated Hibiscus deployment.

[loadbalancer]: https://opendev.org/openstack/octavia/src/commit/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/controllers/load_balancer.py
[filtering]: https://opendev.org/openstack/octavia/src/commit/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/common/pagination.py
[types]: https://opendev.org/openstack/octavia/src/commit/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/api/v2/types
[neutron]: https://docs.openstack.org/api-ref/network/v2/index.html#floating-ips-floatingips
[amphora-update]: https://opendev.org/openstack/octavia/src/commit/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/controller/worker/v2/flows/load_balancer_flows.py#L380
[amphora-template]: https://opendev.org/openstack/octavia/src/commit/93689cb7701da65b5aad387bb824c477c64b0f2b/octavia/common/jinja/haproxy/combined_listeners/templates/haproxy.cfg.j2#L51
