# Copyright 2026 The gateway-api-openstack Authors.
# Licensed under the Apache License, Version 2.0.

"""Project the local REST fixture onto stock Octavia HTTP data models."""

from importlib.metadata import version
import json
from pathlib import Path
import sys

from octavia.common import constants
from octavia.common import data_models
from octavia.common.jinja.haproxy.combined_listeners import jinja_cfg


def model(constructor, data, fields="", **kwargs):
    values = {key: data[key] for key in fields.split()}
    return constructor(id=data["id"], enabled=data["admin_state_up"],
                       provisioning_status=data["provisioning_status"], **values, **kwargs)


def children(inventory, collection):
    return (data for path, data in inventory.items() if path.rpartition("/")[0] == collection)


def pool_from_inventory(inventory, gateway, pool):
    if pool["loadbalancers"] != [{"id": gateway["id"]}]:
        raise ValueError("pool is not attached to the observed Gateway")
    members = list(children(inventory, f"/v2/lbaas/pools/{pool['id']}/members"))
    if any(member["address"] != "127.0.0.1" for member in members):
        raise ValueError("the pilot expects loopback members")
    backends = [
        model(data_models.Member, member, "protocol_port subnet_id",
              ip_address=member["address"], weight=member.get("weight", 1))
        for member in members
    ]
    monitor, = (monitor for monitor in children(inventory, "/v2/lbaas/healthmonitors")
                if monitor["pools"] == [{"id": pool["id"]}])
    health_monitor = model(
        data_models.HealthMonitor, monitor, "type delay timeout http_method url_path expected_codes",
        rise_threshold=monitor["max_retries"], fall_threshold=monitor["max_retries_down"],
    )
    return model(data_models.Pool, pool, "protocol lb_algorithm",
                 members=backends, health_monitor=health_monitor,
                 tls_enabled=pool.get("tls_enabled", False))


def policy_from_inventory(inventory, listener, pools, policy):
    if policy["listener_id"] != listener["id"]:
        raise ValueError("policy is not attached to the observed listener")
    rules = [
        model(data_models.L7Rule, data, "type compare_type value",
              key=data.get("key"), invert=data.get("invert", False))
        for data in children(inventory, f"/v2/lbaas/l7policies/{policy['id']}/rules")
    ]
    return model(data_models.L7Policy, policy, "action position", l7rules=rules,
                 redirect_pool=pools[policy["redirect_pool_id"]],
                 redirect_url=policy.get("redirect_url"), redirect_prefix=policy.get("redirect_prefix"))


def listener_from_inventory(inventory):
    gateway = inventory["/v2/lbaas/loadbalancers/gateway"]
    listener = inventory["/v2/lbaas/listeners/listener"]
    if listener["default_pool_id"] is not None:
        raise ValueError("the pilot requires a listener without a default pool")
    if gateway["vip_address"] != "127.0.0.1":
        raise ValueError("the pilot expects a loopback frontend")
    if listener["loadbalancers"] != [{"id": gateway["id"]}]:
        raise ValueError("listener is not attached to the observed Gateway")

    pools = {pool["id"]: pool_from_inventory(inventory, gateway, pool)
             for pool in children(inventory, "/v2/lbaas/pools")}
    # The Octavia database exposes the listener's policies in position order.
    ordered = sorted(children(inventory, "/v2/lbaas/l7policies"), key=lambda policy: policy["position"])
    policies = [policy_from_inventory(inventory, listener, pools, policy) for policy in ordered]
    load_balancer = model(data_models.LoadBalancer, gateway, "project_id",
                          vip=data_models.Vip(ip_address=gateway["vip_address"]),
                          topology=constants.TOPOLOGY_SINGLE)
    return model(data_models.Listener, listener, "protocol protocol_port",
                 load_balancer=load_balancer, pools=list(pools.values()), l7policies=policies,
                 insert_headers=listener.get("insert_headers", {}))


def main():
    for package, want in (("octavia", "19.0.0"), ("octavia-lib", "3.12.0")):
        if version(package) != want:
            raise ValueError(f"{package} {want} is required, found {version(package)}")
    output = Path(sys.argv[1])
    listener = listener_from_inventory(json.load(sys.stdin))
    amphora = data_models.Amphora(id="local-amphora", lb_network_ip="127.0.0.1")
    jinja_cfg.CONF.set_override("default_connection_limit", 64, group="haproxy_amphora")
    templater = jinja_cfg.JinjaTemplater(
        base_amp_path=str(output), connection_logging=False,
        log_http=str(output / "syslog.sock"), log_server=str(output / "syslog.sock"),
    )
    config = templater.render_loadbalancer_obj(
        amphora, [listener], socket_path=str(output / "stats.sock"),
        feature_compatibility={constants.HTTP_REUSE: True},
    )
    (output / "haproxy.cfg").write_text(config, encoding="utf-8")


if __name__ == "__main__":
    main()
