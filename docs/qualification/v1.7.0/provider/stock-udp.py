#!/usr/bin/env python3
"""Probe UDP listener updates and emit the stock Octavia Amphora LVS config."""

import argparse
from importlib.metadata import version
from pathlib import Path
import unittest
from unittest import mock
from uuid import uuid4

from octavia.amphorae.drivers.haproxy.rest_api_driver import (
    HaproxyAmphoraLoadBalancerDriver,
)
from octavia.common import data_models
from octavia.common import rpc
from octavia.common import utils
from octavia.common.config import cfg
from octavia.network import data_models as network_models
from octavia.tests.functional.api.v2 import base


VIP = "192.0.2.30"
LISTENER_PORT = 18053
MEMBERS = (("198.51.100.11", 15001, 3), ("198.51.100.12", 15002, 1))


class StockUDP(base.BaseAPITest):
    output_dir = None

    def _make_app(self):
        self.enterContext(mock.patch.object(rpc, "init"))
        self.rpc_client = self.enterContext(mock.patch.object(rpc, "get_client")).return_value
        return super()._make_app()

    def create_test_lb(self):
        subnet_id, network_id, port_id = (str(uuid4()) for _ in range(3))
        network = mock.Mock()
        network.get_subnet.return_value = network_models.Subnet(
            id=subnet_id, network_id=network_id, cidr="192.0.2.0/24", ip_version=4,
        )
        network.get_network.return_value = network_models.Network(
            id=network_id, subnets=[subnet_id], port_security_enabled=True,
        )
        network.allocate_vip.return_value = (
            data_models.Vip(ip_address=VIP, subnet_id=subnet_id,
                            network_id=network_id, port_id=port_id),
            [],
        )
        self.enterContext(mock.patch.object(utils, "get_network_driver", return_value=network))
        lb = self.create_load_balancer(subnet_id, provider="amphora", vip_address=VIP)["loadbalancer"]
        self.assertEqual(("amphora", VIP), (lb["provider"], lb["vip_address"]))
        self.set_lb_status(lb["id"])
        return lb["id"]

    def test_single_default_pool_lifecycle(self):
        self.assertEqual("keepalived_lvs", cfg.CONF.amphora_agent.amphora_udp_driver)
        lb_id = self.create_test_lb()
        listener = self.create_listener("UDP", LISTENER_PORT, lb_id)["listener"]
        listener_id = listener["id"]
        self.assertEqual(("UDP", LISTENER_PORT, None), (
            listener["protocol"], listener["protocol_port"], listener["default_pool_id"],
        ))
        self.set_lb_status(lb_id)

        pools = []
        member_paths = []
        for address, port, weight in MEMBERS:
            pool = self.create_pool(lb_id, "UDP", "ROUND_ROBIN")["pool"]
            pool_id = pool["id"]
            self.assertEqual(("UDP", []), (pool["protocol"], pool["listeners"]))
            self.set_lb_status(lb_id)
            member = self.create_member(pool_id, address, port, weight=weight)["member"]
            path = self.MEMBER_PATH.format(pool_id=pool_id, member_id=member["id"])
            self.set_lb_status(lb_id)
            member = self.get(path).json["member"]
            self.assertEqual((address, port, weight), (
                member["address"], member["protocol_port"], member["weight"],
            ))
            pools.append(pool_id)
            member_paths.append(path)
        print("UDP API created an unattached listener and two independent pools with one member each")

        driver = HaproxyAmphoraLoadBalancerDriver()
        client = mock.Mock()
        client.get_info.return_value = {"haproxy_version": "3.0.19"}
        driver.clients["1.0"] = client
        driver.clients["base"] = mock.Mock()
        driver.clients["base"].get_api_version.return_value = {"api_version": "1.0"}
        amphora = data_models.Amphora(id=str(uuid4()), status="ACTIVE")
        listener_path = self.LISTENER_PATH.format(listener_id=listener_id)
        for name, pool_id in (("pool-a", pools[0]), ("pool-b", pools[1]), ("detached", None)):
            self.put(listener_path, {"listener": {"default_pool_id": pool_id}})
            self.set_lb_status(lb_id)
            self.assertEqual(pool_id, self.get_listener(listener_id)["listener"]["default_pool_id"])

            client.reset_mock()
            with self.get_session() as session:
                loadbalancer = self.lb_repo.get(session, id=lb_id)
                driver.update_amphora_listeners(loadbalancer, amphora)
            self.assertEqual(
                [mock.call.get_info(amphora, timeout_dict=None),
                 mock.call.upload_udp_config(amphora, listener_id, mock.ANY, timeout_dict=None),
                 mock.call.reload_listener(amphora, listener_id, timeout_dict=None)],
                client.mock_calls,
            )
            config = client.upload_udp_config.call_args.args[2]
            self.assertIn("net_namespace amphora-haproxy", config)
            if pool_id:
                address, port, weight = MEMBERS[pools.index(pool_id)]
                self.assertIn(f"{VIP} {LISTENER_PORT}", config)
                self.assertIn("lb_kind NAT", config)
                self.assertIn("lb_algo wrr", config)
                self.assertIn("protocol UDP", config)
                self.assertIn(f"real_server {address} {port} {{", config)
                self.assertIn(f"weight {weight}", config)
                self.assertEqual(1, config.count("real_server "))
            else:
                self.assertNotIn("virtual_server", config)
                self.assertNotIn("real_server ", config)
            if self.output_dir is not None:
                with (self.output_dir / f"{name}.conf").open("x", encoding="utf-8") as output:
                    output.write(config)

        for path in member_paths:
            self.get(path)
        self.assertEqual(
            ["create_load_balancer", "create_listener", "create_pool", "create_member",
             "create_pool", "create_member", "update_listener", "update_listener",
             "update_listener"],
            [call.args[1] for call in self.rpc_client.cast.call_args_list],
        )
        print("Stock Amphora uploaded a UDP/NAT/WRR config for each selected pool and no virtual server after detachment")

    def test_udp_listener_rejects_l7_policy(self):
        lb_id = self.create_test_lb()
        listener = self.create_listener("UDP", LISTENER_PORT, lb_id)["listener"]
        self.set_lb_status(lb_id)
        refused = self.create_l7policy(listener["id"], "REJECT", status=501)
        self.assertIn("UDP protocol listeners do not support L7 policies", refused["faultstring"])
        self.assertNotIn("create_l7policy", [call.args[1] for call in self.rpc_client.cast.call_args_list])
        print("Amphora refused a UDP REJECT policy with HTTP 501 before worker dispatch")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, help="New directory for the generated configurations")
    args = parser.parse_args()
    for dist, expected in (("octavia", "19.0.0"), ("octavia-lib", "3.12.0")):
        installed = version(dist)
        if installed != expected:
            parser.error(f"expected {dist} {expected}, found {installed}")
    if args.output_dir is not None:
        args.output_dir.mkdir()
        StockUDP.output_dir = args.output_dir
    print("Octavia 19.0.0, octavia-lib 3.12.0, Amphora keepalived_lvs", flush=True)
    unittest.main(argv=[__file__], verbosity=2)


if __name__ == "__main__":
    main()
