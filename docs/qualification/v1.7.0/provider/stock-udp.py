#!/usr/bin/env python3
"""Probe UDP listener updates and emit the stock Octavia Amphora LVS config."""

import argparse
from importlib.metadata import version
import os
from pathlib import Path
import subprocess
import time
import unittest
from unittest import mock
from uuid import uuid4
from wsgiref.simple_server import WSGIRequestHandler, make_server

from webob import Request

from octavia.amphorae.drivers.haproxy.rest_api_driver import (
    HaproxyAmphoraLoadBalancerDriver,
)
from octavia.common import constants
from octavia.common import data_models
from octavia.common import rpc
from octavia.common import utils
from octavia.common.config import cfg
from octavia.controller.worker.v2.tasks import database_tasks
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

    def install_network(self, member_cidr="198.51.100.0/24", vip_address=lambda requested: requested or VIP):
        subnet_id, member_subnet, network_id = (str(uuid4()) for _ in range(3))
        network = mock.Mock()
        subnets = {
            subnet_id: network_models.Subnet(id=subnet_id, network_id=network_id, cidr="192.0.2.0/24", ip_version=4),
            member_subnet: network_models.Subnet(id=member_subnet, network_id=network_id, cidr=member_cidr, ip_version=4),
        }
        network.get_subnet.side_effect = lambda subnet_id, context=None: subnets[str(subnet_id)]
        network.get_network.return_value = network_models.Network(
            id=network_id, subnets=list(subnets), port_security_enabled=True,
        )
        network.allocate_vip.side_effect = lambda lb: (
            data_models.Vip(ip_address=vip_address(lb.vip.ip_address), subnet_id=subnet_id,
                            network_id=network_id, port_id=str(uuid4())),
            [],
        )
        self.enterContext(mock.patch.object(utils, "get_network_driver", return_value=network))
        return subnet_id, member_subnet

    def create_test_lb(self):
        subnet_id, _ = self.install_network()
        lb = self.create_load_balancer(subnet_id, provider="amphora", vip_address=VIP)["loadbalancer"]
        self.assertEqual(("amphora", VIP), (lb["provider"], lb["vip_address"]))
        self.set_lb_status(lb["id"])
        return lb["id"]

    def worker_config(self, lb_id, listener_id):
        driver = HaproxyAmphoraLoadBalancerDriver()
        client = mock.Mock()
        client.get_info.return_value = {"haproxy_version": "3.0.19"}
        driver.clients["1.0"] = client
        driver.clients["base"] = mock.Mock()
        driver.clients["base"].get_api_version.return_value = {"api_version": "1.0"}
        amphora = data_models.Amphora(id=str(uuid4()), status="ACTIVE")
        with self.get_session() as session:
            loadbalancer = self.lb_repo.get(session, id=lb_id)
            driver.update_amphora_listeners(loadbalancer, amphora)
        self.assertEqual([
            mock.call.get_info(amphora, timeout_dict=None),
            mock.call.upload_udp_config(amphora, listener_id, mock.ANY, timeout_dict=None),
            mock.call.reload_listener(amphora, listener_id, timeout_dict=None),
        ], client.mock_calls)
        return client.upload_udp_config.call_args.args[2]

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

        listener_path = self.LISTENER_PATH.format(listener_id=listener_id)
        for name, pool_id in (("pool-a", pools[0]), ("pool-b", pools[1]), ("detached", None)):
            self.put(listener_path, {"listener": {"default_pool_id": pool_id}})
            self.set_lb_status(lb_id)
            self.assertEqual(pool_id, self.get_listener(listener_id)["listener"]["default_pool_id"])

            config = self.worker_config(lb_id, listener_id)
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

    def controller_graph(self):
        vip_subnet, member_subnet = self.install_network()
        for name, address, port in (("magnum-api", "192.0.2.20", 6443), ("occm-service", "192.0.2.21", 443)):
            lb = self.create_load_balancer(vip_subnet, name=name, provider="amphora",
                                           vip_address=address, tags=[name])["loadbalancer"]
            self.set_lb_status(lb["id"])
            listener = self.create_listener("TCP", port, lb["id"])["listener"]["id"]
            self.set_lb_status(lb["id"])
            pool = self.create_pool_with_listener(lb["id"], listener, "TCP", "ROUND_ROBIN")["pool"]["id"]
            self.set_lb_status(lb["id"])
            self.create_member(pool, "198.51.100.20", port, subnet_id=member_subnet)
            self.set_lb_status(lb["id"])

        def inventory():
            paths = [(self.LBS_PATH, "loadbalancers"), (self.LISTENERS_PATH, "listeners"),
                     (self.POOLS_PATH, "pools"), (self.HMS_PATH, "healthmonitors")]
            result = {}
            for path, key in paths:
                items = self.get(path, params={"project_id": self.project_id}).json[key]
                result[path] = {item["id"]: item for item in items}
            for pool_id in result[self.POOLS_PATH]:
                path = self.MEMBERS_PATH.format(pool_id=pool_id)
                result[path] = {member["id"]: member for member in self.get(path).json["members"]}
            return result

        original = inventory()
        protected_ids = {resource_id for collection in original.values() for resource_id in collection}
        self.assertEqual(8, len(protected_ids))
        lb_id = None
        states = []
        configs = {}

        def serve(environ, start_response):
            nonlocal lb_id
            environ["HTTP_X_PROJECT_ID"] = self.project_id
            request = Request(environ)
            path = request.path_info
            update = request.json_body.get("listener", {}) if request.method == "PUT" and "/listeners/" in path else {}
            response = request.get_response(self.app.app)
            if request.method != "GET" and response.status_int < 300:
                if path == self.BASE_PATH + self.LBS_PATH and request.method == "POST":
                    lb_id = response.json_body["loadbalancer"]["id"]
                self.assertIsNotNone(lb_id)
                self.assertTrue(protected_ids.isdisjoint(path.split("/")))
                if request.method == "DELETE":
                    collection, resource_id = path.split("/")[-2:]
                    # BaseAPITest.set_lb_status completes pending creates and updates.
                    # The Amphora delete flows also remove child rows and their
                    # relationships; run the tagged worker's database tasks here.
                    if collection == "loadbalancers":
                        self.assertEqual(lb_id, resource_id)
                        database_tasks.MarkLBDeletedInDB().execute({constants.LOADBALANCER_ID: resource_id})
                    else:
                        if collection == "members":
                            database_tasks.DeleteMemberInDB().execute({constants.MEMBER_ID: resource_id})
                        elif collection == "healthmonitors":
                            database_tasks.DeleteHealthMonitorInDB().execute({constants.HEALTHMONITOR_ID: resource_id})
                        elif collection == "pools":
                            database_tasks.DeletePoolInDB().execute(resource_id)
                        elif collection == "listeners":
                            database_tasks.DeleteListenerInDB().execute({constants.LISTENER_ID: resource_id})
                        else:
                            self.fail(f"unexpected DELETE collection {collection}")
                        self.set_lb_status(lb_id)
                else:
                    self.set_lb_status(lb_id)
                if "default_pool_id" in update:
                    listener_id = path.rsplit("/", 1)[-1]
                    actual = self.get_listener(listener_id)["listener"]["default_pool_id"]
                    self.assertEqual(update["default_pool_id"], actual)
                    config = self.worker_config(lb_id, listener_id)
                    state = "detached"
                    if actual:
                        members = self.get(self.MEMBERS_PATH.format(pool_id=actual)).json["members"]
                        self.assertEqual(1, len(members))
                        member = members[0]
                        state = {("198.51.100.11", 15001): "pool-a", ("198.51.100.12", 15002): "pool-b"}[
                            member["address"], member["protocol_port"]]
                        self.assertIn(f"real_server {member['address']} {member['protocol_port']} {{", config)
                        self.assertIn("weight 1", config)
                        pool = self.get(self.POOL_PATH.format(pool_id=actual)).json["pool"]
                        if pool.get("healthmonitor_id"):
                            monitor = self.get(self.HM_PATH.format(healthmonitor_id=pool["healthmonitor_id"])).json["healthmonitor"]
                            self.assertEqual(("UDP-CONNECT", 10, 5, 3, 3), (
                                monitor["type"], monitor["delay"], monitor["timeout"],
                                monitor["max_retries"], monitor["max_retries_down"],
                            ))
                            self.assertIn("MISC_CHECK", config)
                            state += "-monitored"
                    else:
                        self.assertNotIn("virtual_server", config)
                    if state in configs:
                        self.assertEqual(configs[state], config)
                    configs[state] = config
                    states.append(state)
            return response(environ, start_response)

        class QuietHandler(WSGIRequestHandler):
            def log_request(self, code="-", size="-"):
                if int(code) >= 400:
                    super().log_request(code, size)

        with make_server("127.0.0.1", 0, serve, handler_class=QuietHandler) as server:
            server.timeout = 0.2
            env = dict(os.environ, HIBISCUS_UDP_FIXTURE=f"127.0.0.1:{server.server_port}",
                       HIBISCUS_UDP_PROJECT=self.project_id, HIBISCUS_UDP_VIP_SUBNET=vip_subnet,
                       HIBISCUS_UDP_MEMBER_SUBNET=member_subnet)
            with subprocess.Popen([self.controller_test, "-test.v", "-test.run", "^TestHibiscusUDPStockAPI$",
                                   "-test.timeout", "90s"], env=env) as child:
                try:
                    deadline = time.monotonic() + 95
                    while child.poll() is None and time.monotonic() < deadline:
                        server.handle_request()
                    self.assertEqual(0, child.poll(), "Go adapter test failed or timed out")
                finally:
                    if child.poll() is None:
                        child.kill()
        self.assertEqual(["pool-a", "detached", "pool-b", "detached", "pool-b-monitored", "detached"], states)
        self.assertEqual(original, inventory())
        if self.output_dir is not None:
            for state, config in configs.items():
                (self.output_dir / f"{state}.conf").write_text(config, encoding="utf-8")
        print("Stock API observed A, B, UDP-CONNECT on B and final detach. The two control load balancers and their six children matched.")


def require_stock_versions(parser):
    for dist, expected in (("octavia", "19.0.0"), ("octavia-lib", "3.12.0")):
        installed = version(dist)
        if installed != expected:
            parser.error(f"expected {dist} {expected}, found {installed}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, help="New directory for the generated configurations")
    parser.add_argument("--controller-test", type=Path, help="Compiled Go graph test executable for the stock API run")
    args = parser.parse_args()
    require_stock_versions(parser)
    if args.output_dir is not None:
        args.output_dir.mkdir()
        StockUDP.output_dir = args.output_dir
    print("Octavia 19.0.0, octavia-lib 3.12.0, Amphora keepalived_lvs", flush=True)
    if args.controller_test:
        StockUDP.controller_test = args.controller_test.resolve(strict=True)
        unittest.main(argv=[__file__, "StockUDP.controller_graph"], verbosity=2)
    unittest.main(argv=[__file__], verbosity=2)


if __name__ == "__main__":
    main()
