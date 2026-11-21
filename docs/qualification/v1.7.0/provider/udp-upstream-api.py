#!/usr/bin/env python3
"""Run a local suite against Octavia 19 and publish its stock Amphora configurations."""

import argparse
from importlib import import_module
from ipaddress import IPv4Address, IPv4Network
import os
from pathlib import Path
import subprocess
import time
import unittest
from unittest import mock
from wsgiref.simple_server import WSGIRequestHandler, make_server

from webob import Request
from webob.exc import HTTPInternalServerError

from octavia.amphorae.drivers.haproxy.rest_api_driver import (
    HaproxyAmphoraLoadBalancerDriver,
)
from octavia.common import constants, data_models
from octavia.controller.worker.v2.tasks import database_tasks
from octavia.db import models


stock = import_module("stock-udp")
VIP_RANGE = IPv4Network("192.0.2.0/24")
FIRST_VIP = IPv4Address("192.0.2.30")
PENDING = (constants.PENDING_CREATE, constants.PENDING_UPDATE, constants.PENDING_DELETE)
DELETE_TASKS = {
    "healthmonitors": (database_tasks.DeleteHealthMonitorInDB, constants.HEALTHMONITOR_ID),
    "l7policies": (database_tasks.DeleteL7PolicyInDB, constants.L7POLICY_ID),
    "listeners": (database_tasks.DeleteListenerInDB, constants.LISTENER_ID),
    "members": (database_tasks.DeleteMemberInDB, constants.MEMBER_ID),
    "rules": (database_tasks.DeleteL7RuleInDB, constants.L7RULE_ID),
}


class QuietHandler(WSGIRequestHandler):
    def log_request(self, code="-", size="-"):
        if int(code) >= 400:
            super().log_request(code, size)


class UpstreamUDP(stock.StockUDP):
    protocol = "UDP"

    def allocate_vip(self, requested):
        if requested:
            address = IPv4Address(requested)
            self.assertTrue(FIRST_VIP <= address < VIP_RANGE.broadcast_address)
            self.assertNotIn(address, self.allocated_vips)
        else:
            address = next(ip for ip in VIP_RANGE.hosts()
                           if ip >= FIRST_VIP and ip not in self.allocated_vips)
        self.allocated_vips.add(address)
        return str(address)

    def complete_delete(self, path, loadbalancer):
        collection, resource_id = path.rstrip("/").split("/")[-2:]
        if collection == "loadbalancers":
            self.assertEqual(loadbalancer.id, resource_id)
            for pool in loadbalancer.pools:
                database_tasks.DeletePoolInDB().execute(pool.id)
            for listener in loadbalancer.listeners:
                database_tasks.DeleteListenerInDB().execute({constants.LISTENER_ID: listener.id})
            database_tasks.MarkLBDeletedInDB().execute({constants.LOADBALANCER_ID: resource_id})
        elif collection == "pools":
            database_tasks.DeletePoolInDB().execute(resource_id)
        else:
            task, id_key = DELETE_TASKS[collection]
            task().execute({id_key: resource_id})

    def configure_renderer(self):
        self.assertEqual("keepalived_lvs", self.conf.conf.amphora_agent.amphora_udp_driver)
        self.renderer = HaproxyAmphoraLoadBalancerDriver().lvs_jinja

    def render_configs(self, loadbalancer):
        return {
            (listener.id, loadbalancer.vip.ip_address):
                self.renderer.build_config(listener=listener).encode("utf-8")
            for listener in loadbalancer.listeners
            if listener.protocol == constants.PROTOCOL_UDP
        }

    def publish(self, lb_id):
        with self.get_session() as session:
            loadbalancer = self.lb_repo.get(session, id=lb_id, show_deleted=False)
            configs = self.render_configs(loadbalancer) if loadbalancer else {}

        previous = self.configs.get(lb_id, {})
        for key in previous.keys() - configs.keys():
            self.apply("delete", lb_id, key, b"")
        for key, config in configs.items():
            if previous.get(key) != config:
                self.apply("apply", lb_id, key, config)
        self.configs[lb_id] = configs

    def apply(self, operation, lb_id, key, config):
        subprocess.run([self.options.apply, operation, lb_id, *key],
                       input=config, check=True, timeout=max(0, self.deadline - time.monotonic()))

    def complete_write(self, request):
        # REST locks the owning LB in a pending state before dispatching to the
        # provider. This server handles requests serially and completes each
        # dispatch before serving the next request.
        with self.get_session() as session:
            pending = (session.query(models.LoadBalancer.id)
                       .filter(models.LoadBalancer.provisioning_status.in_(PENDING)).all())
            self.assertEqual(len(pending), self.rpc_client.cast.call_count, request.path_info)
            if not pending:
                return
            self.assertEqual(1, len(pending), request.path_info)
            lb_id = pending[0].id
            loadbalancer = self.lb_repo.get(session, id=lb_id)

        if request.method == "DELETE":
            self.complete_delete(request.path_info, loadbalancer)
        if loadbalancer.provisioning_status != constants.PENDING_DELETE:
            self.set_lb_status(lb_id)
        self.publish(lb_id)

    def serve(self, environ, start_response):
        environ["HTTP_X_PROJECT_ID"] = self.project_id
        request = Request(environ)
        self.rpc_client.cast.reset_mock()
        try:
            response = request.get_response(self.app.app)
            if request.method in {"POST", "PUT", "DELETE"} and response.status_int < 300:
                self.complete_write(request)
        except Exception as error:
            self.failure = error
            response = HTTPInternalServerError()
        return response(environ, start_response)

    def run_upstream(self):
        self.conf.config(group="api_settings", default_provider_driver="amphora")
        self.allocated_vips = set()
        self.configs = {}
        self.failure = None
        self.configure_renderer()
        vip_subnet, member_subnet = self.install_network("10.244.0.0/16", self.allocate_vip)

        with make_server("127.0.0.1", 0, self.serve, handler_class=QuietHandler) as server:
            prefix = f"HIBISCUS_{self.protocol}_"
            env = dict(os.environ)
            env.update({
                prefix + "FIXTURE": f"127.0.0.1:{server.server_port}",
                prefix + "PROJECT": self.project_id,
                prefix + "VIP_SUBNET": vip_subnet,
                prefix + "MEMBER_SUBNET": member_subnet,
            })
            server.timeout = 0.2
            self.deadline = time.monotonic() + self.options.timeout
            with subprocess.Popen([self.options.run], env=env) as child:
                try:
                    while child.poll() is None and time.monotonic() < self.deadline:
                        server.handle_request()
                        if self.failure:
                            raise self.failure
                    self.assertIsNotNone(child.poll(), f"{self.protocol} conformance runner timed out")
                    self.assertEqual(0, child.returncode, f"{self.protocol} conformance runner failed")
                    self.assertTrue(self.configs, "runner completed without a successful Octavia write")
                finally:
                    if child.poll() is None:
                        child.kill()


class UpstreamHTTP(UpstreamUDP):
    protocol = "HTTP"
    listener_protocols = (constants.PROTOCOL_HTTP,)

    def configure_renderer(self):
        base_path = "/run/hibiscus-http-upstream"
        self.conf.config(group="haproxy_amphora", base_path=base_path, base_cert_dir=base_path)
        self.renderer = HaproxyAmphoraLoadBalancerDriver()

    def render_configs(self, loadbalancer):
        if not loadbalancer.listeners:
            return {}
        self.assertTrue(all(listener.protocol in self.listener_protocols
                            for listener in loadbalancer.listeners))
        agent = mock.Mock()
        agent.get_api_version.return_value = {"api_version": "1.0"}
        agent.get_info.return_value = {"haproxy_version": "3.0.19"}
        agent.get_details.return_value = {}
        self.renderer.clients.update({"base": agent, "1.0": agent})
        amphora = data_models.Amphora(id=loadbalancer.id, status=constants.ACTIVE)
        self.renderer.update_amphora_listeners(loadbalancer, amphora)

        agent.upload_config.assert_called_once_with(
            amphora, loadbalancer.id, mock.ANY, timeout_dict=None,
        )
        agent.reload_listener.assert_called_once_with(amphora, loadbalancer.id, timeout_dict=None)
        agent.upload_cert_pem.assert_not_called()
        config = agent.upload_config.call_args.args[2]
        return {(loadbalancer.vip.ip_address,): config.encode("utf-8")}


class UpstreamTCP(UpstreamHTTP):
    protocol = "TCP"
    # The shared Gateway API base also creates HTTP-only Gateways.
    listener_protocols = (constants.PROTOCOL_HTTP, constants.PROTOCOL_TCP)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    fixtures = {"udp": UpstreamUDP, "http": UpstreamHTTP, "tcp": UpstreamTCP}
    parser.add_argument("--protocol", choices=fixtures, default="udp")
    parser.add_argument("--run", type=Path, required=True, help="Local suite executable")
    parser.add_argument("--apply", type=Path, required=True, help="Configuration callback executable")
    parser.add_argument("--timeout", type=int, default=300, help="Suite deadline in seconds")
    options = parser.parse_args()
    if options.timeout <= 0:
        parser.error("--timeout must be positive")
    stock.require_stock_versions(parser)
    options.run = options.run.resolve(strict=True)
    options.apply = options.apply.resolve(strict=True)
    fixture = fixtures[options.protocol]
    fixture.options = options
    unittest.main(argv=[__file__, f"{fixture.__name__}.run_upstream"], verbosity=2)


if __name__ == "__main__":
    main()
