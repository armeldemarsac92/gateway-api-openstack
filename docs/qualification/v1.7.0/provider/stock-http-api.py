#!/usr/bin/env python3
"""Check how stock Octavia v2 handles Gateway HTTP operations it cannot express."""

from importlib.metadata import version
import unittest
from uuid import uuid4

from octavia.tests.functional.api.v2 import base


class StockHTTP(base.BaseAPITest):
    def test_header_insertion_and_response_codes(self):
        lb_id = self.create_load_balancer(str(uuid4()))["loadbalancer"]["id"]
        self.set_lb_status(lb_id)
        forwarding_header = {"X-Forwarded-Port": "true"}
        listener = self.create_listener("HTTP", 80, lb_id, insert_headers=forwarding_header)["listener"]
        listener_id = listener["id"]
        listener_path = self.LISTENER_PATH.format(listener_id=listener_id)
        self.assertEqual(forwarding_header, self.get(listener_path).json["listener"]["insert_headers"])
        self.set_lb_status(lb_id)
        pool_id = self.create_pool(lb_id, "HTTP", "ROUND_ROBIN")["pool"]["id"]
        self.set_lb_status(lb_id)
        policy = self.create_l7policy(listener_id, "REDIRECT_TO_POOL", redirect_pool_id=pool_id)["l7policy"]
        policy_path = self.L7POLICY_PATH.format(l7policy_id=policy["id"])
        self.set_lb_status(lb_id)

        rejected = self.put(listener_path, {"listener": {"insert_headers": {"X-Tenant": "true"}}}, status=400)
        self.assertIn("X-Tenant", rejected.json["faultstring"])
        self.assertIn("not a valid option for insert_headers", rejected.json["faultstring"])
        self.assertEqual(forwarding_header, self.get(listener_path).json["listener"]["insert_headers"])

        rejected = self.put(policy_path, {"l7policy": {"insert_headers": {"X-Tenant": "blue"}}}, status=400)
        self.assertIn("Unknown attribute", rejected.json["faultstring"])
        self.assertIn("insert_headers", rejected.json["faultstring"])
        self.assertEqual(pool_id, self.get(policy_path).json["l7policy"]["redirect_pool_id"])

        redirect = {
            "listener_id": listener_id,
            "action": "REDIRECT_TO_URL",
            "redirect_url": "https://example.test/",
        }
        for code in (404, 500):
            with self.subTest(code=code):
                body = {"l7policy": {**redirect, "redirect_http_code": code}}
                response = self.post(self.L7POLICIES_PATH, body, status=400)
                self.assertIn("redirect_http_code", response.json["faultstring"])
                self.assertIn(str(code), response.json["faultstring"])
        body = {"l7policy": {**redirect, "redirect_http_code": 301}}
        response = self.post(self.L7POLICIES_PATH, body, status=201)
        self.assertEqual(301, response.json["l7policy"]["redirect_http_code"])


if __name__ == "__main__":
    if (version("octavia"), version("octavia-lib")) != ("19.0.0", "3.12.0"):
        raise SystemExit("The stock Hibiscus probe needs octavia 19.0.0 and octavia-lib 3.12.0")
    unittest.main(verbosity=2)
