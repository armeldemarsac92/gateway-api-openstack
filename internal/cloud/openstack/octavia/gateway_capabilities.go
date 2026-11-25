/*
Copyright 2026 The gateway-api-openstack Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package octavia

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
)

const (
	GatewayCapabilityHTTPHeaderExact           = "http_header_exact_v1"
	GatewayCapabilityHTTPQueryParamExact       = "http_query_param_exact_v1"
	GatewayCapabilityHTTPRequestPolicy         = "http_request_policy_v1"
	GatewayCapabilityHTTPRequestMirror         = "http_request_mirror_v1"
	GatewayCapabilityRequestURLRewrite         = "request_url_rewrite_v1"
	GatewayCapabilityRequestCORS               = "request_cors_v1"
	GatewayCapabilityHTTPRequestRedirect       = "http_request_redirect_v1"
	GatewayCapabilityHTTPMethodMatch           = "http_method_match_v1"
	GatewayCapabilityFixedHTTPResponses        = "fixed_http_responses_v1"
	GatewayCapabilityWeightedHTTPTargets       = "weighted_http_targets_v1"
	GatewayCapabilityTargetRequestFilters      = "weighted_http_target_request_filters_v1"
	GatewayCapabilityRequestHeaderOperations   = "request_header_operations_v1"
	GatewayCapabilityResponseHeaderOperations  = "response_header_operations_v1"
	GatewayCapabilityFrontendAuthFallback      = "frontend_client_auth_insecure_fallback_v1"
	GatewayCapabilityBackendRetryBudget        = "backend_retry_budget_v1"
	GatewayCapabilityTCPConnectionRejection    = "tcp_connection_rejection_v1"
	GatewayCapabilityHTTPCookieName            = "http_cookie_name_v1"
	GatewayCapabilityBackendH2C                = "backend_h2c_v1"
	GatewayCapabilityGRPCContentType           = "grpc_content_type_v1"
	GatewayCapabilityBackendTLSHostname        = "backend_tls_hostname_v1"
	GatewayCapabilityBackendTLSSubjectAltNames = "backend_tls_subject_alt_names_v1"
	GatewayCapabilityBackendTLSH2ALPN          = "backend_tls_h2_alpn_v1"
	GatewayCapabilityHTTPSListenerIsolation    = "https_listener_isolation_v1"
	GatewayCapabilityTLSSNIPassthrough         = "tls_sni_passthrough_v1"
	GatewayCapabilityTLSSNISuffix              = "tls_sni_suffix_v1"
	GatewayCapabilityTCPTLSTermination         = "tcp_tls_termination_v1"
	GatewayCapabilityTCPTLSMixedMode           = "tcp_tls_mixed_mode_v1"
	GatewayCapabilityTCPBackendTLSAuth         = "tcp_tls_backend_auth_v1"
	GatewayCapabilityL7PolicyInitialRules      = "l7policy_initial_rules_v1"
	GatewayCapabilityL7PolicyBatchActivation   = "l7policy_batch_activation_v1"
)

// MissingGatewayCapability reports an explicit negative capability discovery.
type MissingGatewayCapability struct {
	Provider   string
	Capability string
}

func (e *MissingGatewayCapability) Error() string {
	return fmt.Sprintf("provider %q did not advertise Octavia Gateway capability %q", e.Provider, e.Capability)
}

// VerifyGatewayCapabilities checks the selected provider on the authenticated,
// region-specific load-balancer endpoint. Only an explicit capability list
// authorizes use of the requested extensions.
func (s *Service) VerifyGatewayCapabilities(ctx context.Context, provider string, required ...string) error {
	if strings.TrimSpace(provider) == "" {
		return fmt.Errorf("missing effective load balancer provider in Octavia response")
	}
	var response struct {
		Capabilities []string `json:"gateway_capabilities"`
	}
	endpoint := s.client.ServiceURL("lbaas", "providers", url.PathEscape(provider), "gateway_capabilities")
	if _, err := s.client.Get(ctx, endpoint, &response, nil); err != nil {
		return fmt.Errorf("discover Gateway capabilities for Octavia provider %q: %w", provider, err)
	}
	if response.Capabilities == nil {
		return fmt.Errorf("discover Gateway capabilities for Octavia provider %q: response does not contain a capability list", provider)
	}
	for _, capability := range required {
		if !slices.Contains(response.Capabilities, capability) {
			return &MissingGatewayCapability{Provider: provider, Capability: capability}
		}
	}
	return nil
}
