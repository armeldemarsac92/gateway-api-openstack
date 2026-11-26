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
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
)

const (
	GatewayCapabilityHTTPLongHostname          = "http_long_hostname_v1"
	GatewayCapabilityHTTPLiteralPath           = "http_literal_path_v1"
	GatewayCapabilityHTTPHeaderExact           = "http_header_exact_v1"
	GatewayCapabilityHTTPHeaderName256         = "http_header_name_256_v1"
	GatewayCapabilityHTTPQueryParamExact       = "http_query_param_exact_v1"
	GatewayCapabilityHTTPRequestPolicy         = "http_request_policy_v1"
	GatewayCapabilityHTTPRetryBackoff          = "http_retry_backoff_v1"
	GatewayCapabilityHTTPRequestMirror         = "http_request_mirror_v1"
	GatewayCapabilityRequestURLRewrite         = "request_url_rewrite_v1"
	GatewayCapabilityRequestCORS               = "request_cors_v1"
	GatewayCapabilityHTTPRequestRedirect       = "http_request_redirect_v1"
	GatewayCapabilityHTTPMethodMatch           = "http_method_match_v1"
	GatewayCapabilityFixedHTTPResponses        = "fixed_http_responses_v1"
	GatewayCapabilityWeightedHTTPTargets       = "weighted_http_targets_v1"
	GatewayCapabilityTargetRequestFilters      = "weighted_http_target_request_filters_v1"
	GatewayCapabilityTargetResponseHeaders     = "weighted_http_target_response_headers_v1"
	GatewayCapabilityRequestHeaderOperations   = "request_header_operations_v1"
	GatewayCapabilityResponseHeaderOperations  = "response_header_operations_v1"
	GatewayCapabilityFrontendAuthFallback      = "frontend_client_auth_insecure_fallback_v1"
	GatewayCapabilityBackendRetryBudget        = "backend_retry_budget_v1"
	GatewayCapabilityTCPDefaultPoolTLS         = "tcp_default_pool_tls_v1"
	GatewayCapabilityTCPWeightedDefaultPools   = "tcp_weighted_default_pools_v1"
	GatewayCapabilityTCPConnectionRejection    = "tcp_connection_rejection_v1"
	GatewayCapabilityUDPPacketDrop             = "udp_packet_drop_v1"
	GatewayCapabilityUDPExtendedMemberWeights  = "udp_extended_member_weights_v1"
	GatewayCapabilityHTTPHeaderPersistence     = "http_header_persistence_v1"
	GatewayCapabilityHTTPCookieName            = "http_cookie_name_v1"
	GatewayCapabilityHTTPCookiePath            = "http_cookie_path_v1"
	GatewayCapabilityHTTPCookieAbsoluteTimeout = "http_cookie_absolute_timeout_v1"
	GatewayCapabilityHTTPCookiePermanent       = "http_cookie_permanent_v1"
	GatewayCapabilityBackendH2C                = "backend_h2c_v1"
	GatewayCapabilityGRPCContentType           = "grpc_content_type_v1"
	GatewayCapabilityGRPCRequestMirror         = "grpc_request_mirror_v1"
	GatewayCapabilityGRPCBinaryMetadata        = "grpc_binary_response_metadata_v1"
	GatewayCapabilityGRPCBinaryRequestMetadata = "grpc_binary_request_metadata_v1"
	GatewayCapabilityBackendTLSHostname        = "backend_tls_hostname_v1"
	GatewayCapabilityBackendTLSSystemCA        = "backend_tls_system_ca_v1"
	GatewayCapabilityBackendTLSSubjectAltNames = "backend_tls_subject_alt_names_v1"
	GatewayCapabilityBackendTLSH2ALPN          = "backend_tls_h2_alpn_v1"
	GatewayCapabilityHTTPSListenerIsolation    = "https_listener_isolation_v1"
	GatewayCapabilityTLSSNIPassthrough         = "tls_sni_passthrough_v1"
	GatewayCapabilityTLSSNISuffix              = "tls_sni_suffix_v1"
	GatewayCapabilityTLSSNIWeightedPools       = "tls_sni_weighted_pools_v1"
	GatewayCapabilityTCPTLSTermination         = "tcp_tls_termination_v1"
	GatewayCapabilityTCPTLSMixedMode           = "tcp_tls_mixed_mode_v1"
	GatewayCapabilityTCPBackendTLSAuth         = "tcp_tls_backend_auth_v1"
	GatewayCapabilityL7PolicyInitialRules      = "l7policy_initial_rules_v1"
	GatewayCapabilityL7PolicyBatchActivation   = "l7policy_batch_activation_v1"
)

const GatewayCapabilityL7PolicyConditionalRemoval = "l7policy_conditional_removal_v1"

const GatewayCapabilityL7PolicyClosedListenerRemoval = "l7policy_closed_listener_removal_v1"

const GatewayCapabilityTLSSNIPassthroughWeightedPools = "tls_sni_passthrough_weighted_pools_v1"

// RuleTypeHTTPPathLiteral is the Octavia type for a raw HTTP path predicate.
const RuleTypeHTTPPathLiteral l7policies.RuleType = "HTTP_PATH_LITERAL"

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
	return s.VerifyGatewayCapabilitiesForBody(ctx, provider, 0, required...)
}

// VerifyGatewayCapabilitiesForBody also checks the regional per-request limit
// when a route forwards its request body to an authorization service.
func (s *Service) VerifyGatewayCapabilitiesForBody(ctx context.Context, provider string, bodySize int32, required ...string) error {
	response, err := s.providerGatewayCapabilities(ctx, provider)
	if err != nil {
		return err
	}
	for _, capability := range required {
		if !slices.Contains(response.Capabilities, capability) {
			return &MissingGatewayCapability{Provider: provider, Capability: capability}
		}
	}
	if bodySize > 0 {
		limit, err := response.authBodyLimit()
		if err != nil {
			return fmt.Errorf("invalid maximum authorization body size reported by Octavia provider %q: %w", provider, err)
		}
		if bodySize > limit {
			return fmt.Errorf("authorization requires %d bytes, region advertises %d: %w", bodySize, limit,
				&MissingGatewayCapability{Provider: provider, Capability: GatewayCapabilityPolicyExternalAuthForwardBody})
		}
	}
	return nil
}

type providerGatewayCapabilities struct {
	Capabilities     []string        `json:"gateway_capabilities"`
	MaxAuthBodyBytes json.RawMessage `json:"l7policy_external_auth_max_body_bytes"`
}

func (response providerGatewayCapabilities) authBodyLimit() (int32, error) {
	var limit int32
	if len(response.MaxAuthBodyBytes) != 0 {
		if err := json.Unmarshal(response.MaxAuthBodyBytes, &limit); err != nil {
			return 0, err
		}
		if limit < 0 {
			return 0, fmt.Errorf("maximum authorization body size must be nonnegative")
		}
	}
	return limit, nil
}

// GatewayCapabilities reads the selected region's capability list.
func (s *Service) GatewayCapabilities(ctx context.Context, provider string) ([]string, error) {
	response, err := s.providerGatewayCapabilities(ctx, provider)
	if err != nil {
		return nil, err
	}
	return response.Capabilities, nil
}

func (s *Service) providerGatewayCapabilities(ctx context.Context, provider string) (*providerGatewayCapabilities, error) {
	if strings.TrimSpace(provider) == "" {
		return nil, fmt.Errorf("missing effective load balancer provider in Octavia response")
	}
	var response providerGatewayCapabilities
	endpoint := s.client.ServiceURL("lbaas", "providers", url.PathEscape(provider), "gateway_capabilities")
	if _, err := s.client.Get(ctx, endpoint, &response, nil); err != nil {
		return nil, fmt.Errorf("discover Gateway capabilities for Octavia provider %q: %w", provider, err)
	}
	if response.Capabilities == nil {
		return nil, fmt.Errorf("discover Gateway capabilities for Octavia provider %q: response does not contain a capability list", provider)
	}
	return &response, nil
}
