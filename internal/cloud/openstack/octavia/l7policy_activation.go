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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/monitors"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

type PolicySnapshot struct {
	Revision string
	Policy   L7PolicyObservation
	Rules    []RuleObservation
}

type PolicyActivationRevision struct {
	ID       string `json:"id"`
	Revision string `json:"revision"`
}

const MaxPolicyActivationBatch = 64

type PolicyActivationBatch struct {
	ExpectedGraphRevision string                     `json:"expected_graph_revision"`
	ExpectedOrder         []string                   `json:"expected_order"`
	Policies              []PolicyActivationRevision `json:"policies"`
}

const GatewayCapabilityGRPCAuthTransition = "l7policy_grpc_auth_transition_v1"

type GRPCAuthTransitionRule struct {
	ID   string   `json:"id"`
	Tags []string `json:"tags"`
}

type GRPCAuthTransitionPolicy struct {
	PolicyActivationRevision
	Tags         []string                 `json:"tags"`
	Rules        []GRPCAuthTransitionRule `json:"rules"`
	Origin       string                   `json:"redirect_pool_id,omitempty"`
	ExternalAuth *PolicyExternalAuth      `json:"external_auth,omitempty"`
}

type GRPCAuthTransitionBatch struct {
	ExpectedGraphRevision string                     `json:"expected_graph_revision"`
	ExpectedOrder         []string                   `json:"expected_order"`
	Policies              []GRPCAuthTransitionPolicy `json:"policies"`
}

type PolicyActivationGraph struct {
	Revision     string
	LoadBalancer *LoadBalancer
	Listener     *PolicyActivationListener
	Pools        []PolicyActivationPoolInventory
	Policies     []PolicySnapshot
}

type PolicyActivationListener struct {
	listeners.Listener
	DefaultPool         json.RawMessage    `json:"default_pool_id"`
	DefaultHTTPResponse *FixedHTTPResponse `json:"default_http_response"`
	HTTPSVirtualHosts   []string           `json:"https_virtual_hosts"`
}

type PolicyActivationPoolInventory struct {
	Pool    *PolicyActivationPool    `json:"pool"`
	Members []PolicyActivationMember `json:"members"`
}

type PolicyActivationPool struct {
	pools.Pool
	Persistence      json.RawMessage   `json:"session_persistence"`
	AdminState       *bool             `json:"admin_state_up"`
	TLSEnabled       *bool             `json:"tls_enabled"`
	BackendH2C       *bool             `json:"backend_h2c"`
	BackendTLSHost   string            `json:"backend_tls_hostname"`
	BackendRequireH2 bool              `json:"backend_tls_require_h2"`
	HealthMonitor    *monitors.Monitor `json:"healthmonitor"`
	BackendSystemCA  json.RawMessage   `json:"backend_tls_system_ca"`
	BackendCADigest  json.RawMessage   `json:"backend_tls_system_ca_sha256"`
	BackendSANs      json.RawMessage   `json:"backend_tls_subject_alt_names"`
}

func (row *PolicyActivationPool) MatchesBackendTLSVerification(proven PoolBackendTLS) bool {
	var current PoolBackendTLS
	current.observeSystemCA(row.BackendSystemCA, row.BackendCADigest)
	if current.SystemCAInvalid || proven.SystemCAInvalid || current.SystemCA != proven.SystemCA ||
		current.SystemCAObserved != proven.SystemCAObserved || current.SystemCADigestObserved != proven.SystemCADigestObserved ||
		current.SystemCASHA256 != proven.SystemCASHA256 || proven.SubjectAltNamesInvalid {
		return false
	}
	if row.BackendSANs == nil {
		return !proven.SubjectAltNamesObserved
	}
	names, err := decodeBackendTLSSubjectAltNames(row.BackendSANs)
	return err == nil && proven.SubjectAltNamesObserved && slices.Equal(names, proven.SubjectAltNames)
}

type PolicyActivationMember struct {
	Member       pools.Member
	AdminStateUp *bool `json:"admin_state_up"`
	Backup       *bool `json:"backup"`
	Weight       *int  `json:"weight"`
}

func (row *PolicyActivationMember) UnmarshalJSON(data []byte) error {
	type memberFlags PolicyActivationMember
	if err := json.Unmarshal(data, (*memberFlags)(row)); err != nil {
		return err
	}
	return json.Unmarshal(data, &row.Member)
}

func (s *Service) ObservePolicyActivation(ctx context.Context, listenerID string) (PolicyActivationGraph, error) {
	return s.observePolicyActivation(ctx, s.client.ServiceURL("lbaas", "listeners", listenerID, "l7policy_activation"))
}

type GRPCAuthRecovery struct {
	OperationID string `json:"operation_id"`
}

func (s *Service) ObserveGRPCAuthRecovery(ctx context.Context, listenerID string) (GRPCAuthRecovery, error) {
	var recovery GRPCAuthRecovery
	endpoint := s.client.ServiceURL("lbaas", "listeners", listenerID, "l7policy_grpc_auth_transition", "recovery")
	if _, err := s.client.Get(ctx, endpoint, &recovery, nil); err != nil {
		return GRPCAuthRecovery{}, fmt.Errorf("observe Octavia gRPC authorization recovery: %w", err)
	}
	if recovery.OperationID == "" {
		return GRPCAuthRecovery{}, errors.New("Octavia gRPC authorization recovery omitted the operation ID")
	}
	return recovery, nil
}

func (s *Service) RecoverGRPCAuth(ctx context.Context, listenerID string, recovery GRPCAuthRecovery) error {
	endpoint := s.client.ServiceURL("lbaas", "listeners", listenerID, "l7policy_grpc_auth_transition", "recovery")
	_, err := s.client.Post(ctx, endpoint, recovery, nil, &gophercloud.RequestOpts{OkCodes: []int{http.StatusNoContent}})
	if err != nil {
		return fmt.Errorf("recover Octavia gRPC authorization transition: %w", err)
	}
	return nil
}

func (s *Service) ObserveGRPCAuthTransition(ctx context.Context, listenerID string, destinations []string) (PolicyActivationGraph, error) {
	endpoint := s.client.ServiceURL("lbaas", "listeners", listenerID, "l7policy_grpc_auth_transition")
	if len(destinations) != 0 {
		endpoint += "?" + url.Values{"pool_ids": {strings.Join(destinations, ",")}}.Encode()
	}
	return s.observePolicyActivation(ctx, endpoint)
}

func (s *Service) TransitionGRPCAuth(ctx context.Context, listenerID string, batch GRPCAuthTransitionBatch) error {
	body := struct {
		Transition GRPCAuthTransitionBatch `json:"l7policy_grpc_auth_transition"`
	}{Transition: batch}
	endpoint := s.client.ServiceURL("lbaas", "listeners", listenerID, "l7policy_grpc_auth_transition")
	_, err := s.client.Post(ctx, endpoint, body, nil, &gophercloud.RequestOpts{OkCodes: []int{http.StatusNoContent}})
	if err != nil {
		return fmt.Errorf("switch Octavia L7 policy gRPC authorization: %w", err)
	}
	return nil
}

func (s *Service) observePolicyActivation(ctx context.Context, endpoint string) (PolicyActivationGraph, error) {
	var response struct {
		Activation struct {
			Revision     string                          `json:"graph_revision"`
			LoadBalancer *LoadBalancer                   `json:"loadbalancer"`
			Listener     *PolicyActivationListener       `json:"listener"`
			Pools        []PolicyActivationPoolInventory `json:"pools"`
			Policies     []struct {
				Revision string          `json:"revision"`
				Policy   json.RawMessage `json:"policy"`
			} `json:"policies"`
		} `json:"l7policy_activation"`
	}
	if _, err := s.client.Get(ctx, endpoint, &response, nil); err != nil {
		return PolicyActivationGraph{}, fmt.Errorf("read Octavia L7 policy activation snapshot: %w", err)
	}
	wire := response.Activation
	if !validPolicyRevision(wire.Revision) || wire.LoadBalancer == nil || wire.Listener == nil ||
		wire.Pools == nil || wire.Policies == nil {
		return PolicyActivationGraph{}, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			errors.New("incomplete graph in Octavia L7 policy activation snapshot"))
	}
	snapshot := PolicyActivationGraph{
		Revision: wire.Revision, LoadBalancer: wire.LoadBalancer, Listener: wire.Listener,
		Pools: wire.Pools, Policies: make([]PolicySnapshot, 0, len(wire.Policies)),
	}
	for _, item := range wire.Policies {
		row, err := readPolicySnapshot(item.Revision, item.Policy)
		if err != nil {
			return PolicyActivationGraph{}, err
		}
		snapshot.Policies = append(snapshot.Policies, row)
	}
	return snapshot, nil
}

func readPolicySnapshot(revision string, raw json.RawMessage) (PolicySnapshot, error) {
	if !validPolicyRevision(revision) {
		return PolicySnapshot{}, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			errors.New("invalid revision in Octavia L7 policy snapshot"))
	}
	policy, err := readPolicyObservation(raw)
	if err != nil {
		return PolicySnapshot{}, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			fmt.Errorf("read Octavia L7 policy snapshot: %w", err))
	}
	var row struct {
		Rules []RuleObservation `json:"rules"`
	}
	if err := json.Unmarshal(raw, &row); err != nil || row.Rules == nil {
		return PolicySnapshot{}, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady,
			errors.New("incomplete rules in Octavia L7 policy snapshot"))
	}
	return PolicySnapshot{Revision: revision, Policy: policy, Rules: row.Rules}, nil
}

func validPolicyRevision(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (s *Service) ActivatePolicies(ctx context.Context, listenerID string, batch PolicyActivationBatch) error {
	body := struct {
		Activation PolicyActivationBatch `json:"l7policy_activation"`
	}{Activation: batch}
	endpoint := s.client.ServiceURL("lbaas", "listeners", listenerID, "l7policy_activation")
	_, err := s.client.Post(ctx, endpoint, body, nil, &gophercloud.RequestOpts{OkCodes: []int{http.StatusNoContent}})
	if err != nil {
		return fmt.Errorf("activate Octavia L7 policies: %w", err)
	}
	return nil
}

// policyActivationOptions captures modifiers which can change how a policy
// routes requests independently of the action and the rule list.
func policyActivationOptions(hostname, targets, fixed, redirect, rewrite, cors, request, response, requestPolicy, requestMirrors, externalAuth json.RawMessage) string {
	var options = map[string]any{}
	for _, option := range []struct {
		name  string
		value json.RawMessage
	}{
		{"hostname", hostname}, {"targets", targets}, {"fixed_response", fixed}, {"redirect_request", redirect},
		{"request_url_rewrite", rewrite}, {"request_cors", cors}, {"request_headers", request}, {"response_headers", response}, {httpRequestPolicyField, requestPolicy}, {requestMirrorsField, requestMirrors},
		{policyExternalAuthField, externalAuth},
	} {
		if len(option.value) == 0 {
			continue
		}
		var value any
		decoder := json.NewDecoder(bytes.NewReader(option.value))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			options[option.name] = string(option.value)
			continue
		}
		if value == nil {
			continue
		}
		switch option.name {
		case "hostname":
			if value == "" {
				continue
			}
		case "targets", "request_headers", "response_headers", requestMirrorsField:
			if list, ok := value.([]any); ok && len(list) == 0 {
				continue
			}
		}
		options[option.name] = value
	}
	if len(options) == 0 {
		return ""
	}
	encoded, _ := json.Marshal(options)
	return string(encoded)
}
