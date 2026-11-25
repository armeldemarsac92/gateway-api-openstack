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

package graph

import (
	"context"
	"errors"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/backendca"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/l7policies"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

type frontendCAObservation struct {
	ref                 string
	needsPublish        bool
	canClear            bool
	issue               string
	fallbackState       octavia.ClientAuthFallback
	fallbackTracked     bool
	fallbackUnavailable bool
}

// The tag is written atomically with fallback and survives controller restarts.
// Only an untracked listener may interpret an omitted field as historical.
var frontendFallbackTag = openstackidentity.Tag("frontend-client-auth-fallback", "v1")

func (ca frontendCAObservation) canCreate(spec cloud.ScopedListener) bool {
	return !ca.fallbackUnavailable && (spec.TLS == nil || spec.TLS.ClientValidation == nil || ca.ref != "")
}

func (ca frontendCAObservation) matches(listener listeners.Listener, spec *cloud.ListenerTLS) bool {
	if listener.ClientCRLContainerRef != "" || ca.fallbackUnavailable {
		return false
	}
	if spec.ClientValidation != nil && spec.ClientValidation.InsecureFallback {
		if !ca.fallbackTracked || ca.fallbackState != octavia.ClientAuthFallbackEnabled {
			return false
		}
	} else if ca.fallbackState != octavia.ClientAuthFallbackDisabled &&
		(ca.fallbackTracked || ca.fallbackState != octavia.ClientAuthFallbackAbsent) {
		return false
	}
	if spec.ClientValidation != nil {
		return ca.ref != "" && listener.ClientCATLSContainerRef == ca.ref &&
			listener.ClientAuthentication == string(listeners.ClientAuthenticationMandatory)
	}
	return listener.ClientCATLSContainerRef == "" &&
		(listener.ClientAuthentication == "" || listener.ClientAuthentication == string(listeners.ClientAuthenticationNone))
}

func (ca frontendCAObservation) canUpdate(listener listeners.Listener, spec *cloud.ListenerTLS) bool {
	if listener.ClientCRLContainerRef != "" || ca.fallbackUnavailable || ca.fallbackNeedsClear(spec) {
		return false
	}
	if spec.ClientValidation != nil {
		return ca.ref != ""
	}
	return ca.matches(listener, spec) || ca.canClear
}

func (ca frontendCAObservation) fallbackNeedsClear(spec *cloud.ListenerTLS) bool {
	switch ca.fallbackState {
	case octavia.ClientAuthFallbackEnabled:
		validation := spec.ClientValidation
		return !ca.fallbackTracked || ca.fallbackUnavailable || ca.ref == "" ||
			spec.CertificateRef == "" || validation == nil || !validation.InsecureFallback
	case octavia.ClientAuthFallbackUnreadable:
		return true
	case octavia.ClientAuthFallbackAbsent:
		return ca.fallbackTracked
	default:
		return false
	}
}

// Observe only owned, verified revisions. A failed or incomplete Barbican
// lookup removes authorization for the affected port before route discovery.
func (p *Provider) observeFrontendCAs(ctx context.Context, desired desiredGatewayGraph, actual *observedGatewayGraph) {
	disposition := desired.spec.Gateway.Disposition
	if disposition != cloud.GraphEnsure && disposition != cloud.GraphWithdrawTLS {
		return
	}
	var owned []backendca.Revision
	listed := false
	var listErr error
	capabilityChecked, fallbackAvailable := false, false
	for _, spec := range desired.spec.Gateway.Spec.ScopedListeners {
		if spec.TLS == nil {
			continue
		}
		var listener listeners.Listener
		if i := slices.IndexFunc(actual.scopedListeners, func(l listeners.Listener) bool { return l.ProtocolPort == spec.Port }); i >= 0 {
			listener = actual.scopedListeners[i]
		}
		observed := frontendCAObservation{
			fallbackState:   actual.clientAuthFallback[listener.ID],
			fallbackTracked: slices.Contains(listener.Tags, frontendFallbackTag),
		}
		if ca := spec.TLS.ClientValidation; hasAuthorizedListener(spec) && ca != nil {
			if ca.InsecureFallback && !capabilityChecked {
				capabilityChecked = true
				fallbackAvailable = p.octavia.VerifyGatewayCapabilities(ctx, actual.loadBalancer.Provider, octavia.GatewayCapabilityFrontendAuthFallback) == nil
			}
			if ca.InsecureFallback && !fallbackAvailable {
				observed.fallbackUnavailable = true
				observed.issue = "Octavia frontend client certificate validation mode AllowInsecureFallback is unavailable in this region"
			} else if p.backendCA == nil {
				observed.issue = "The Barbican client for HTTPS client CAs is unavailable"
			} else {
				rev, err := p.backendCA.Find(ctx, desired.spec.Identity.GatewayUID, ca.ConfigMapUID, ca.Digest, listener.ClientCATLSContainerRef)
				if err == nil {
					observed.ref = rev.Reference()
					observed.needsPublish = observed.ref == ""
				} else {
					observed.issue = frontendCAReadIssue(err)
				}
			}
		} else if hasAuthorizedListener(spec) && p.backendCA != nil && disposition == cloud.GraphEnsure && desired.spec.Gateway.BackendCAOwned &&
			listener.ClientCATLSContainerRef != "" && listener.ClientAuthentication == string(listeners.ClientAuthenticationMandatory) {
			// A valid spec can explicitly remove validation. Only undo native
			// settings on a listener whose current CA this Gateway published.
			if !listed {
				owned, listErr = p.backendCA.List(ctx, desired.spec.Identity.GatewayUID)
				listed = true
			}
			if listErr != nil {
				observed.issue = frontendCAReadIssue(listErr)
			}
			observed.canClear = slices.ContainsFunc(owned, func(rev backendca.Revision) bool { return rev.Reference() == listener.ClientCATLSContainerRef })
		}
		if actual.frontendCA == nil {
			actual.frontendCA = make(map[int]frontendCAObservation)
		}
		actual.frontendCA[spec.Port] = observed
	}
}

// A disabled listener with an uncertain fallback mode must read back false
// before it can be reopened or receive another request to set fallback.
func (actual observedGatewayGraph) nextFrontendFallbackClear(desired desiredGatewayGraph) *graphMutation {
	if desired.spec.Gateway.Disposition != cloud.GraphEnsure || actual.loadBalancer == nil || actual.loadBalancer.ProvisioningStatus != "ACTIVE" {
		return nil
	}
	configured := scopedHTTPListeners(desired)
	for _, listener := range actual.scopedListeners {
		spec, present := configured[listener.ProtocolPort]
		if !present || spec.TLS == nil || listener.Protocol != terminatedHTTPS || listener.ProvisioningStatus != "ACTIVE" ||
			!actual.adminObserved[listener.ID] || listener.AdminStateUp {
			continue
		}
		ca := actual.frontendCA[spec.Port]
		// Listener identity authorizes closure. Changing client auth also
		// requires evidence that this Gateway managed the fallback or CA.
		ownsClientAuth := ca.fallbackTracked || ca.canClear || ca.ref != "" && ca.ref == listener.ClientCATLSContainerRef
		if ownsClientAuth && ca.fallbackNeedsClear(spec.TLS) {
			return &graphMutation{kind: graphDisableFrontendClientAuthFallback, listenerID: listener.ID, scopedListener: spec,
				redactTLSError: true, message: "Clearing insecure client certificate fallback on a disabled Octavia HTTPS listener"}
		}
	}
	return nil
}

func (actual observedGatewayGraph) nextFrontendCAMutation(desired desiredGatewayGraph) *graphMutation {
	for _, spec := range desired.spec.Gateway.Spec.ScopedListeners {
		if actual.frontendCA[spec.Port].needsPublish {
			message := "Publishing an Octavia HTTPS client CA revision"
			if !desired.spec.Gateway.BackendCAOwned {
				message = "Recording ownership of Gateway CA revisions"
			}
			return &graphMutation{kind: graphPublishFrontendCA, scopedListener: spec, message: message}
		}
	}
	return nil
}

// Only the classified cause is safe to expose. Barbican errors may contain
// certificate references or response bodies.
func frontendCAReadIssue(err error) string {
	category, ok := cloud.ErrorCategoryOf(classifyOpenStackError(err))
	if !ok {
		return "Waiting to verify the HTTPS client CA in Barbican"
	}
	switch category {
	case cloud.ErrorCategoryAuthentication, cloud.ErrorCategoryAuthorization:
		return "Barbican denied access to the HTTPS client CA"
	case cloud.ErrorCategoryTimeout, cloud.ErrorCategoryRetryableService, cloud.ErrorCategoryRateLimit:
		return "Barbican is temporarily unavailable for HTTPS client CA verification"
	default:
		return "Waiting to verify the HTTPS client CA in Barbican"
	}
}

func (actual observedGatewayGraph) closedDeniedHTTPSPorts(spec cloud.GatewaySpec) []int {
	if actual.loadBalancer != nil && (actual.scopedListeners == nil || actual.loadBalancer.ProvisioningStatus != "ACTIVE") {
		return nil
	}
	var ports []int
	for _, config := range spec.ScopedListeners {
		if hasAuthorizedListener(config) && !actual.frontendCA[config.Port].fallbackUnavailable {
			continue
		}
		i := slices.IndexFunc(actual.scopedListeners, func(listener listeners.Listener) bool { return listener.ProtocolPort == config.Port })
		if i < 0 || actual.scopedListeners[i].ProvisioningStatus == "ACTIVE" &&
			actual.adminObserved[actual.scopedListeners[i].ID] && !actual.scopedListeners[i].AdminStateUp {
			ports = append(ports, config.Port)
		}
	}
	return ports
}

// Deleting the listener cascades Octavia's L7 policies and their rules.
// Prove ownership of every dependent first, and preserve pools and other ports.
func (p *Provider) deleteUnclosableFrontend(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, listenerID string) error {
	i := slices.IndexFunc(actual.scopedListeners, func(l listeners.Listener) bool { return l.ID == listenerID })
	if i < 0 || !actual.defaultPoolsObserved || actual.scopedListeners[i].DefaultPoolID != "" {
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, errors.New("cannot prove the HTTPS listener has no default pool"))
	}
	listener := actual.scopedListeners[i]
	inventory := p.readHTTPSPolicyInventory(ctx, listener)
	if inventory.err != nil {
		return frontendWithdrawalProviderError(inventory.err, "Could not verify the HTTPS listener policy inventory")
	}
	if !inventory.complete {
		return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, errors.New("cannot verify the HTTPS listener policy inventory"))
	}
	for _, entry := range inventory.items {
		policy := entry.Policy
		role, known := readRouteResourceRole(policy.Tags, rolePolicyExact, rolePolicyPrefix)
		owner, owned := desired.identity.RouteOwner(policy.Tags, role.raw)
		if !known || !owned || policy.ProjectID != p.projectID || policy.ListenerID != listener.ID {
			return cloud.NewProviderError(cloud.ErrorCategoryOwnershipConflict, errors.New("cannot prove ownership of an HTTPS listener policy"))
		}
		observed, err := p.observeRoutePolicy(ctx, routeGraphOwner{identity: desired.identity, recovery: &owner}, policy, listener.ID, role)
		if err != nil {
			return frontendWithdrawalProviderError(err, "Could not verify the HTTPS listener rule inventory")
		}
		if observed.pending() {
			return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, errors.New("waiting for HTTPS policy provisioning"))
		}
		rules := make([]l7policies.Rule, 0, len(observed.rules))
		for _, rule := range observed.rules {
			if rule.rule.ProjectID != p.projectID {
				return cloud.NewProviderError(cloud.ErrorCategoryOwnershipConflict, errors.New("cannot prove ownership of an HTTPS listener rule"))
			}
			rules = append(rules, rule.rule)
		}
		if err := crossCheckCompleteChildInventory("L7 policy", policy.ID, "rule", policy.Rules, rules, func(rule l7policies.Rule) string { return rule.ID }); err != nil {
			return err
		}
	}
	if desired.spec.ValidateMutation != nil {
		if err := desired.spec.ValidateMutation(ctx); err != nil {
			return err
		}
	}
	err := p.executeGatewayDeletionStep(ctx, listenerDeletionStep(listenerID))
	return frontendWithdrawalProviderError(err, "Could not remove the HTTPS listener with an unreadable client CA")
}

func frontendWithdrawalProviderError(err error, message string) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return err
	}
	category, ok := cloud.ErrorCategoryOf(classifyOpenStackError(err))
	if !ok {
		category = cloud.ErrorCategoryResourceFailure
	}
	return cloud.NewProviderError(category, errors.New(message))
}
