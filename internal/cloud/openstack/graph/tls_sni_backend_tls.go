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
	"crypto/sha256"
	"fmt"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
)

type sniBackendTLSIdentity struct {
	Hostname    string
	CARevision  string
	ServicePort int
}

func sameSNIBackendTLS(a, b *cloud.BackendTLSValidation) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Hostname == b.Hostname && a.CAConfigMapUID == b.CAConfigMapUID && a.CADigest == b.CADigest
}

func sniBackendServiceTag(route cloud.TLSSNIRoute) string {
	return sniTLSBackendServiceTag(route.ServiceUID, route.ServicePort)
}

func sniTLSBackendServiceTag(uid string, port int) string {
	hash := sha256.Sum256(fmt.Appendf(nil, "%s\x00%d", uid, port))
	return openstackidentity.Tag("sni-backend", fmt.Sprintf("%x", hash))
}

// An older protected pool without a destination tag still protects aliases
// on its owning route. A tagged pool protects exactly its Service and port.
func (fragment *observedSNIFragment) protectsService(key sniFragmentKey, target desiredSNIFragment) bool {
	if !fragment.protectedBackendTLS() {
		return false
	}
	if target.route.ServicePort != 0 && slices.Contains(fragment.pool.Tags, sniBackendServiceTag(target.route)) {
		return true
	}
	for _, service := range target.route.BackendServices {
		if slices.Contains(fragment.pool.Tags, sniTLSBackendServiceTag(service.UID, service.Port)) {
			return true
		}
	}
	return key.sniRouteKey == target.key.sniRouteKey && !slices.ContainsFunc(fragment.pool.Tags, func(tag string) bool {
		name, _, valid := openstackidentity.ParseTag(tag)
		return valid && name == "sni-backend"
	})
}

// Keep one obsolete pool as durable proof of each protected destination
// until Octavia reports its authenticated replacement and its new selector.
func sniBackendTLSWitnesses(wanted map[sniFragmentKey]desiredSNIFragment, observed map[sniFragmentKey]*observedSNIFragment, retire []sniFragmentKey) map[sniFragmentKey]bool {
	witnesses := make(map[sniFragmentKey]bool)
	for _, target := range wanted {
		if target.route.BackendTLS == nil {
			continue
		}
		if successor := observed[target.key]; successor != nil && successor.rule != nil && successor.matches(target) {
			continue
		}
		for _, key := range retire {
			if _, stillWanted := wanted[key]; !stillWanted && observed[key].protectsService(key, target) {
				witnesses[key] = true
				break
			}
		}
	}
	return witnesses
}

func (fragment *observedSNIFragment) protectedBackendTLS() bool {
	return hasObservedBackendTLS(*fragment.pool, fragment.backendTLS) || hasBackendTLSSettings(*fragment.pool, fragment.backendTLS)
}

func (fragment *observedSNIFragment) closedBackendPool() bool {
	return fragment.pool.ProvisioningStatus == "ACTIVE" && fragment.backendTLS.AdminObserved && !fragment.pool.AdminStateUp
}

func (fragment *observedSNIFragment) hasForwardingMembers() bool {
	return len(fragment.members) != 0 && !fragment.closedBackendPool()
}

func (fragment *observedSNIFragment) matchesBackendTransport(target desiredSNIFragment) bool {
	if target.route.BackendTLS == nil {
		return fragment.pool.AdminStateUp && !fragment.protectedBackendTLS()
	}
	return target.backendTLS.phase == backendTLSVerified &&
		slices.Contains(fragment.pool.Tags, backendTLSManagedTag) &&
		slices.Contains(fragment.pool.Tags, backendTLSRevisionTag(target.route.BackendTLS)) &&
		slices.Contains(fragment.pool.Tags, sniBackendServiceTag(target.route)) &&
		!differentBackendTLSRevision(fragment.pool.Tags, backendTLSRevisionTag(target.route.BackendTLS))
}

func sniDisablePoolStep(fragment *observedSNIFragment) graphMutation {
	return graphMutation{kind: graphSNIMutation, sniWithdrawal: true, redactTLSError: true,
		sni:     sniMutation{kind: sniDisableBackendTLSPool, parentID: fragment.pool.ID},
		message: "Closing a TCP pool without verified backend TLS authorization"}
}

// prepareSNIRouteBackendTLS turns unavailable TLS backends into closed
// selectors before planning creates and attaches any new resources.
func (p *Provider) prepareSNIRouteBackendTLS(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) error {
	if !cloud.IsTCPTLSListenerProtocol(desired.protocol) || desired.spec.Gateway.Disposition != cloud.GraphEnsure {
		return nil
	}
	hasStrictPool := slices.ContainsFunc(desired.sni, func(target desiredSNIFragment) bool { return target.route.BackendTLS != nil })
	for _, fragment := range actual.sni {
		hasStrictPool = hasStrictPool || fragment.protectedBackendTLS()
	}
	provider := desired.spec.Gateway.Spec.Provider
	if actual.loadBalancer != nil {
		provider = actual.loadBalancer.Provider
	}
	capable := hasStrictPool && p.backendTLSPolicy && p.backendCA != nil && p.octavia.VerifyGatewayCapabilities(ctx, provider, tcpBackendTLSCapabilities()...) == nil
	if err := ctx.Err(); err != nil {
		return err
	}
	desired.sniHeldClosed = make(map[sniFragmentKey]bool)
	desired.sniBlockedRoutes = make(map[string]bool)
	var targets []desiredSNIFragment
	for _, target := range desired.sni {
		tls := target.route.BackendTLS
		blocked := target.route.Deny
		if tls != nil {
			var observed observedRouteGraph
			if fragment := actual.sni[target.key]; fragment != nil {
				observed.pool, observed.backendTLS = fragment.pool, fragment.backendTLS
			}
			route := desiredRoute{spec: cloud.RouteSpec{Identity: target.route.Identity, BackendTLS: tls}}
			clientIdentity := desired.spec.Gateway.BackendClientCertificate
			clientIdentity.Denied = clientIdentity.Denied || clientIdentity.Reference != ""
			target.backendTLS = p.observeBackendTLS(ctx, route, observed, func(bool, bool) bool { return capable },
				desired.spec.Gateway.BackendCAOwned, clientIdentity)
			blocked = target.backendTLS.denies()
		} else if !target.route.Deny && (!capable || !desired.spec.Gateway.SNIPoliciesObserved) {
			for key, fragment := range actual.sni {
				if fragment.protectsService(key, target) ||
					fragment.protectedBackendTLS() && fragment.selectorHostname() == target.route.Hostname {
					blocked = true
					break
				}
			}
		}
		if !blocked {
			targets = append(targets, target)
			continue
		}

		desired.sniBlockedRoutes[target.key.uid] = true
		host := target.route.Hostname
		hasSelector := false
		for key, fragment := range actual.sni {
			protected := fragment.protectedBackendTLS()
			holdSelector := fragment.selectorHostname() == host && (!target.route.Deny || protected)
			holdService := (tls != nil || !capable || !desired.spec.Gateway.SNIPoliciesObserved) && fragment.protectsService(key, target)
			if key == target.key && !target.route.Deny || holdSelector || holdService ||
				protected && key.sniRouteKey == target.key.sniRouteKey && fragment.rule == nil {
				// A disabled terminating TCP pool remains a rejecting
				// selector in Amphora, shielding any broader hostname.
				desired.sniHeldClosed[key] = true
				hasSelector = hasSelector || fragment.hasActiveSelector(host)
			}
		}
		if !hasSelector && hasSNIFallback(host, desired.sni, actual.sni) {
			if target.route.Deny {
				targets = append(targets, target)
				continue
			}
			guards, err := buildSNITargets(desired.spec.Identity, []cloud.TLSSNIRoute{{
				Identity: target.route.Identity, Hostname: host, Deny: true, Passthrough: target.route.Passthrough,
			}})
			if err != nil {
				return err
			}
			targets = append(targets, guards[0])
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	desired.sni = targets
	return nil
}

// A denied target can supersede an older selector that still carries members.
// A pool-admin change also works when strict TLS capability or CA access is gone.
func sniRestrictMembers(fragment *observedSNIFragment) graphMutation {
	if fragment.protectedBackendTLS() {
		return sniDisablePoolStep(fragment)
	}
	return sniClearMembersStep(fragment)
}
