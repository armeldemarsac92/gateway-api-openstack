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
	"net/http"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
)

var (
	errTCPRejectionUnavailable = errors.New("waiting for regional Octavia TCP connection rejection support")
	errTCPRejectionReadback    = errors.New("waiting for complete Octavia TCP connection rejection readback")
)

func (p *Provider) observeTCPConnectionRejectionCapability(ctx context.Context, desired *desiredGatewayGraph, actual observedGatewayGraph) {
	if !hasTCPRejectionTarget(*desired) {
		return
	}
	available := false
	if actual.loadBalancer == nil || actual.loadBalancer.Provider == "amphora" {
		available, _ = p.TCPConnectionRejectionAvailable(ctx)
	}
	for i := range desired.routes {
		route := &desired.routes[i]
		route.tcpRejectionUnavailable = route.spec.TCPConnectionRejection != nil && !available
	}
}

func hasTCPRejectionTarget(desired desiredGatewayGraph) bool {
	return slices.ContainsFunc(desired.routes, func(route desiredRoute) bool { return route.spec.TCPConnectionRejection != nil })
}

func hasTCPRejectionReadback(observed observedGatewayGraph) bool {
	return observed.protocol == cloud.ListenerProtocolTCP &&
		slices.ContainsFunc(observed.routes, func(route observedRouteGraph) bool { return route.tcpRejection.Present })
}

func tcpRejectionBatchRequired(desired desiredRoute, observed observedRouteGraph) bool {
	return desired.protocol() == cloud.ListenerProtocolTCP &&
		(desired.spec.TCPConnectionRejection != nil || observed.tcpRejection.Value != nil || observed.tcpRejection.Invalid)
}

func planTCPMembersAndRejection(desired desiredRoute, observed observedRouteGraph) (*routeMutation, []string, error) {
	step, ids, changed, err := tcpMembersAndRejection(desired, observed, false)
	if !changed || err != nil {
		return nil, ids, err
	}
	return &step, ids, nil
}

// A pending batch can still hold old rows marked for deletion. Their
// destinations do not form part of the persisted replacement payload.
func tcpMembersAndRejection(desired desiredRoute, observed observedRouteGraph, pending bool) (routeMutation, []string, bool, error) {
	if !observed.tcpRejection.Present || observed.tcpRejection.Invalid || len(observed.memberBatchUnreported) != 0 {
		return routeMutation{}, nil, false, errTCPRejectionReadback
	}
	if err := crossCheckCompleteChildInventory("pool", observed.pool.ID, "member", observed.pool.Members, observed.members, func(member pools.Member) string { return member.ID }); err != nil {
		return routeMutation{}, nil, false, errors.Join(errTCPRejectionReadback, err)
	}
	byAddress := make(map[string]pools.Member, len(observed.members))
	for _, member := range observed.members {
		if pending && member.ProvisioningStatus == "PENDING_DELETE" {
			continue
		}
		byAddress[memberKey(member.Address, member.ProtocolPort)] = member
	}
	current, wanted := observed.tcpRejection.Value, desired.spec.TCPConnectionRejection
	changed := (current == nil) != (wanted == nil) || current != nil && wanted != nil && *current != *wanted ||
		len(byAddress) != len(desired.members)
	members := make([]pools.BatchUpdateMemberOpts, 0, len(desired.members))
	var memberIDs []string
	for _, member := range desired.members {
		existing, matched := byAddress[memberKey(member.Address, member.Port)]
		opts, err := tcpRejectionBatchMember(desired, member, existing)
		if err != nil {
			return routeMutation{}, nil, false, err
		}
		members = append(members, opts)
		if matched {
			memberIDs = append(memberIDs, existing.ID)
		}
		tags := slices.Clone(existing.Tags)
		slices.Sort(tags)
		changed = changed || !matched || existing.SubnetID != *opts.SubnetID || !existing.AdminStateUp ||
			existing.Weight != *opts.Weight || existing.Backup != *opts.Backup || !slices.Equal(tags, opts.Tags)
	}
	return routeMutation{kind: routeMutationReplaceTCPMembers, id: observed.pool.ID, tcpMembers: members}, memberIDs, changed, nil
}

func tcpRejectionBatchMember(desired desiredRoute, member cloud.Member, existing pools.Member) (pools.BatchUpdateMemberOpts, error) {
	opts := pools.BatchUpdateMemberOpts{
		Address: member.Address, ProtocolPort: member.Port, SubnetID: &desired.spec.MemberSubnetID,
		AdminStateUp: new(true), Backup: new(false), Weight: desired.memberWeight(member),
	}
	weightTag := desired.nodePortWeightTag()
	if existing.ID == "" {
		tags, err := desired.identity.RouteTags(roleMember)
		if err != nil {
			return opts, err
		}
		opts.Tags = tags
		opts.Name = new(resourceName(desired.spec.Identity, roleMember))
		if opts.Weight == nil {
			opts.Weight = new(1)
		}
	} else {
		opts.Tags = slices.Clone(existing.Tags)
		opts.Name = &existing.Name
		if existing.MonitorAddress != "" {
			opts.MonitorAddress = &existing.MonitorAddress
		}
		if existing.MonitorPort != 0 {
			opts.MonitorPort = &existing.MonitorPort
		}
		if opts.Weight == nil {
			opts.Weight = &existing.Weight
		}
		if member.Weight == 0 || member.Draining {
			opts.Backup = &existing.Backup
		}
	}
	if weightTag != "" {
		tagged := slices.Contains(opts.Tags, weightTag)
		switch {
		case member.Weight > 0 && !tagged:
			opts.Tags = append(opts.Tags, weightTag)
		case member.Weight == 0 && tagged:
			opts.Tags = slices.DeleteFunc(opts.Tags, func(tag string) bool { return tag == weightTag })
			opts.Weight = new(1)
		}
	}
	slices.Sort(opts.Tags)
	return opts, nil
}

func tcpRejectionProvisioningPending(route desiredRoute, observed observedRouteGraph) string {
	if route.protocol() == cloud.ListenerProtocolTCP && observed.tcpRejection.Present {
		return observed.backendProvisioningIssue(true, true)
	}
	return ""
}

func defaultPoolCanServe(route desiredRoute) bool {
	fraction := route.spec.TCPConnectionRejection
	return fraction != nil && fraction.RejectWeight == fraction.TotalWeight ||
		slices.ContainsFunc(route.members, func(member cloud.Member) bool { return !member.Draining })
}

// An explicit rejection of the batch leaves the old member set serving.
// Reprove the owned default before withdrawing it. An ambiguous response
// follows ordinary re-observation because Octavia may have accepted the write.
func (p *Provider) withdrawAfterRejectedTCPBatch(ctx context.Context, desired desiredGatewayGraph, index int, result *cloud.GatewayGraphResult, failure error) (bool, error) {
	if !slices.ContainsFunc([]int{http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound, http.StatusMethodNotAllowed,
		http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity, http.StatusNotImplemented}, func(code int) bool { return gophercloud.ResponseCodeIs(failure, code) }) {
		return false, nil
	}
	actual, err := p.observeGatewayGraph(ctx, desired)
	if err != nil || actual.loadBalancer != nil && actual.loadBalancer.ProvisioningStatus != "ACTIVE" {
		return false, err
	}
	if actual.loadBalancer == nil || index < len(actual.routes) && actual.routes[index].pool == nil {
		// An independently observed disappearance can be handled by normal
		// graph staging. A 404 on an existing pool may identify its subnet.
		return isNotFound(failure), nil
	}
	if index >= len(actual.routes) {
		return false, nil
	}
	route := actual.routes[index]
	if route.pool.ProvisioningStatus != "ERROR" && route.backendProvisioningIssue(true, true) != "" {
		return false, nil
	}
	frontends := tcpRejectionPoolListeners(actual, route.pool.ID)
	listener := slices.IndexFunc(frontends, func(listener listeners.Listener) bool {
		return listener.ProvisioningStatus == "ACTIVE" || listener.ProvisioningStatus == "ERROR"
	})
	if listener == -1 {
		return false, nil
	}
	return true, p.withdrawTCPRejectionDefault(ctx, desired, actual, index, frontends[listener].ID, result)
}

func tcpRejectionPoolListeners(actual observedGatewayGraph, poolID string) []listeners.Listener {
	if actual.listener != nil && actual.listener.DefaultPoolID == poolID {
		return []listeners.Listener{*actual.listener}
	}
	var listenersForPool []listeners.Listener
	for _, listener := range actual.scopedListeners {
		if listener.DefaultPoolID == poolID {
			listenersForPool = append(listenersForPool, listener)
		}
	}
	return listenersForPool
}

func (p *Provider) withdrawTCPRejectionDefault(ctx context.Context, desired desiredGatewayGraph, actual observedGatewayGraph, index int, listenerID string, result *cloud.GatewayGraphResult) error {
	step := clearListenerDefaultPool(cloud.ListenerProtocolTCP)
	step.listenerID = listenerID
	if validate := desired.spec.ValidateMutation; validate != nil {
		if err := validate(ctx); err != nil {
			return err
		}
	}
	err := p.executeGraphMutation(ctx, desired, actual, step)
	if err != nil && !isNotFound(err) {
		return err
	}
	outcome := p.progressingOutcome(step.message)
	outcome.MutationSubmitted = err == nil
	result.Outcome = outcome
	result.Routes[index].Result.Outcome = outcome
	result.Routes[index].Result.ObservationDeferred = false
	return nil
}
