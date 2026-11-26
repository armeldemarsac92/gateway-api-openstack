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

package routebackend

import (
	"cmp"
	"errors"
	"fmt"
	"math/big"
	"net/netip"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

const maxOctaviaMemberWeight = 256

// ErrWeightedMembersUnrepresentable reports an exact ratio or endpoint count
// that cannot fit in one Octavia pool.
var ErrWeightedMembersUnrepresentable = errors.New("weights cannot be represented in a single Octavia pool")

// ErrBackendWithoutReadyEndpoints reports an empty positive-weight backend.
var ErrBackendWithoutReadyEndpoints = errors.New("positive-weight backend has no ready endpoints")

// WeightedServiceMembers pairs a Service backendRef weight with eligible members.
type WeightedServiceMembers struct {
	Weight  int32
	Members []cloud.Member
}

// WeightedMembers divides each backend's weight among its unique eligible
// destinations, then adds the shares of destinations reached through multiple backends.
func WeightedMembers(protocol string, groups []WeightedServiceMembers) ([]cloud.Member, error) {
	type readyGroup struct {
		weight    int64
		endpoints map[netip.AddrPort]struct{}
	}
	type weightedDestination struct {
		member cloud.Member
		share  big.Int
	}
	readyGroups := make([]readyGroup, 0, len(groups))
	destinations := make(map[netip.AddrPort]*weightedDestination)
	denominator := big.NewInt(1)
	var count, commonFactor big.Int
	for i, group := range groups {
		if group.Weight < 0 {
			return nil, fmt.Errorf("%s backendRef %d has a negative weight", protocol, i+1)
		}
		if group.Weight == 0 {
			continue
		}
		ready := make(map[netip.AddrPort]struct{}, len(group.Members))
		for _, member := range group.Members {
			address, err := netip.ParseAddr(member.Address)
			if err != nil || member.Port < 1 || member.Port > 65535 {
				return nil, fmt.Errorf("%s backendRef %d has an invalid endpoint %q port %d", protocol, i+1, member.Address, member.Port)
			}
			endpoint := netip.AddrPortFrom(address, uint16(member.Port))
			destination := destinations[endpoint]
			if destination == nil {
				member.Address = address.String()
				destinations[endpoint] = &weightedDestination{member: member}
			} else if destination.member.RoutedExternal != member.RoutedExternal {
				return nil, fmt.Errorf("%s backends require different subnet bindings for the same destination: %w", protocol, ErrWeightedMembersUnrepresentable)
			} else if destination.member.ProviderID != member.ProviderID {
				return nil, fmt.Errorf("%s backends have conflicting provider IDs for the same destination", protocol)
			}
			if !member.Draining {
				ready[endpoint] = struct{}{}
			}
		}
		if len(ready) == 0 {
			return nil, fmt.Errorf("%s backendRef %d: %w", protocol, i+1, ErrBackendWithoutReadyEndpoints)
		}
		readyGroups = append(readyGroups, readyGroup{weight: int64(group.Weight), endpoints: ready})

		// Use the least common multiple of the endpoint counts as the scale.
		// Counts across up to 16 backendRefs can exceed machine integers when combined.
		count.SetInt64(int64(len(ready)))
		commonFactor.GCD(nil, nil, denominator, &count)
		denominator.Quo(denominator, &commonFactor)
		denominator.Mul(denominator, &count)
	}
	if len(readyGroups) == 0 {
		return nil, nil
	}

	var share, weight big.Int
	for _, group := range readyGroups {
		count.SetInt64(int64(len(group.endpoints)))
		weight.SetInt64(group.weight)
		share.Quo(denominator, &count)
		share.Mul(&share, &weight)
		for endpoint := range group.endpoints {
			total := &destinations[endpoint].share
			total.Add(total, &share)
		}
	}

	// Dividing by the GCD yields the smallest integral ratio and leaves draining
	// endpoints at zero. If it exceeds Octavia's limit, no exact ratio can fit.
	var gcd big.Int
	for _, destination := range destinations {
		gcd.GCD(nil, nil, &gcd, &destination.share)
	}
	members := make([]cloud.Member, 0, len(destinations))
	active := 0
	for _, destination := range destinations {
		total := &destination.share
		total.Quo(total, &gcd)
		if !total.IsInt64() || total.Int64() > maxOctaviaMemberWeight {
			return nil, fmt.Errorf("the exact %s backend ratio exceeds Octavia's maximum member weight of %d: %w", protocol, maxOctaviaMemberWeight, ErrWeightedMembersUnrepresentable)
		}
		member := destination.member
		member.Draining = total.Sign() == 0
		member.Weight = int(total.Int64())
		if !member.Draining {
			active++
		}
		members = append(members, member)
	}
	if protocol == cloud.ListenerProtocolTCP && active > cloud.MaxTCPRoundRobinMembers {
		return nil, fmt.Errorf("weighted TCP forwarding requires at most %d active endpoints in one HAProxy pool: %w", cloud.MaxTCPRoundRobinMembers, ErrWeightedMembersUnrepresentable)
	}
	slices.SortFunc(members, func(left, right cloud.Member) int {
		return cmp.Or(cmp.Compare(left.Address, right.Address), cmp.Compare(left.Port, right.Port))
	})
	return members, nil
}
