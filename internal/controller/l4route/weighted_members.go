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

package l4route

import (
	"cmp"
	"errors"
	"fmt"
	"math/big"
	"net/netip"
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

const maxOctaviaUDPMemberWeight = 256

var errUDPBackendWithoutReadyEndpoints = errors.New("positive-weight backend has no ready endpoints")

type weightedServiceMembers struct {
	weight  int32
	members []cloud.Member
}

// weightedUDPMembers divides each backend's weight among its unique ready
// endpoints, then adds the shares of any endpoint reached through multiple backends.
func weightedUDPMembers(groups []weightedServiceMembers) ([]cloud.Member, error) {
	type readyGroup struct {
		weight    int64
		endpoints map[netip.AddrPort]struct{}
	}
	readyGroups := make([]readyGroup, 0, len(groups))
	shares := make(map[netip.AddrPort]*big.Int)
	denominator := big.NewInt(1)
	var count, commonFactor big.Int
	for i, group := range groups {
		if group.weight < 0 {
			return nil, fmt.Errorf("UDP backendRef %d has a negative weight", i+1)
		}
		if group.weight == 0 {
			continue
		}
		ready := make(map[netip.AddrPort]struct{}, len(group.members))
		for _, member := range group.members {
			address, err := netip.ParseAddr(member.Address)
			if err != nil || member.Port < 1 || member.Port > 65535 {
				return nil, fmt.Errorf("UDP backendRef %d has an invalid endpoint %q port %d", i+1, member.Address, member.Port)
			}
			endpoint := netip.AddrPortFrom(address, uint16(member.Port))
			if _, found := shares[endpoint]; !found {
				shares[endpoint] = new(big.Int)
			}
			if !member.Draining {
				ready[endpoint] = struct{}{}
			}
		}
		if len(ready) == 0 {
			return nil, fmt.Errorf("UDP backendRef %d: %w", i+1, errUDPBackendWithoutReadyEndpoints)
		}
		readyGroups = append(readyGroups, readyGroup{weight: int64(group.weight), endpoints: ready})

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
			total := shares[endpoint]
			total.Add(total, &share)
		}
	}

	// Dividing by the GCD yields the smallest integral ratio and leaves draining
	// endpoints at zero. If it exceeds Octavia's limit, no exact ratio can fit.
	var gcd big.Int
	for _, total := range shares {
		gcd.GCD(nil, nil, &gcd, total)
	}
	members := make([]cloud.Member, 0, len(shares))
	for endpoint, total := range shares {
		total.Quo(total, &gcd)
		if !total.IsInt64() || total.Int64() > maxOctaviaUDPMemberWeight {
			return nil, fmt.Errorf("the exact UDP backend ratio exceeds Octavia's maximum member weight of %d", maxOctaviaUDPMemberWeight)
		}
		members = append(members, cloud.Member{
			Address:  endpoint.Addr().String(),
			Port:     int(endpoint.Port()),
			Draining: total.Sign() == 0,
			Weight:   int(total.Int64()),
		})
	}
	slices.SortFunc(members, func(left, right cloud.Member) int {
		return cmp.Or(cmp.Compare(left.Address, right.Address), cmp.Compare(left.Port, right.Port))
	})
	return members, nil
}
