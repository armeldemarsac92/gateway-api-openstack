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

const udpWeightErrorDenominator = 256

// ErrWeightedMembersUnrepresentable reports a ratio, binding or endpoint count
// that cannot fit in one Octavia pool.
var ErrWeightedMembersUnrepresentable = errors.New("weights cannot be represented in a single Octavia pool")

// ErrBackendWithoutReadyEndpoints reports an empty positive-weight backend.
var ErrBackendWithoutReadyEndpoints = errors.New("positive-weight backend has no ready endpoints")

// WeightedServiceMembers pairs a Service backendRef weight with eligible members.
type WeightedServiceMembers struct {
	Weight  int32
	Members []cloud.Member
}

type weightedDestination struct {
	member cloud.Member
	share  big.Int
}

// WeightedMembers divides each backend's weight among its unique eligible
// destinations, then adds the shares of destinations reached through multiple backends.
// For UDP, a ratio outside the exact member weight range may be rounded within
// the allowed total variation distance.
func WeightedMembers(protocol string, groups []WeightedServiceMembers) ([]cloud.Member, error) {
	return weightedMembers(protocol, groups, cloud.MaxOctaviaMemberWeight)
}

// ExtendedUDPWeightedMembers allocates weights for a complete UDP discard batch.
// Callers try native weights first and negotiate regional support before writing.
func ExtendedUDPWeightedMembers(groups []WeightedServiceMembers) ([]cloud.Member, error) {
	return weightedMembers(cloud.ListenerProtocolUDP, groups, cloud.MaxUDPExtendedMemberWeight)
}

func weightedMembers(protocol string, groups []WeightedServiceMembers, maxWeight int64) ([]cloud.Member, error) {
	type readyGroup struct {
		weight    int64
		endpoints map[netip.AddrPort]struct{}
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

	// Dividing by the GCD yields the smallest integral ratio. Its largest
	// member fits exactly when maxShare <= gcd * maxWeight.
	var gcd, maxShare, exactLimit big.Int
	for _, destination := range destinations {
		gcd.GCD(nil, nil, &gcd, &destination.share)
		if destination.share.Cmp(&maxShare) > 0 {
			maxShare.Set(&destination.share)
		}
	}
	exactLimit.Mul(&gcd, big.NewInt(maxWeight))
	exact := maxShare.Cmp(&exactLimit) <= 0
	if !exact {
		if protocol != cloud.ListenerProtocolUDP {
			return nil, fmt.Errorf("the exact %s backend ratio exceeds Octavia's maximum member weight of %d: %w", protocol, maxWeight, ErrWeightedMembersUnrepresentable)
		}
		if !roundUDPWeights(destinations, &maxShare, maxWeight) {
			return nil, fmt.Errorf("scaling and rounding the UDP backend ratio within the member weight limit of %d did not meet the 0.390625 percentage point tolerance: %w", maxWeight, ErrWeightedMembersUnrepresentable)
		}
	}
	members := make([]cloud.Member, 0, len(destinations))
	active := 0
	for _, destination := range destinations {
		member := destination.member
		if exact {
			weight.Quo(&destination.share, &gcd)
			member.Weight = int(weight.Int64())
		}
		member.Draining = member.Weight == 0
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

type udpShareGroup struct {
	share        *big.Int
	count        int64
	idealTotal   big.Int
	roundedTotal big.Int
	weight       int64
}

func roundUDPWeights(destinations map[netip.AddrPort]*weightedDestination, maxShare *big.Int, maxWeight int64) bool {
	// Equal shares round to the same weight at every scale. Group them so
	// each candidate visits distinct shares instead of every destination.
	groups := make(map[string]*udpShareGroup)
	for _, destination := range destinations {
		if destination.share.Sign() == 0 {
			continue
		}
		key := string(destination.share.Bytes())
		group := groups[key]
		if group == nil {
			group = &udpShareGroup{share: &destination.share}
			groups[key] = group
		}
		group.count++
	}
	var idealTotal, count big.Int
	for _, group := range groups {
		count.SetInt64(group.count)
		group.idealTotal.Mul(group.share, &count)
		idealTotal.Add(&idealTotal, &group.idealTotal)
	}

	var scale, rounded, roundedTotal big.Int
	var difference, ideal, errorNumerator, errorDenominator big.Int
	halfMaxShare := new(big.Int).Rsh(maxShare, 1)
	toleranceDenominator := big.NewInt(udpWeightErrorDenominator)
	tryScale := func(maximum int64) bool {
		scale.SetInt64(maximum)
		roundedTotal.SetInt64(0)
		for _, group := range groups {
			// Round to the nearest integer, with ties upward and a minimum of 1.
			rounded.Mul(group.share, &scale)
			rounded.Add(&rounded, halfMaxShare)
			rounded.Quo(&rounded, maxShare)
			group.weight = max(1, rounded.Int64())
			rounded.SetInt64(group.weight)
			count.SetInt64(group.count)
			group.roundedTotal.Mul(&rounded, &count)
			roundedTotal.Add(&roundedTotal, &group.roundedTotal)
		}

		// Total variation is half the sum of absolute differences between the
		// normalized shares. Equal shares have equal differences, so using group
		// totals includes each destination's contribution with its multiplicity.
		errorNumerator.SetInt64(0)
		for _, group := range groups {
			difference.Mul(&group.roundedTotal, &idealTotal)
			ideal.Mul(&group.idealTotal, &roundedTotal)
			difference.Sub(&difference, &ideal)
			errorNumerator.Add(&errorNumerator, difference.Abs(&difference))
		}
		errorNumerator.Mul(&errorNumerator, toleranceDenominator)
		errorDenominator.Mul(&idealTotal, &roundedTotal)
		errorDenominator.Lsh(&errorDenominator, 1)
		if errorNumerator.Cmp(&errorDenominator) > 0 {
			return false
		}
		for _, destination := range destinations {
			destination.member.Weight = 0
			if group := groups[string(destination.share.Bytes())]; group != nil {
				destination.member.Weight = int(group.weight)
			}
		}
		return true
	}
	if maxWeight <= cloud.MaxOctaviaMemberWeight {
		for maximum := maxWeight; maximum > 0; maximum-- {
			if tryScale(maximum) {
				return true
			}
		}
		return false
	}
	for maximum := maxWeight; maximum > cloud.MaxOctaviaMemberWeight; maximum /= 2 {
		if tryScale(maximum) {
			return true
		}
		for _, candidate := range udpExtendedRoundingScales(groups, maxShare, maximum) {
			if tryScale(candidate) {
				return true
			}
		}
	}
	return false
}

func udpExtendedRoundingScales(groups map[string]*udpShareGroup, maxShare *big.Int, maximum int64) []int64 {
	// Low integer weights are susceptible to large relative rounding errors.
	// Bucket equal rounded weights at the top of this band. Try scales that
	// match the bucket's mean ideal share to nearby integer weights. Also try
	// either side of rounding boundaries for its smallest and largest shares.
	// Several buckets can agree only in a narrow interval between their weight
	// changes. Both loops are bounded by largestBucket, and every candidate
	// must pass the full pool check.
	const largestBucket = udpWeightErrorDenominator / 2
	type weightBucket struct {
		count    int64
		share    big.Int
		minShare *big.Int
		maxShare *big.Int
	}
	var buckets [largestBucket + 1]weightBucket
	for _, group := range groups {
		if group.weight <= largestBucket {
			bucket := &buckets[group.weight]
			bucket.count += group.count
			bucket.share.Add(&bucket.share, &group.idealTotal)
			if bucket.minShare == nil || group.share.Cmp(bucket.minShare) < 0 {
				bucket.minShare = group.share
			}
			if bucket.maxShare == nil || group.share.Cmp(bucket.maxShare) > 0 {
				bucket.maxShare = group.share
			}
		}
	}
	var candidates []int64
	minScale := max(cloud.MaxOctaviaMemberWeight, maximum/2)
	var scaledCount, level, candidate, halfShare, divisor big.Int
	one := big.NewInt(1)
	addCandidate := func(scale *big.Int) {
		if scale.IsInt64() && scale.Int64() > minScale && scale.Int64() < maximum {
			candidates = append(candidates, scale.Int64())
		}
	}
	for weight := int64(largestBucket); weight > 0; weight-- {
		bucket := &buckets[weight]
		if bucket.count == 0 {
			continue
		}
		scaledCount.SetInt64(bucket.count)
		scaledCount.Mul(maxShare, &scaledCount)
		halfShare.Rsh(&bucket.share, 1)
		for target := weight; target >= max(1, weight/2); target-- {
			level.SetInt64(target)
			candidate.Mul(&scaledCount, &level)
			candidate.Add(&candidate, &halfShare)
			candidate.Quo(&candidate, &bucket.share)
			addCandidate(&candidate)
		}
		for _, share := range []*big.Int{bucket.minShare, bucket.maxShare} {
			divisor.Lsh(share, 1)
			for target := weight; target >= max(2, weight/2); target-- {
				// The first scale that rounds this share up to target.
				level.SetInt64(2*target - 1)
				candidate.Mul(maxShare, &level)
				candidate.Add(&candidate, &divisor)
				candidate.Sub(&candidate, one)
				candidate.Quo(&candidate, &divisor)
				addCandidate(&candidate)
				candidate.Sub(&candidate, one)
				addCandidate(&candidate)
			}
			if bucket.minShare == bucket.maxShare {
				break
			}
		}
	}
	return candidates
}
