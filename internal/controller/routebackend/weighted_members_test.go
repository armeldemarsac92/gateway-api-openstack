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
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func TestWeightedMembers(t *testing.T) {
	a := cloud.Member{Address: "10.0.0.1", Port: 53}
	b := cloud.Member{Address: "10.0.0.2", Port: 53}
	c := cloud.Member{Address: "10.0.0.3", Port: 53}
	drainingC := cloud.Member{Address: c.Address, Port: c.Port, Draining: true}
	drainingD := cloud.Member{Address: "10.0.0.4", Port: 53, Draining: true}
	e := cloud.Member{Address: "10.0.0.5", Port: 53}

	for _, test := range []struct {
		name   string
		groups []WeightedServiceMembers
		want   []cloud.Member
	}{
		{
			name: "70-30-0",
			groups: []WeightedServiceMembers{
				{Weight: 30, Members: []cloud.Member{b}},
				{Weight: 70, Members: []cloud.Member{a}},
				{Weight: 0, Members: []cloud.Member{c, drainingD}},
			},
			want: []cloud.Member{
				{Address: a.Address, Port: a.Port, Weight: 7},
				{Address: b.Address, Port: b.Port, Weight: 3},
			},
		},
		{
			name: "unequal counts with duplicate, shared and draining members",
			groups: []WeightedServiceMembers{
				{Weight: 30, Members: []cloud.Member{c}},
				{Weight: 70, Members: []cloud.Member{b, drainingC, a, b, drainingD}},
				{Weight: 5, Members: []cloud.Member{a, b}},
			},
			want: []cloud.Member{
				{Address: a.Address, Port: a.Port, Weight: 5},
				{Address: b.Address, Port: b.Port, Weight: 5},
				{Address: c.Address, Port: c.Port, Weight: 4},
				drainingD,
			},
		},
		{
			name: "Octavia limit is inclusive",
			groups: []WeightedServiceMembers{
				{Weight: 256_000, Members: []cloud.Member{a}},
				{Weight: 1_000, Members: []cloud.Member{b}},
			},
			want: []cloud.Member{
				{Address: a.Address, Port: a.Port, Weight: 256},
				{Address: b.Address, Port: b.Port, Weight: 1},
			},
		},
		{
			name: "UDP rounding after merging shared destinations",
			groups: []WeightedServiceMembers{
				{Weight: 1028, Members: []cloud.Member{a, b, drainingC, drainingD}},
				{Weight: 510, Members: []cloud.Member{a}},
				{Weight: 1, Members: []cloud.Member{c}},
				{Weight: 0, Members: []cloud.Member{e}},
			},
			want: []cloud.Member{
				{Address: a.Address, Port: a.Port, Weight: 256},
				{Address: b.Address, Port: b.Port, Weight: 129},
				{Address: c.Address, Port: c.Port, Weight: 1},
				drainingD,
			},
		},
		{
			name: "UDP rounding uses the largest acceptable maximum",
			groups: []WeightedServiceMembers{
				{Weight: 1001, Members: []cloud.Member{a}},
				{Weight: 30, Members: []cloud.Member{b, c, e}},
			},
			want: []cloud.Member{
				{Address: a.Address, Port: a.Port, Weight: 232},
				{Address: b.Address, Port: b.Port, Weight: 2},
				{Address: c.Address, Port: c.Port, Weight: 2},
				{Address: e.Address, Port: e.Port, Weight: 2},
			},
		},
		{
			name:   "disabled backends have no members",
			groups: []WeightedServiceMembers{{Weight: 0, Members: []cloud.Member{a, drainingD}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			members, err := WeightedMembers(cloud.ListenerProtocolUDP, test.groups)
			if err != nil || !slices.Equal(members, test.want) {
				t.Fatalf("weighted UDP members = %+v, %v, want %+v", members, err, test.want)
			}
		})
	}
}

func TestWeightedMembersRejectsBackendWithoutReadyEndpoints(t *testing.T) {
	ready := cloud.Member{Address: "10.0.0.1", Port: 53}
	draining := cloud.Member{Address: "10.0.0.2", Port: 53, Draining: true}
	groups := []WeightedServiceMembers{
		{Weight: 257, Members: []cloud.Member{ready}},
		{Weight: 1, Members: []cloud.Member{draining}},
	}
	if members, err := WeightedMembers(cloud.ListenerProtocolUDP, groups); !errors.Is(err, ErrBackendWithoutReadyEndpoints) || len(members) != 0 {
		t.Fatalf("backend without ready endpoints returned %+v, %v", members, err)
	}
}

func TestWeightedTCPMembersRespectHAProxyActiveServerLimit(t *testing.T) {
	endpoints := make([]cloud.Member, 4095)
	for i := range endpoints {
		address := netip.AddrFrom4([4]byte{10, 1, byte(i >> 8), byte(i % 256)})
		endpoints[i] = cloud.Member{Address: address.String(), Port: 8080}
	}
	groups := []WeightedServiceMembers{
		{Weight: 4095, Members: endpoints},
		{Weight: 1, Members: []cloud.Member{{Address: "10.2.0.1", Port: 8080}}},
	}
	if members, err := WeightedMembers(cloud.ListenerProtocolTCP, groups); err == nil || len(members) != 0 {
		t.Fatalf("4096 active TCP endpoints produced %d members, error %v", len(members), err)
	}
	endpoints[0].Draining = true
	groups[0].Weight--
	if members, err := WeightedMembers(cloud.ListenerProtocolTCP, groups); err != nil || len(members) != 4096 || !members[0].Draining {
		t.Fatalf("4095 active TCP endpoints plus one draining produced %d members, error %v", len(members), err)
	}
}

func TestWeightedMembersWithLargeCommonDenominator(t *testing.T) {
	// These pairwise coprime endpoint counts have an LCM above uint64. Every
	// endpoint still has the same share because each weight scales with its count.
	counts := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53}
	groups := make([]WeightedServiceMembers, 0, len(counts))
	var want []cloud.Member
	for i, count := range counts {
		group := WeightedServiceMembers{Weight: int32(count * 10_000)}
		for j := 0; j < count; j++ {
			member := cloud.Member{Address: fmt.Sprintf("10.0.%d.%d", i+10, j+10), Port: 53}
			group.Members = append(group.Members, member)
			member.Weight = 1
			want = append(want, member)
		}
		groups = append(groups, group)
	}
	members, err := WeightedMembers(cloud.ListenerProtocolUDP, groups)
	if err != nil || !slices.Equal(members, want) {
		t.Fatalf("large denominator produced incorrect members (got %d, want %d), error %v", len(members), len(want), err)
	}
}
