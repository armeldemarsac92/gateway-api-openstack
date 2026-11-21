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
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

func TestWeightedMembers(t *testing.T) {
	a := cloud.Member{Address: "10.0.0.1", Port: 53}
	b := cloud.Member{Address: "10.0.0.2", Port: 53}
	c := cloud.Member{Address: "10.0.0.3", Port: 53}
	drainingC := cloud.Member{Address: c.Address, Port: c.Port, Draining: true}
	drainingD := cloud.Member{Address: "10.0.0.4", Port: 53, Draining: true}

	for _, test := range []struct {
		name   string
		groups []weightedServiceMembers
		want   []cloud.Member
	}{
		{
			name: "70-30-0",
			groups: []weightedServiceMembers{
				{weight: 30, members: []cloud.Member{b}},
				{weight: 70, members: []cloud.Member{a}},
				{weight: 0, members: []cloud.Member{c, drainingD}},
			},
			want: []cloud.Member{
				{Address: a.Address, Port: a.Port, Weight: 7},
				{Address: b.Address, Port: b.Port, Weight: 3},
			},
		},
		{
			name: "unequal counts with duplicate, shared and draining members",
			groups: []weightedServiceMembers{
				{weight: 30, members: []cloud.Member{c}},
				{weight: 70, members: []cloud.Member{b, drainingC, a, b, drainingD}},
				{weight: 5, members: []cloud.Member{a, b}},
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
			groups: []weightedServiceMembers{
				{weight: 256_000, members: []cloud.Member{a}},
				{weight: 1_000, members: []cloud.Member{b}},
			},
			want: []cloud.Member{
				{Address: a.Address, Port: a.Port, Weight: 256},
				{Address: b.Address, Port: b.Port, Weight: 1},
			},
		},
		{
			name:   "disabled backends have no members",
			groups: []weightedServiceMembers{{weight: 0, members: []cloud.Member{a, drainingD}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			members, err := weightedMembers(cloud.ListenerProtocolUDP, test.groups)
			if err != nil || !slices.Equal(members, test.want) {
				t.Fatalf("weighted UDP members = %+v, %v, want %+v", members, err, test.want)
			}
		})
	}
}

func TestWeightedMembersFailClosed(t *testing.T) {
	ready := cloud.Member{Address: "10.0.0.1", Port: 53}
	draining := cloud.Member{Address: "10.0.0.2", Port: 53, Draining: true}
	groups := []weightedServiceMembers{
		{weight: 257, members: []cloud.Member{ready}},
		{weight: 1, members: []cloud.Member{draining}},
	}
	if members, err := weightedMembers(cloud.ListenerProtocolUDP, groups); !errors.Is(err, errBackendWithoutReadyEndpoints) || len(members) != 0 {
		t.Fatalf("backend without ready endpoints returned %+v, %v", members, err)
	}
	groups[1].members[0].Draining = false
	if members, err := weightedMembers(cloud.ListenerProtocolUDP, groups); err == nil || !strings.Contains(err.Error(), "256") || len(members) != 0 {
		t.Fatalf("unrepresentable ratio returned %+v, %v", members, err)
	}
}

func TestWeightedTCPMembersRespectHAProxyActiveServerLimit(t *testing.T) {
	endpoints := make([]cloud.Member, 4095)
	for i := range endpoints {
		address := netip.AddrFrom4([4]byte{10, 1, byte(i >> 8), byte(i % 256)})
		endpoints[i] = cloud.Member{Address: address.String(), Port: 8080}
	}
	groups := []weightedServiceMembers{
		{weight: 4095, members: endpoints},
		{weight: 1, members: []cloud.Member{{Address: "10.2.0.1", Port: 8080}}},
	}
	if members, err := weightedMembers(cloud.ListenerProtocolTCP, groups); err == nil || len(members) != 0 {
		t.Fatalf("4096 active TCP endpoints produced %d members, error %v", len(members), err)
	}
	endpoints[0].Draining = true
	groups[0].weight--
	if members, err := weightedMembers(cloud.ListenerProtocolTCP, groups); err != nil || len(members) != 4096 || !members[0].Draining {
		t.Fatalf("4095 active TCP endpoints plus one draining produced %d members, error %v", len(members), err)
	}
}

func TestWeightedMembersWithLargeCommonDenominator(t *testing.T) {
	// These pairwise coprime endpoint counts have an LCM above uint64. Every
	// endpoint still has the same share because each weight scales with its count.
	counts := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53}
	groups := make([]weightedServiceMembers, 0, len(counts))
	var want []cloud.Member
	for i, count := range counts {
		group := weightedServiceMembers{weight: int32(count * 10_000)}
		for j := 0; j < count; j++ {
			member := cloud.Member{Address: fmt.Sprintf("10.0.%d.%d", i+10, j+10), Port: 53}
			group.members = append(group.members, member)
			member.Weight = 1
			want = append(want, member)
		}
		groups = append(groups, group)
	}
	members, err := weightedMembers(cloud.ListenerProtocolUDP, groups)
	if err != nil || !slices.Equal(members, want) {
		t.Fatalf("large denominator produced incorrect members (got %d, want %d), error %v", len(members), len(want), err)
	}
}
