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
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	controllergraph "github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestTCPGraphRefusesIncompleteBackendInventory(t *testing.T) {
	for _, test := range []struct {
		name           string
		path           string
		response       map[string]any
		emptyFirstPage bool
	}{
		{name: "omitted members", path: tcpMembers, response: map[string]any{}},
		{name: "null members", path: tcpMembers, response: map[string]any{"members": nil}},
		{name: "omitted second member page", path: tcpMembers, response: map[string]any{}, emptyFirstPage: true},
		{name: "omitted pools", path: tcpPools, response: map[string]any{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newTCPGraphFixture(t)
			f.settleTransportGraph(f.provider())
			p := newTestProvider(t, hibiscusServiceClients(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && r.URL.Path == test.path {
					if test.emptyFirstPage && r.URL.Query().Get("marker") == "" {
						safetyWriteJSON(t, w, map[string]any{
							"members":       []any{},
							"members_links": []map[string]string{{"rel": "next", "href": "http://" + r.Host + test.path + "?marker=next"}},
						})
						return
					}
					safetyWriteJSON(t, w, test.response)
					return
				}
				f.serveHTTP(w, r)
			})), ProviderConfig{})
			delete(f.inventory[tcpPool], "members")
			f.inventory[tcpMember]["tags"] = []string{"independent"}
			f.spec.Routes[0].Disposition = cloud.GraphDelete
			before := len(f.writes)
			_, err := p.ReconcileGatewayGraph(t.Context(), f.spec)
			if !errors.Is(err, cloud.ErrDependencyNotReady) || len(f.writes) != before {
				t.Fatalf("incomplete backend inventory returned %v, writes %v", err, f.writes[before:])
			}
		})
	}
}

type lateMutationSource struct {
	f       *tcpGraphFixture
	changed bool
	checks  int
}

var errChangedListener = errors.New("listener election changed")

func (s *lateMutationSource) ObserveGatewayGraph(context.Context, controllergraph.Request) (controllergraph.GatewayInput, error) {
	return controllergraph.GatewayInput{Node: s.f.spec.Gateway}, nil
}

func (s *lateMutationSource) ObserveRouteGraph(context.Context, controllergraph.Request, *gatewayv1.Gateway) (controllergraph.RouteInputs, error) {
	return controllergraph.RouteInputs{Nodes: s.f.spec.Routes, Validate: func(context.Context) error {
		s.checks++
		if s.changed {
			return errChangedListener
		}
		return nil
	}}, nil
}

func TestTCPGraphRevalidatesAfterReadingCloud(t *testing.T) {
	for _, mutation := range []string{"attachment", "withdrawal"} {
		t.Run(mutation, func(t *testing.T) {
			f := newTCPGraphFixture(t)
			provider := f.provider()
			f.settleTransportGraph(provider)
			f.spec.Routes[0].Disposition = cloud.GraphPreserve
			if mutation == "attachment" {
				f.settleTransportGraph(provider)
				f.spec.Routes[0].Disposition = cloud.GraphEnsure
			}
			source := &lateMutationSource{f: f}
			p := newTestProvider(t, hibiscusServiceClients(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && r.URL.Path == tcpPools {
					source.changed = true
				}
				f.serveHTTP(w, r)
			})), ProviderConfig{})
			writer := controllergraph.Writer{Gateway: source, Routes: source, Coordinator: &controllergraph.Coordinator{}, Provider: p}
			before := len(f.writes)
			_, err := writer.ReconcileGraph(t.Context(), controllergraph.Request{Identity: f.spec.Identity, Action: controllergraph.Ensure})
			if !errors.Is(err, errChangedListener) || source.checks != 2 || len(f.writes) != before {
				t.Fatalf("reconcile with a changed input returned %v, validation checks %d, writes %v", err, source.checks, f.writes[before:])
			}
		})
	}
}
