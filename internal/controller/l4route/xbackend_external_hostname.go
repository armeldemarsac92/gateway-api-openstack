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
	"context"
	"net/netip"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

// Keep one answer window for each route and Gateway. A route plan and the
// writer must use the same answers to agree on the forwarding checkpoint.
type tcpExternalHostnameCache struct {
	lookup    routebackend.IPLookup
	now       func() time.Time
	refreshes graph.Coordinator
	mu        sync.Mutex
	windows   map[tcpExternalHostnameKey]*tcpExternalHostnameWindow
	nextPrune time.Time
}

type tcpExternalHostnameKey struct {
	gatewayUID types.UID
	routeUID   types.UID
}

type tcpExternalHostnameQuestion struct {
	backendUID types.UID
	generation int64
	hostname   string
}

type tcpExternalHostnameWindow struct {
	gatewayGeneration int64
	routeGeneration   int64
	questions         []tcpExternalHostnameQuestion
	cidrs             []netip.Prefix
	answers           map[string]*[]netip.Addr
	expires           time.Time
}

type tcpExternalHostnameObservations struct {
	windows   map[tcpExternalHostnameKey]*tcpExternalHostnameWindow
	sealed    bool
	refreshAt time.Time
}

func (backend transportBackend) externalHostname() string {
	if backend.xBackend == nil || backend.xBackend.Spec.Type != gatewayx.BackendTypeExternalHostname {
		return ""
	}
	return string(backend.xBackend.Spec.ExternalHostname.Hostname) + "."
}

func (c *tcpExternalHostnameCache) time() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// InvalidateExternalHostnames discards answers after loss of route or Gateway authority.
func (r *Reconciler) InvalidateExternalHostnames(identity cloud.Identity) {
	cache := &r.externalHostnames
	cache.mu.Lock()
	defer cache.mu.Unlock()
	gatewayUID, routeUID := types.UID(identity.GatewayUID), types.UID(identity.RouteUID)
	if routeUID != "" {
		delete(cache.windows, tcpExternalHostnameKey{gatewayUID, routeUID})
		return
	}
	for key := range cache.windows {
		if key.gatewayUID == gatewayUID {
			delete(cache.windows, key)
		}
	}
}

// reserveWindow returns an existing answer or reserves its slot for a refresh.
// The slot identity lets invalidation discard any lookup still in flight.
func (c *tcpExternalHostnameCache) reserveWindow(key tcpExternalHostnameKey, gateway *gatewayv1.Gateway, route *transportRoute, questions []tcpExternalHostnameQuestion, cidrs []netip.Prefix) (*tcpExternalHostnameWindow, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.time()
	if !now.Before(c.nextPrune) {
		for key, window := range c.windows {
			if !now.Before(window.expires) {
				delete(c.windows, key)
			}
		}
		c.nextPrune = now.Add(routebackend.ExternalHostnameRefresh)
	}
	window := c.windows[key]
	if window.matches(gateway, route, questions, cidrs) && now.Before(window.expires) {
		return window, true
	}
	if c.windows == nil {
		c.windows = make(map[tcpExternalHostnameKey]*tcpExternalHostnameWindow)
	}
	reservation := &tcpExternalHostnameWindow{expires: now.Add(routebackend.ExternalHostnameRefresh)}
	c.windows[key] = reservation
	return reservation, false
}

func (c *tcpExternalHostnameCache) store(key tcpExternalHostnameKey, reservation, window *tcpExternalHostnameWindow) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.windows[key] != reservation {
		return controller.ErrGraphInputChanged
	}
	c.windows[key] = window
	return nil
}

func (w *tcpExternalHostnameWindow) matches(gateway *gatewayv1.Gateway, route *transportRoute, questions []tcpExternalHostnameQuestion, cidrs []netip.Prefix) bool {
	return w != nil && w.gatewayGeneration == gateway.Generation && w.routeGeneration == route.GetGeneration() &&
		slices.Equal(w.questions, questions) && slices.Equal(w.cidrs, cidrs)
}

func (r *Reconciler) observeTCPExternalHostnames(ctx context.Context, gateway *gatewayv1.Gateway, route *transportRoute, backends []transportBackend, observations *tcpExternalHostnameObservations) (map[string]*[]netip.Addr, error) {
	var questions []tcpExternalHostnameQuestion
	for _, backend := range backends {
		if hostname := backend.externalHostname(); hostname != "" {
			questions = append(questions, tcpExternalHostnameQuestion{backend.xBackend.UID, backend.xBackend.Generation, hostname})
		}
	}
	if len(questions) == 0 {
		return nil, nil
	}
	if observations == nil {
		return nil, controller.ErrGraphInputChanged
	}
	key := tcpExternalHostnameKey{gateway.UID, route.GetUID()}
	cidrs := r.Config.XBackendExternalHostnameCIDRs
	if observations.sealed {
		// Freshly check the Kubernetes authorization and re-use only answers
		// sealed for exactly these questions. The fence must never query DNS.
		window := observations.windows[key]
		if !window.matches(gateway, route, questions, cidrs) {
			return nil, controller.ErrGraphInputChanged
		}
		return window.answers, nil
	}
	cache := &r.externalHostnames
	// The route reconciler can plan while the Gateway writer is observing
	// the same route. Refresh only once per key and recheck after waiting.
	release, err := graph.Acquire(ctx, &cache.refreshes, string(key.gatewayUID)+"/"+string(key.routeUID))
	if err != nil {
		return nil, err
	}
	defer release()
	window, cached := cache.reserveWindow(key, gateway, route, questions, cidrs)
	if !cached {
		reservation := window
		queries := make(map[string]*[]netip.Addr, len(questions))
		for _, question := range questions {
			if queries[question.hostname] == nil {
				queries[question.hostname] = new([]netip.Addr)
			}
		}
		if err := routebackend.ResolveExternalHostnames(ctx, cache.lookup, queries, cidrs); err != nil {
			return nil, err
		}
		window = &tcpExternalHostnameWindow{
			gatewayGeneration: gateway.Generation, routeGeneration: route.GetGeneration(),
			questions: questions, cidrs: slices.Clone(cidrs), answers: queries,
			expires: cache.time().Add(controller.OpenStackResyncAfter(routebackend.ExternalHostnameRefresh, route.GetUID())),
		}
		if err := cache.store(key, reservation, window); err != nil {
			return nil, err
		}
	}
	if observations.windows == nil {
		observations.windows = make(map[tcpExternalHostnameKey]*tcpExternalHostnameWindow)
	}
	observations.windows[key] = window
	if observations.refreshAt.IsZero() || window.expires.Before(observations.refreshAt) {
		observations.refreshAt = window.expires
	}
	return window.answers, nil
}

func routedTCPMembers(answers *[]netip.Addr, port int) []cloud.Member {
	var members []cloud.Member
	for _, address := range *answers {
		members = append(members, cloud.Member{Address: address.String(), Port: port, RoutedExternal: true})
	}
	return members
}
