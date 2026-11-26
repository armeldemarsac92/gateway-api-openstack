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

package httproute

import (
	"context"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/routebackend"
)

// ExternalHostnameCache preserves DNS results across the mutations of a
// Gateway graph. It holds no mutation authority and is empty on manager startup.
type ExternalHostnameCache struct {
	lookup    routebackend.IPLookup
	now       func() time.Time
	mu        sync.Mutex
	windows   map[types.UID]externalHostnameWindow
	nextPrune time.Time
}

type externalHostnameWindow struct {
	gateway externalHostnameGateway
	expires time.Time
	answers map[types.NamespacedName]externalHostnameAnswer
}

type externalHostnameGateway struct {
	generation      int64
	className       gatewayv1.ObjectName
	classUID        types.UID
	classGeneration int64
	topology        string
}

type externalHostnameAnswer struct {
	uid        types.UID
	generation int64
	spec       gatewayx.BackendSpec
	consumers  map[types.UID]externalHostnameConsumer
	addresses  []netip.Addr
}

type externalHostnameConsumer struct {
	generation   int64
	namespaceUID types.UID
	sections     []sectionIdentity
}

func (c *ExternalHostnameCache) time() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *ExternalHostnameCache) window(uid types.UID, gateway externalHostnameGateway) externalHostnameWindow {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.time()
	if !now.Before(c.nextPrune) {
		for uid, value := range c.windows {
			if !now.Before(value.expires) {
				delete(c.windows, uid)
			}
		}
		c.nextPrune = now.Add(routebackend.ExternalHostnameRefresh)
	}
	value := c.windows[uid]
	if !now.Before(value.expires) || value.gateway != gateway {
		return externalHostnameWindow{}
	}
	return value
}

func (c *ExternalHostnameCache) store(uid types.UID, window externalHostnameWindow) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.windows == nil {
		c.windows = make(map[types.UID]externalHostnameWindow)
	}
	c.windows[uid] = window
}

// Invalidate discards observations after loss of the Gateway's graph authority.
func (c *ExternalHostnameCache) Invalidate(identity cloud.Identity) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	uid := types.UID(identity.GatewayUID)
	if identity.RouteUID == "" {
		delete(c.windows, uid)
		return
	}
	// A detaching route invalidates only its own backend observations.
	window := c.windows[uid]
	window.answers = maps.Clone(window.answers)
	for key, answer := range window.answers {
		if _, used := answer.consumers[types.UID(identity.RouteUID)]; used {
			delete(window.answers, key)
		}
	}
	if len(window.answers) == 0 {
		delete(c.windows, uid)
	} else {
		c.windows[uid] = window
	}
}

// resolveExternalHostnameMembers populates only forwarding pools in this planned
// graph. Kubernetes dependency observations keep the XBackend spec, UID and
// generation and never depend on a second DNS lookup during the mutation fence.
func (r *Reconciler) resolveExternalHostnameMembers(ctx context.Context, inputs *routeModelInputs, ensures []routeValidationSnapshot) (time.Time, error) {
	cache := r.ExternalHostnames
	gateway := inputs.gateway
	if !inputs.input.XBackendProfile.ExternalHostname || len(inputs.input.XBackends) == 0 {
		cache.Invalidate(controller.GatewayIdentity(r.Config, gateway))
		return time.Time{}, nil
	}
	type target struct {
		pools     []*cloud.HTTPBackend
		consumers map[types.UID]externalHostnameConsumer
		query     *[]netip.Addr
	}
	targets := make(map[types.NamespacedName]*target)
	var authority *routeValidationSnapshot
	for i := range ensures {
		snapshot := &ensures[i]
		for part := range snapshot.desired.HTTPPortSpecs() {
			for i := range part.HTTPBackends {
				backend := &part.HTTPBackends[i]
				if backend.Kind != cloud.HTTPBackendKindXBackend || !backend.RoutedExternal {
					continue
				}
				// Keep a refused gRPC pool empty when the region cannot provide
				// its HTTP/2 transport. A pending h2c observation retains BackendH2C.
				if backend.BackendTLS != nil && backend.BackendTLS.Denied ||
					part.GRPC && backend.BackendTLS == nil && !backend.BackendH2C {
					continue
				}
				key := types.NamespacedName{Namespace: backend.Namespace, Name: backend.Name}
				if targets[key] == nil {
					targets[key] = &target{consumers: make(map[types.UID]externalHostnameConsumer)}
				}
				selected := inputs.admission.eligible[routeKey(snapshot.route)]
				targets[key].consumers[snapshot.route.UID] = externalHostnameConsumer{
					generation: snapshot.route.Generation, namespaceUID: selected.namespaceUID, sections: selected.sections,
				}
				targets[key].pools = append(targets[key].pools, backend)
				authority = snapshot
			}
		}
	}
	if len(targets) == 0 {
		cache.Invalidate(controller.GatewayIdentity(r.Config, gateway))
		return time.Time{}, nil
	}
	if cache == nil {
		return time.Time{}, fmt.Errorf("XBackend ExternalHostname requires a shared DNS observation cache")
	}
	identity := externalHostnameGateway{
		generation: gateway.Generation, className: gateway.Spec.GatewayClassName,
		classUID: authority.gatewayClass.UID, classGeneration: authority.gatewayClass.Generation,
		topology: gateway.Annotations[r.Config.GatewayTopologyAnnotation()],
	}
	previous := cache.window(gateway.UID, identity)
	window := externalHostnameWindow{gateway: identity, expires: previous.expires, answers: make(map[types.NamespacedName]externalHostnameAnswer)}
	queries := make(map[string]*[]netip.Addr)
	for _, observation := range inputs.input.XBackends {
		target := targets[observation.Key]
		if target == nil {
			continue
		}
		answer := externalHostnameAnswer{
			uid: observation.UID, generation: observation.Generation, spec: observation.Spec, consumers: target.consumers,
		}
		if old, ok := previous.answers[observation.Key]; ok && sameExternalHostnameQuestion(old, answer) {
			answer.addresses = old.addresses
		} else {
			hostname := string(answer.spec.ExternalHostname.Hostname) + "."
			if queries[hostname] == nil {
				queries[hostname] = new([]netip.Addr)
			}
			target.query = queries[hostname]
		}
		window.answers[observation.Key] = answer
	}
	if err := routebackend.ResolveExternalHostnames(ctx, cache.lookup, queries, r.Config.XBackendExternalHostnameCIDRs); err != nil {
		return time.Time{}, err
	}
	for key, answer := range window.answers {
		target := targets[key]
		if result := target.query; result != nil {
			answer.addresses = *result
			window.answers[key] = answer
		}
		for _, pool := range target.pools {
			pool.Members = nil
			for _, address := range answer.addresses {
				pool.Members = append(pool.Members, cloud.Member{Address: address.String(), Port: pool.Port})
			}
			pool.Unavailable = len(pool.Members) == 0
		}
	}
	if window.expires.IsZero() {
		window.expires = cache.time().Add(controller.OpenStackResyncAfter(routebackend.ExternalHostnameRefresh, gateway.UID))
	}
	cache.store(gateway.UID, window)
	return window.expires, nil
}

func sameExternalHostnameQuestion(a, b externalHostnameAnswer) bool {
	return a.uid == b.uid && a.generation == b.generation && equality.Semantic.DeepEqual(a.spec, b.spec) &&
		maps.EqualFunc(a.consumers, b.consumers, func(a, b externalHostnameConsumer) bool {
			return a.generation == b.generation && a.namespaceUID == b.namespaceUID && slices.Equal(a.sections, b.sections)
		})
}
