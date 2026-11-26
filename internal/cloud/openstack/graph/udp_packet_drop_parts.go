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
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

const udpDropPartPrefix = "udp/"

func udpDropPartKey(port int) string {
	return udpDropPartPrefix + strconv.Itoa(port)
}

func udpDropPartPort(key string) int {
	value, ok := strings.CutPrefix(key, udpDropPartPrefix)
	if !ok {
		return 0
	}
	port, err := strconv.Atoi(value)
	if err != nil || port < 1 || port > 65535 || strconv.Itoa(port) != value {
		return 0
	}
	return port
}

func udpDropResourceRole(base, key string) string {
	return "udp-drop-v1/" + base + "/" + strings.TrimPrefix(key, udpDropPartPrefix)
}

func parseUDPDropResourceRole(raw string) (routeResourceRole, bool) {
	tail, ok := strings.CutPrefix(raw, "udp-drop-v1/")
	base, value, found := strings.Cut(tail, "/")
	key := udpDropPartPrefix + value
	if !ok || !found || udpDropPartPort(key) == 0 || base != rolePool && base != roleMember && base != roleMonitor {
		return routeResourceRole{}, false
	}
	return routeResourceRole{raw: raw, base: base, backend: key}, true
}

type udpDropPlanningPart struct {
	owner     int
	migration udpPoolMigration
}

type udpPoolMigration int

const (
	udpNoMigration udpPoolMigration = iota
	udpReplaceStockPool
	udpDetachSharedStockPool
)

// Only the physical plan is expanded. The Gateway and route UIDs stay the
// same, and observation and reconciliation publish one entry per route.
func udpDropPlanningParts(desired desiredGatewayGraph, actual observedGatewayGraph) (desiredGatewayGraph, observedGatewayGraph, []udpDropPlanningPart) {
	if !actual.scoped || actual.protocol != cloud.ListenerProtocolUDP || !hasUDPDropTarget(desired) && !hasUDPDropReadback(actual) {
		return desired, actual, nil
	}
	partDesired, partActual := desired, actual
	partDesired.routes, partDesired.spec.Routes, partActual.routes = nil, nil, nil
	var parts []udpDropPlanningPart
	for index, route := range desired.routes {
		node := desired.spec.Routes[index]
		var observed observedRouteGraph
		if index < len(actual.routes) {
			observed = actual.routes[index]
		}
		remaining := maps.Clone(observed.udpParts)
		observed.udpParts = nil
		add := func(part desiredRoute, readback observedRouteGraph, disposition cloud.GraphDisposition, migration udpPoolMigration) {
			partNode := node
			partNode.Spec, partNode.Disposition = part.spec, disposition
			if node.RetainListenerPorts != nil {
				partNode.RetainListenerPorts = slices.DeleteFunc(slices.Clone(node.RetainListenerPorts), func(port int) bool {
					return !slices.Contains(part.spec.ScopedListenerPorts, port)
				})
			}
			partNode.RestrictUDPPacketDrop = node.RestrictUDPPacketDrop && disposition == cloud.GraphPreserve && len(partNode.RetainListenerPorts) != 0
			partDesired.routes = append(partDesired.routes, part)
			partDesired.spec.Routes = append(partDesired.spec.Routes, partNode)
			partActual.routes = append(partActual.routes, readback)
			parts = append(parts, udpDropPlanningPart{owner: index, migration: migration})
		}
		ports := route.spec.ScopedListenerPorts
		if node.Disposition == cloud.GraphPreserve {
			ports = node.RetainListenerPorts
		}
		wantDrop := route.spec.UDPPacketDrop != nil && len(ports) != 0 && node.Disposition != cloud.GraphDelete
		if !wantDrop {
			add(route, observed, node.Disposition, udpNoMigration)
		} else {
			var legacyPort int
			detachLegacy := len(ports) > 1
			if observed.pool != nil {
				attached := servingPoolListeners(actual, observed.pool.ID)
				detachLegacy = detachLegacy || len(attached) > 1
				if observed.udpDropPool() && len(attached) == 1 {
					port := attached[0].ProtocolPort
					if slices.Contains(ports, port) && remaining[udpDropPartKey(port)] == nil {
						legacyPort = port
					}
				}
			}
			if observed.pool != nil && legacyPort == 0 {
				disposition := cloud.GraphDelete
				migration := udpNoMigration
				if !observed.udpDropPool() {
					migration = udpReplaceStockPool
					if detachLegacy {
						migration = udpDetachSharedStockPool
					}
				}
				if migration != udpNoMigration && (route.udpDropUnverified || node.RestrictUDPPacketDrop) {
					// Keep a proven, still-authorized stock default while
					// discovery or the route's checkpoint is pending.
					disposition = node.Disposition
				}
				add(route, observed, disposition, migration)
			}
			for _, port := range ports {
				part := route
				part.spec.ScopedListenerPorts = []int{port}
				if port == legacyPort {
					add(part, observed, node.Disposition, udpNoMigration)
					continue
				}
				key := udpDropPartKey(port)
				part.backendKey = key
				var readback observedRouteGraph
				if found := remaining[key]; found != nil {
					readback = *found
					delete(remaining, key)
				}
				add(part, readback, node.Disposition, udpNoMigration)
			}
		}
		for _, key := range slices.Sorted(maps.Keys(remaining)) {
			part := route
			part.backendKey = key
			part.spec.ScopedListenerPorts = nil
			disposition := cloud.GraphDelete
			if desired.spec.Gateway.Disposition == cloud.GraphPreserve && node.Disposition == cloud.GraphPreserve {
				disposition = cloud.GraphPreserve
			}
			add(part, *remaining[key], disposition, udpNoMigration)
		}
	}
	return partDesired, partActual, parts
}

func combineUDPDropResults(result *cloud.GatewayGraphResult, parts []udpDropPlanningPart, routeCount int) {
	if len(parts) == 0 || len(result.Routes) == 0 {
		return
	}
	if len(result.Routes) != len(parts) {
		result.Routes = nil
		return
	}
	combined := make([]cloud.RouteGraphResult, routeCount)
	seen := make([]bool, routeCount)
	for index, part := range result.Routes {
		owner := parts[index].owner
		route := &combined[owner]
		if !seen[owner] {
			*route = part
			seen[owner] = true
			continue
		}
		merged, next := &route.Result, part.Result
		mutated := merged.Outcome.MutationSubmitted || next.Outcome.MutationSubmitted
		if next.Outcome.State != cloud.OutcomeReady {
			merged.Outcome = next.Outcome
		}
		merged.Outcome.MutationSubmitted = mutated
		merged.ObservationDeferred = merged.ObservationDeferred && next.ObservationDeferred
		if id := next.State.PoolID; id != "" {
			if merged.State.PoolID != "" {
				merged.State.PoolIDs = append(merged.State.PoolIDs, merged.State.PoolID)
				merged.State.PoolID = ""
			}
			if len(merged.State.PoolIDs) == 0 {
				merged.State.PoolID = id
			} else {
				merged.State.PoolIDs = append(merged.State.PoolIDs, id)
			}
		}
		merged.State.MemberIDs = append(merged.State.MemberIDs, next.State.MemberIDs...)
	}
	result.Routes = combined
}
