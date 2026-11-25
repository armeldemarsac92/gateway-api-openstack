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
	"net"
	"net/netip"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayx "sigs.k8s.io/gateway-api/apisx/v1alpha1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

const (
	externalHostnameRefresh       = 30 * time.Second
	externalHostnameLookupTimeout = 2 * time.Second
	externalHostnameLookupWorkers = 8
)

type ipLookup func(context.Context, string, string) ([]netip.Addr, error)

func hasRoutedHTTPBackend(spec cloud.RouteSpec) bool {
	for port := range spec.HTTPPortSpecs() {
		if slices.ContainsFunc(port.HTTPBackends, func(backend cloud.HTTPBackend) bool { return backend.RoutedExternal }) {
			return true
		}
	}
	return false
}

// resolveExternalHostnameMembers populates only forwarding pools in this planned
// graph. Kubernetes dependency observations keep the XBackend spec, UID and
// generation and never depend on a second DNS lookup during the mutation fence.
func (r *Reconciler) resolveExternalHostnameMembers(ctx context.Context, nodes []cloud.RouteGraphNode, observations []model.XBackendObservation) error {
	hostnames := make(map[types.NamespacedName]string)
	for _, observation := range observations {
		if observation.Spec.Type == gatewayx.BackendTypeExternalHostname && observation.Spec.ExternalHostname != nil {
			hostnames[observation.Key] = string(observation.Spec.ExternalHostname.Hostname) + "."
		}
	}
	if len(hostnames) == 0 {
		return nil
	}
	targets := make(map[string][]*cloud.HTTPBackend)
	for i := range nodes {
		if nodes[i].Disposition != cloud.GraphEnsure {
			continue
		}
		for part := range nodes[i].Spec.HTTPPortSpecs() {
			for i := range part.HTTPBackends {
				backend := &part.HTTPBackends[i]
				if backend.RoutedExternal {
					key := types.NamespacedName{Namespace: backend.Namespace, Name: backend.Name}
					hostname := hostnames[key]
					targets[hostname] = append(targets[hostname], backend)
				}
			}
		}
	}
	if len(targets) == 0 {
		return nil
	}
	lookup := r.externalHostnameLookup
	if lookup == nil {
		lookup = net.DefaultResolver.LookupNetIP
	}
	var workers sync.WaitGroup
	slots := make(chan struct{}, externalHostnameLookupWorkers)
	for hostname, backends := range targets {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			workers.Wait()
			return ctx.Err()
		}
		workers.Go(func() {
			defer func() { <-slots }()
			addresses := r.lookupExternalHostname(ctx, lookup, hostname)
			for _, backend := range backends {
				backend.Members = nil
				for _, address := range addresses {
					backend.Members = append(backend.Members, cloud.Member{Address: address.String(), Port: backend.Port})
				}
				backend.Unavailable = len(backend.Members) == 0
			}
		})
	}
	workers.Wait()
	return ctx.Err()
}

func (r *Reconciler) lookupExternalHostname(ctx context.Context, lookup ipLookup, hostname string) []netip.Addr {
	query, cancel := context.WithTimeout(ctx, externalHostnameLookupTimeout)
	defer cancel()
	addresses, err := lookup(query, "ip4", hostname)
	if err != nil {
		log.FromContext(ctx).V(2).Info("ExternalHostname lookup returned no usable addresses", "hostname", hostname, "error", err)
		return nil
	}
	usable := make([]netip.Addr, 0, len(addresses))
	for _, address := range addresses {
		// The Go resolver may represent A answers as IPv4-mapped IPv6.
		address = address.Unmap()
		if address.Is4() && address.IsGlobalUnicast() && !address.IsLoopback() &&
			slices.ContainsFunc(r.Config.XBackendExternalHostnameCIDRs, func(prefix netip.Prefix) bool { return prefix.Contains(address) }) {
			usable = append(usable, address)
		}
	}
	slices.SortFunc(usable, netip.Addr.Compare)
	return slices.Compact(usable)
}
