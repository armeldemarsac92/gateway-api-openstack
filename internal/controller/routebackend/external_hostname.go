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
	"context"
	"net"
	"net/netip"
	"slices"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	ExternalHostnameRefresh       = 30 * time.Second
	externalHostnameLookupTimeout = 2 * time.Second
	externalHostnameLookupWorkers = 8
)

type IPLookup func(context.Context, string, string) ([]netip.Addr, error)

// ResolveExternalHostnames queries absolute names and keeps only destinations
// the operator has authorized for routed IPv4 backends.
func ResolveExternalHostnames(ctx context.Context, lookup IPLookup, queries map[string]*[]netip.Addr, cidrs []netip.Prefix) error {
	if lookup == nil {
		lookup = net.DefaultResolver.LookupNetIP
	}
	var workers sync.WaitGroup
	slots := make(chan struct{}, externalHostnameLookupWorkers)
	for hostname, answer := range queries {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			workers.Wait()
			return ctx.Err()
		}
		workers.Go(func() {
			defer func() { <-slots }()
			*answer = lookupExternalHostname(ctx, lookup, hostname, cidrs)
		})
	}
	workers.Wait()
	return ctx.Err()
}

func lookupExternalHostname(ctx context.Context, lookup IPLookup, hostname string, cidrs []netip.Prefix) []netip.Addr {
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
			slices.ContainsFunc(cidrs, func(prefix netip.Prefix) bool { return prefix.Contains(address) }) {
			usable = append(usable, address)
		}
	}
	slices.SortFunc(usable, netip.Addr.Compare)
	return slices.Compact(usable)
}
