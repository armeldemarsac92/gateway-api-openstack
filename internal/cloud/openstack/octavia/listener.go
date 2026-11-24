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

package octavia

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
)

// ListListeners lists and extracts listeners matching opts.
func (s *Service) ListListeners(ctx context.Context, opts listeners.ListOpts) ([]listeners.Listener, error) {
	inventory, err := s.ListListenerInventory(ctx, opts)
	return inventory.Items, err
}

// ListenerInventory retains field presence needed to prove a listener has
// been disabled and detached. Both zero values otherwise accept missing JSON.
type ListenerInventory struct {
	DefaultHTTPResponseCodes map[string]int
	HTTPSVirtualHosts        map[string][]string
	TCPTLSTermination        map[string]bool
	TCPTLSMixedMode          map[string]bool
	SNICertificatesObserved  map[string]bool
	Items                    []listeners.Listener
	DefaultPoolsObserved     bool
	AdminStatesObserved      map[string]bool
}

func (s *Service) ListListenerInventory(ctx context.Context, opts listeners.ListOpts) (ListenerInventory, error) {
	pages, err := listeners.List(s.client, opts).AllPages(ctx)
	if err != nil {
		return ListenerInventory{}, fmt.Errorf("list listeners: %w", err)
	}
	var response struct {
		Listeners []struct {
			listeners.Listener
			TCPTLSTermination   json.RawMessage `json:"tcp_tls_termination"`
			TCPTLSMixedMode     json.RawMessage `json:"tcp_tls_mixed_mode"`
			SNICertificates     *[]string       `json:"sni_container_refs"`
			VirtualHosts        json.RawMessage `json:"https_virtual_hosts"`
			DefaultHTTPResponse *struct {
				StatusCode int `json:"status_code"`
			} `json:"default_http_response"`
			DefaultPool  json.RawMessage `json:"default_pool_id"`
			AdminStateUp *bool           `json:"admin_state_up"`
		} `json:"listeners"`
	}
	if err := pages.(listeners.ListenerPage).ExtractInto(&response); err != nil {
		return ListenerInventory{}, fmt.Errorf("extract listeners: %w", err)
	}
	inventory := ListenerInventory{DefaultPoolsObserved: true}
	if len(response.Listeners) > 0 {
		inventory.Items = make([]listeners.Listener, len(response.Listeners))
		inventory.AdminStatesObserved = make(map[string]bool, len(response.Listeners))
	}
	for i, observed := range response.Listeners {
		item := &inventory.Items[i]
		*item = observed.Listener
		if observed.SNICertificates != nil {
			item.SniContainerRefs = *observed.SNICertificates
			if inventory.SNICertificatesObserved == nil {
				inventory.SNICertificatesObserved = make(map[string]bool)
			}
			inventory.SNICertificatesObserved[item.ID] = true
		}
		if observed.TCPTLSTermination != nil {
			if inventory.TCPTLSTermination == nil {
				inventory.TCPTLSTermination = make(map[string]bool)
			}
			inventory.TCPTLSTermination[item.ID] = string(observed.TCPTLSTermination) == "true"
		}
		if observed.TCPTLSMixedMode != nil {
			if inventory.TCPTLSMixedMode == nil {
				inventory.TCPTLSMixedMode = make(map[string]bool)
			}
			// Only a literal true verifies mixed mode. Preserve listener
			// identity and admin state so a malformed row can still close.
			inventory.TCPTLSMixedMode[item.ID] = string(observed.TCPTLSMixedMode) == "true"
		}
		if observed.VirtualHosts != nil {
			if inventory.HTTPSVirtualHosts == nil {
				inventory.HTTPSVirtualHosts = make(map[string][]string)
			}
			var hosts []string
			if err := json.Unmarshal(observed.VirtualHosts, &hosts); err != nil || hosts == nil {
				// Keep its identity and admin state available for withdrawal.
				// A present, unreadable set cannot verify any allowed section.
				hosts = []string{}
			}
			inventory.HTTPSVirtualHosts[item.ID] = hosts
		}
		if observed.DefaultHTTPResponse != nil {
			if inventory.DefaultHTTPResponseCodes == nil {
				inventory.DefaultHTTPResponseCodes = make(map[string]int)
			}
			inventory.DefaultHTTPResponseCodes[item.ID] = observed.DefaultHTTPResponse.StatusCode
		}
		if observed.AdminStateUp != nil {
			item.AdminStateUp = *observed.AdminStateUp
			inventory.AdminStatesObserved[item.ID] = true
		}
		switch string(observed.DefaultPool) {
		case "null":
			// The public API explicitly reports a detached listener.
		case "", `""`:
			inventory.DefaultPoolsObserved = false
		default:
			if err := json.Unmarshal(observed.DefaultPool, &item.DefaultPoolID); err != nil {
				return ListenerInventory{}, fmt.Errorf("extract listener default pool: %w", err)
			}
		}
	}
	return inventory, nil
}

// CreateListener creates one listener.
func (s *Service) CreateListener(ctx context.Context, opts listeners.CreateOpts) (*listeners.Listener, error) {
	return listeners.Create(ctx, s.client, opts).Extract()
}

type tcpTLSListenerOptions struct {
	listeners.CreateOpts
	mixed bool
}

func (opts tcpTLSListenerOptions) ToListenerCreateMap() (map[string]any, error) {
	body, err := opts.CreateOpts.ToListenerCreateMap()
	if err == nil {
		fields := body["listener"].(map[string]any)
		fields["tcp_tls_termination"] = true
		if opts.mixed {
			fields["tcp_tls_mixed_mode"] = true
		}
	}
	return body, err
}

// CreateTCPTLSListener creates a disabled TCP listener with immutable termination.
func (s *Service) CreateTCPTLSListener(ctx context.Context, opts listeners.CreateOpts, mixed bool) (*listeners.Listener, error) {
	if opts.Protocol != listeners.ProtocolTCP || opts.DefaultTlsContainerRef == "" || opts.AdminStateUp == nil || *opts.AdminStateUp {
		return nil, fmt.Errorf("TCP TLS termination requires a disabled TCP listener and a default certificate")
	}
	return listeners.Create(ctx, s.client, tcpTLSListenerOptions{CreateOpts: opts, mixed: mixed}).Extract()
}

// UpdateListener updates one listener.
func (s *Service) UpdateListener(ctx context.Context, id string, opts listeners.UpdateOpts) (*listeners.Listener, error) {
	return listeners.Update(ctx, s.client, id, opts).Extract()
}

type listenerClientAuthenticationOptions struct {
	listeners.UpdateOpts
	ref string
}

func (opts listenerClientAuthenticationOptions) ToListenerUpdateMap() (map[string]any, error) {
	body, err := opts.UpdateOpts.ToListenerUpdateMap()
	if err != nil {
		return nil, err
	}
	fields := body["listener"].(map[string]any)
	if opts.ref != "" {
		fields["client_ca_tls_container_ref"] = opts.ref
		fields["client_authentication"] = listeners.ClientAuthenticationMandatory
	} else {
		fields["client_ca_tls_container_ref"] = nil
		fields["client_authentication"] = listeners.ClientAuthenticationNone
	}
	return body, nil
}

// SetListenerClientAuthentication updates the native mode and CA atomically.
// Pass an empty reference only for an explicit removal on a closed listener.
func (s *Service) SetListenerClientAuthentication(ctx context.Context, id string, opts listeners.UpdateOpts, ref string) error {
	_, err := listeners.Update(ctx, s.client, id, listenerClientAuthenticationOptions{UpdateOpts: opts, ref: ref}).Extract()
	return err
}

// DeleteListener deletes one listener.
func (s *Service) DeleteListener(ctx context.Context, id string) error {
	return listeners.Delete(ctx, s.client, id).ExtractErr()
}

type notFoundListenerOptions struct{ listeners.CreateOpts }

func (opts notFoundListenerOptions) ToListenerCreateMap() (map[string]any, error) {
	body, err := opts.CreateOpts.ToListenerCreateMap()
	if err != nil {
		return nil, err
	}
	body["listener"].(map[string]any)["default_http_response"] = map[string]int{"status_code": http.StatusNotFound}
	return body, nil
}

func (notFoundListenerOptions) ToListenerUpdateMap() (map[string]any, error) {
	return map[string]any{"listener": map[string]any{"default_http_response": map[string]int{"status_code": http.StatusNotFound}}}, nil
}

// CreateListenerWithNotFound creates an HTTP-family listener on the opt-in provider.
func (s *Service) CreateListenerWithNotFound(ctx context.Context, opts listeners.CreateOpts) (*listeners.Listener, error) {
	return listeners.Create(ctx, s.client, notFoundListenerOptions{opts}).Extract()
}

type isolatedHTTPSListenerOptions struct {
	listeners.CreateOptsBuilder
	virtualHosts []string
}

func (opts isolatedHTTPSListenerOptions) ToListenerCreateMap() (map[string]any, error) {
	body, err := opts.CreateOptsBuilder.ToListenerCreateMap()
	if err == nil {
		body["listener"].(map[string]any)["https_virtual_hosts"] = opts.virtualHosts
	}
	return body, err
}

// CreateIsolatedHTTPSListener creates the disabled TLS listener with its complete
// set of virtual hosts in the same request as its certificate and ownership tags.
func (s *Service) CreateIsolatedHTTPSListener(ctx context.Context, opts listeners.CreateOpts, hosts []string, notFound bool) (*listeners.Listener, error) {
	if len(hosts) == 0 || opts.Protocol != listeners.ProtocolTerminatedHTTPS || opts.AdminStateUp == nil || *opts.AdminStateUp {
		return nil, fmt.Errorf("isolated HTTPS listener requires virtual hosts and a disabled TERMINATED_HTTPS frontend")
	}
	var create listeners.CreateOptsBuilder = opts
	if notFound {
		create = notFoundListenerOptions{opts}
	}
	return listeners.Create(ctx, s.client, isolatedHTTPSListenerOptions{create, hosts}).Extract()
}

type httpsVirtualHosts []string

func (hosts httpsVirtualHosts) ToListenerUpdateMap() (map[string]any, error) {
	if len(hosts) == 0 {
		return nil, fmt.Errorf("isolated HTTPS listener requires at least one virtual host")
	}
	return map[string]any{"listener": map[string]any{"https_virtual_hosts": []string(hosts)}}, nil
}

// SetListenerHTTPSVirtualHosts changes the section inventory on a disabled
// listener after policies referring to removed names have been retired.
func (s *Service) SetListenerHTTPSVirtualHosts(ctx context.Context, id string, hosts []string) error {
	_, err := listeners.Update(ctx, s.client, id, httpsVirtualHosts(hosts)).Extract()
	return err
}

// SetListenerNotFound restores the unmatched response on an existing owned listener.
func (s *Service) SetListenerNotFound(ctx context.Context, id string) error {
	_, err := listeners.Update(ctx, s.client, id, notFoundListenerOptions{}).Extract()
	return err
}
