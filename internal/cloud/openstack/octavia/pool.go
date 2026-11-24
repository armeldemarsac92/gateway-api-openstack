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

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/gophercloud/gophercloud/v2/pagination"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

// ListPools lists and extracts pools matching opts for read-only inventory.
func (s *Service) ListPools(ctx context.Context, opts pools.ListOpts) ([]pools.Pool, error) {
	pages, err := pools.List(s.client, opts).AllPages(ctx)
	if err != nil {
		return nil, fmt.Errorf("list pools: %w", err)
	}
	items, err := pools.ExtractPools(pages)
	if err != nil {
		return nil, fmt.Errorf("extract pools: %w", err)
	}
	return items, nil
}

// PoolInventory carries the provider extension separately from SDK pool types.
type PoolInventory struct {
	Items      []pools.Pool
	BackendH2C map[string]bool
	BackendTLS map[string]PoolBackendTLS
}

// PoolBackendTLS records which transport fields the API actually reported.
// Explicit null is a reported absence of a hostname or certificate reference.
type PoolBackendTLS struct {
	Hostname          string
	HostnameObserved  bool
	CAObserved        bool
	ClientRefObserved bool
	RequireH2         bool
	RequireH2Observed bool
	EnabledObserved   bool
	H2CObserved       bool
	AdminObserved     bool
}

func (tls PoolBackendTLS) Complete() bool {
	return tls.HostnameObserved && tls.CAObserved && tls.ClientRefObserved && tls.EnabledObserved && tls.AdminObserved
}

// ListPoolInventory requires an explicit pool collection on every page and
// reads backend transport alongside the stock fields.
func (s *Service) ListPoolInventory(ctx context.Context, opts pools.ListOpts) (PoolInventory, error) {
	inventory := PoolInventory{BackendH2C: make(map[string]bool), BackendTLS: make(map[string]PoolBackendTLS)}
	items, err := listCompleteCollection(ctx, pools.List(s.client, opts), func(result pagination.PageResult) pagination.Page {
		return pools.PoolPage{LinkedPageBase: pagination.LinkedPageBase{PageResult: result}}
	}, "pools", func(page pagination.Page) ([]pools.Pool, error) {
		var response struct {
			Pools []struct {
				pools.Pool
				BackendH2C *bool           `json:"backend_h2c"`
				Hostname   json.RawMessage `json:"backend_tls_hostname"`
				CA         json.RawMessage `json:"ca_tls_container_ref"`
				ClientRef  json.RawMessage `json:"tls_container_ref"`
				RequireH2  *bool           `json:"backend_tls_require_h2"`
				Enabled    *bool           `json:"tls_enabled"`
				Admin      *bool           `json:"admin_state_up"`
			} `json:"pools"`
		}
		if err := page.(pools.PoolPage).ExtractInto(&response); err != nil {
			return nil, err
		}
		if response.Pools == nil {
			return nil, nil
		}
		items := make([]pools.Pool, 0, len(response.Pools))
		for _, pool := range response.Pools {
			tls := PoolBackendTLS{HostnameObserved: len(pool.Hostname) != 0, CAObserved: len(pool.CA) != 0,
				ClientRefObserved: len(pool.ClientRef) != 0,
				RequireH2Observed: pool.RequireH2 != nil, EnabledObserved: pool.Enabled != nil, AdminObserved: pool.Admin != nil,
				H2CObserved: pool.BackendH2C != nil}
			tls.RequireH2 = pool.RequireH2 != nil && *pool.RequireH2
			if tls.HostnameObserved && json.Unmarshal(pool.Hostname, &tls.Hostname) != nil ||
				tls.CAObserved && json.Unmarshal(pool.CA, &pool.CATLSContainerRef) != nil ||
				tls.ClientRefObserved && json.Unmarshal(pool.ClientRef, &pool.TLSContainerRef) != nil {
				return nil, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("invalid TLS field in Octavia pool inventory"))
			}
			pool.TLSEnabled = pool.Enabled != nil && *pool.Enabled
			pool.AdminStateUp = pool.Admin != nil && *pool.Admin
			inventory.BackendTLS[pool.ID] = tls
			items = append(items, pool.Pool)
			if pool.BackendH2C != nil && *pool.BackendH2C {
				inventory.BackendH2C[pool.ID] = true
			}
		}
		return items, nil
	})
	inventory.Items = items
	return inventory, err
}

func listCompleteCollection[T any](ctx context.Context, pager pagination.Pager, create func(pagination.PageResult) pagination.Page, field string, extract func(pagination.Page) ([]T, error)) ([]T, error) {
	var items []T
	err := eachCollectionPage(ctx, pager, create, func(page pagination.Page) error {
		batch, err := extract(page)
		if err != nil {
			return err
		}
		if batch == nil {
			return missingCollection(field)
		}
		items = append(items, batch...)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", field, err)
	}
	return items, nil
}

// EachPage normally skips pages whose collection decodes as empty. Inspect
// every page before deciding whether an empty result is authoritative.
type collectionPage struct{ pagination.Page }

func (page collectionPage) IsEmpty() (bool, error) {
	return false, nil
}

func eachCollectionPage(ctx context.Context, pager pagination.Pager, create func(pagination.PageResult) pagination.Page, consume func(pagination.Page) error) error {
	if pager.Err != nil {
		return pager.Err
	}
	explicit := pager.WithPageCreator(func(result pagination.PageResult) pagination.Page {
		return collectionPage{create(result)}
	})
	explicit.Headers = pager.Headers
	return explicit.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		err := consume(page.(collectionPage).Page)
		return err == nil, err
	})
}

func missingCollection(field string) error {
	return cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("missing %s collection in Octavia response", field))
}

// CreatePoolOpts adds optional backend transport to a stock pool create request.
type CreatePoolOpts struct {
	pools.CreateOpts
	BackendH2C          bool
	BackendTLSHostname  string
	BackendTLSRequireH2 bool
}

func (opts CreatePoolOpts) ToPoolCreateMap() (map[string]any, error) {
	body, err := opts.CreateOpts.ToPoolCreateMap()
	if err != nil {
		return nil, err
	}
	pool := body["pool"].(map[string]any)
	if opts.BackendH2C {
		pool["backend_h2c"] = true
	}
	if opts.BackendTLSHostname != "" {
		pool["backend_tls_hostname"] = opts.BackendTLSHostname
	}
	if opts.BackendTLSRequireH2 {
		pool["backend_tls_require_h2"] = true
	}
	return body, nil
}

// UpdatePoolOpts adds optional backend transport to a stock pool update request.
type UpdatePoolOpts struct {
	pools.UpdateOpts
	BackendH2C        *bool
	BackendTLS        *BackendTLSSettings
	ClearTLSClientRef bool
}

// BackendTLSSettings changes hostname authentication, trust and client identity in one write.
// A zero value clears these settings on a disabled pool. RequireH2 is
// optional so existing HTTP pools do not use the gRPC provider extension.
type BackendTLSSettings struct {
	Hostname                   string
	CAReference                string
	ClientCertificateReference string
	RequireH2                  *bool
}

func (opts UpdatePoolOpts) ToPoolUpdateMap() (map[string]any, error) {
	body, err := opts.UpdateOpts.ToPoolUpdateMap()
	if err != nil {
		return nil, err
	}
	pool := body["pool"].(map[string]any)
	if opts.BackendH2C != nil {
		pool["backend_h2c"] = *opts.BackendH2C
	}
	if tls := opts.BackendTLS; tls != nil {
		pool["tls_enabled"] = tls.Hostname != ""
		pool["backend_tls_hostname"], pool["ca_tls_container_ref"] = nil, nil
		if tls.Hostname != "" {
			pool["backend_tls_hostname"], pool["ca_tls_container_ref"] = tls.Hostname, tls.CAReference
		}
		pool["tls_container_ref"], pool["crl_container_ref"] = nil, nil
		if tls.ClientCertificateReference != "" {
			pool["tls_container_ref"] = tls.ClientCertificateReference
		}
		if tls.RequireH2 != nil {
			pool["backend_tls_require_h2"] = *tls.RequireH2
		}
	}
	if opts.ClearTLSClientRef {
		pool["tls_container_ref"] = nil
	}
	return body, nil
}

// PlaintextTCPPoolOpts requests a TCP pool without backend re-encryption.
type PlaintextTCPPoolOpts struct{ pools.CreateOpts }

func (opts PlaintextTCPPoolOpts) ToPoolCreateMap() (map[string]any, error) {
	body, err := opts.CreateOpts.ToPoolCreateMap()
	if err == nil {
		body["pool"].(map[string]any)["tls_enabled"] = false
	}
	return body, err
}

// CreatePool creates one pool.
func (s *Service) CreatePool(ctx context.Context, opts pools.CreateOptsBuilder) (*pools.Pool, error) {
	return pools.Create(ctx, s.client, opts).Extract()
}

// UpdatePool updates one pool.
func (s *Service) UpdatePool(ctx context.Context, id string, opts pools.UpdateOptsBuilder) (*pools.Pool, error) {
	return pools.Update(ctx, s.client, id, opts).Extract()
}

// DeletePool deletes one pool.
func (s *Service) DeletePool(ctx context.Context, id string) error {
	return pools.Delete(ctx, s.client, id).ExtractErr()
}

// ListMembers lists the members beneath one pool for read-only inventory.
func (s *Service) ListMembers(ctx context.Context, poolID string, opts pools.ListMembersOpts) ([]pools.Member, error) {
	pages, err := pools.ListMembers(s.client, poolID, opts).AllPages(ctx)
	if err != nil {
		return nil, fmt.Errorf("list pool members: %w", err)
	}
	items, err := pools.ExtractMembers(pages)
	if err != nil {
		return nil, fmt.Errorf("extract pool members: %w", err)
	}
	return items, nil
}

// ListCompleteMembers requires an explicit member collection on every page.
func (s *Service) ListCompleteMembers(ctx context.Context, poolID string, opts pools.ListMembersOpts) ([]pools.Member, error) {
	items, _, err := s.listCompleteMembers(ctx, poolID, opts, "", nil)
	return items, err
}

// ListMembersWithBackup reports the IDs whose backup flag is omitted or null.
// The SDK's boolean alone cannot distinguish either case from explicit false.
func (s *Service) ListMembersWithBackup(ctx context.Context, poolID string, opts pools.ListMembersOpts) ([]pools.Member, map[string]struct{}, error) {
	return s.listCompleteMembers(ctx, poolID, opts, "backup", nil)
}

// ListMembersWithAdminState reports the IDs without an explicit administrative state.
func (s *Service) ListMembersWithAdminState(ctx context.Context, poolID string, opts pools.ListMembersOpts) ([]pools.Member, map[string]struct{}, error) {
	return s.listCompleteMembers(ctx, poolID, opts, "admin_state_up", nil)
}

// ListMembersForPolicyActivation proves the flags affecting member placement
// and selection without changing the ordinary withdrawal checks.
func (s *Service) ListMembersForPolicyActivation(ctx context.Context, poolID string, opts pools.ListMembersOpts, inspectAdmin bool) ([]pools.Member, map[string]struct{}, map[string]struct{}, error) {
	var inspect string
	if inspectAdmin {
		inspect = "admin_state_up"
	}
	missingActivationFields := make(map[string]struct{})
	items, missingAdmin, err := s.listCompleteMembers(ctx, poolID, opts, inspect, missingActivationFields)
	return items, missingAdmin, missingActivationFields, err
}

func (s *Service) listCompleteMembers(ctx context.Context, poolID string, opts pools.ListMembersOpts, inspectFlag string, missingActivationFields map[string]struct{}) ([]pools.Member, map[string]struct{}, error) {
	var items []pools.Member
	var unreported map[string]struct{}
	err := eachCollectionPage(ctx, pools.ListMembers(s.client, poolID, opts), func(result pagination.PageResult) pagination.Page {
		return pools.MemberPage{LinkedPageBase: pagination.LinkedPageBase{PageResult: result}}
	}, func(page pagination.Page) error {
		batch, err := pools.ExtractMembers(page)
		if err != nil {
			return err
		}
		if batch == nil {
			return missingCollection("members")
		}
		items = append(items, batch...)
		if inspectFlag == "" && missingActivationFields == nil {
			return nil
		}
		var response struct {
			Members []struct {
				ID           string `json:"id"`
				Backup       *bool  `json:"backup"`
				AdminStateUp *bool  `json:"admin_state_up"`
				Weight       *int   `json:"weight"`
			} `json:"members"`
		}
		if err := page.(pools.MemberPage).ExtractInto(&response); err != nil {
			return fmt.Errorf("extract member %s flags: %w", inspectFlag, err)
		}
		for _, member := range response.Members {
			if missingActivationFields != nil && (member.Backup == nil || member.AdminStateUp == nil || member.Weight == nil) {
				missingActivationFields[member.ID] = struct{}{}
			}
			if inspectFlag == "" {
				continue
			}
			reported := member.Backup != nil
			if inspectFlag == "admin_state_up" {
				reported = member.AdminStateUp != nil
			}
			if !reported {
				if unreported == nil {
					unreported = make(map[string]struct{})
				}
				unreported[member.ID] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("list pool members: %w", err)
	}
	return items, unreported, nil
}

// CreateMember creates one member beneath a pool.
func (s *Service) CreateMember(ctx context.Context, poolID string, opts pools.CreateMemberOpts) (*pools.Member, error) {
	return pools.CreateMember(ctx, s.client, poolID, opts).Extract()
}

// UpdateMember updates one member beneath a pool.
func (s *Service) UpdateMember(ctx context.Context, poolID, memberID string, opts pools.UpdateMemberOpts) (*pools.Member, error) {
	return pools.UpdateMember(ctx, s.client, poolID, memberID, opts).Extract()
}

// DeleteMember deletes one member beneath a pool.
func (s *Service) DeleteMember(ctx context.Context, poolID, memberID string) error {
	return pools.DeleteMember(ctx, s.client, poolID, memberID).ExtractErr()
}

// ClearMembers removes every member without changing the pool or its policies.
func (s *Service) ClearMembers(ctx context.Context, poolID string) error {
	return pools.BatchUpdateMembers(ctx, s.client, poolID, []pools.BatchUpdateMemberOpts{}).ExtractErr()
}
