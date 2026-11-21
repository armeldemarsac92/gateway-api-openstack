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

// ListCompletePools requires an explicit pool collection on every page.
func (s *Service) ListCompletePools(ctx context.Context, opts pools.ListOpts) ([]pools.Pool, error) {
	var items []pools.Pool
	err := eachCollectionPage(ctx, pools.List(s.client, opts), func(result pagination.PageResult) pagination.Page {
		return pools.PoolPage{LinkedPageBase: pagination.LinkedPageBase{PageResult: result}}
	}, func(page pagination.Page) error {
		batch, err := pools.ExtractPools(page)
		if err != nil {
			return err
		}
		if batch == nil {
			return missingCollection("pools")
		}
		items = append(items, batch...)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list pools: %w", err)
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

// CreatePool creates one pool.
func (s *Service) CreatePool(ctx context.Context, opts pools.CreateOpts) (*pools.Pool, error) {
	return pools.Create(ctx, s.client, opts).Extract()
}

// UpdatePool updates one pool.
func (s *Service) UpdatePool(ctx context.Context, id string, opts pools.UpdateOpts) (*pools.Pool, error) {
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
	items, _, err := s.listCompleteMembers(ctx, poolID, opts, false)
	return items, err
}

// ListMembersWithBackup reports the IDs whose backup flag is omitted or null.
// The SDK's boolean alone cannot distinguish either case from explicit false.
func (s *Service) ListMembersWithBackup(ctx context.Context, poolID string, opts pools.ListMembersOpts) ([]pools.Member, map[string]struct{}, error) {
	return s.listCompleteMembers(ctx, poolID, opts, true)
}

func (s *Service) listCompleteMembers(ctx context.Context, poolID string, opts pools.ListMembersOpts, inspectBackup bool) ([]pools.Member, map[string]struct{}, error) {
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
		if !inspectBackup {
			return nil
		}
		var response struct {
			Members []struct {
				ID     string `json:"id"`
				Backup *bool  `json:"backup"`
			} `json:"members"`
		}
		if err := page.(pools.MemberPage).ExtractInto(&response); err != nil {
			return fmt.Errorf("extract member backup flags: %w", err)
		}
		for _, member := range response.Members {
			if member.Backup == nil {
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
