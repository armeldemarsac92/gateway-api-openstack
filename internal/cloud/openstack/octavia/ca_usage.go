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
	"errors"
	"net/url"
	"path"

	"github.com/google/uuid"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/gophercloud/gophercloud/v2/pagination"
)

// caAttachment retains null separately from a missing trust reference.
type caAttachment struct {
	ID                 string          `json:"id"`
	ProjectID          string          `json:"project_id"`
	ProvisioningStatus string          `json:"provisioning_status"`
	BackendCA          json.RawMessage `json:"ca_tls_container_ref"`
	FrontendCA         json.RawMessage `json:"client_ca_tls_container_ref"`
}

// CAUnused checks both uses of a PEM CA in the authenticated regional project.
// A pending configuration may still have the previous trust installed on an Amphora.
func (s *Service) CAUnused(ctx context.Context, projectID, reference string) (bool, error) {
	wanted, ok := caReferenceID(reference)
	if !ok || projectID == "" {
		return false, errors.New("invalid project or CA revision for Octavia usage check")
	}
	poolRows, err := listCompleteCollection(ctx, pools.List(s.client, pools.ListOpts{ProjectID: projectID}), func(result pagination.PageResult) pagination.Page {
		return pools.PoolPage{LinkedPageBase: pagination.LinkedPageBase{PageResult: result}}
	}, "pools", func(page pagination.Page) ([]caAttachment, error) {
		var response struct {
			Pools []caAttachment `json:"pools"`
		}
		err := page.(pools.PoolPage).ExtractInto(&response)
		return response.Pools, err
	})
	if err != nil {
		return false, err
	}
	for _, pool := range poolRows {
		unused, err := pool.excludes(projectID, wanted, pool.BackendCA)
		if err != nil || !unused {
			return false, err
		}
	}
	listenerRows, err := listCompleteCollection(ctx, listeners.List(s.client, listeners.ListOpts{ProjectID: projectID}), func(result pagination.PageResult) pagination.Page {
		return listeners.ListenerPage{LinkedPageBase: pagination.LinkedPageBase{PageResult: result}}
	}, "listeners", func(page pagination.Page) ([]caAttachment, error) {
		var response struct {
			Listeners []caAttachment `json:"listeners"`
		}
		err := page.(listeners.ListenerPage).ExtractInto(&response)
		return response.Listeners, err
	})
	if err != nil {
		return false, err
	}
	for _, listener := range listenerRows {
		unused, err := listener.excludes(projectID, wanted, listener.FrontendCA)
		if err != nil || !unused {
			return false, err
		}
	}
	return true, nil
}

func (row caAttachment) excludes(projectID string, wanted uuid.UUID, field json.RawMessage) (bool, error) {
	if row.ID == "" || row.ProjectID != projectID || len(field) == 0 {
		return false, errors.New("incomplete Octavia CA reference inventory")
	}
	var ref string
	if err := json.Unmarshal(field, &ref); err != nil {
		return false, errors.New("invalid CA reference in Octavia inventory")
	}
	if ref != "" {
		id, ok := caReferenceID(ref)
		if !ok {
			return false, errors.New("unrecognized CA reference in Octavia inventory")
		}
		if id == wanted {
			return false, nil
		}
	}
	if row.ProvisioningStatus != "ACTIVE" {
		return false, errors.New("cannot confirm CA detachment while Octavia changes are pending or failed")
	}
	return true, nil
}

// Catalog aliases and bare secret UUIDs can identify the same immutable Barbican revision.
func caReferenceID(reference string) (uuid.UUID, bool) {
	u, err := url.Parse(reference)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(path.Base(u.Path))
	return id, err == nil && id != uuid.Nil
}
