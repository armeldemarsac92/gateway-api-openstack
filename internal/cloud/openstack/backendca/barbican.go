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

package backendca

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/apierrors"
	"github.com/google/uuid"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/keymanager/v1/secrets"
)

const listLimit = 100

type keyManager struct {
	service *gophercloud.ServiceClient
	creator string
}

func newKeyManager(service *gophercloud.ServiceClient, creator string) (keyManager, error) {
	if service == nil || service.ProviderClient == nil {
		return keyManager{}, ownershipError("authenticated Barbican service is unavailable")
	}
	endpoint, err := url.Parse(service.Endpoint)
	if err != nil || (endpoint.Scheme != "https" && endpoint.Scheme != "http") ||
		endpoint.Hostname() == "" || endpoint.User != nil || endpoint.RawQuery != "" ||
		endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.RawPath != "" ||
		!strings.HasSuffix(service.Endpoint, "/") || creator == "" {
		return keyManager{}, ownershipError("invalid authenticated Barbican endpoint or creator")
	}

	// Gophercloud appends v1 even when the Keystone endpoint includes it.
	service.ResourceBase = service.Endpoint
	if !strings.HasSuffix(endpoint.Path, "/v1/") {
		service.ResourceBase += "v1/"
	}
	return keyManager{service: service, creator: creator}, nil
}

func (c keyManager) secretID(reference string) string {
	id, ok := strings.CutPrefix(reference, c.service.ServiceURL("secrets", ""))
	if !ok {
		return ""
	}
	parsed, err := uuid.Parse(id)
	if err != nil || parsed == uuid.Nil || parsed.String() != id {
		return ""
	}
	return id
}

func (c keyManager) verified(ctx context.Context, name, prefix string) ([]Revision, error) {
	items, err := c.list(ctx, name, prefix)
	if err != nil {
		return nil, err
	}
	// When an ambiguous POST leads to duplicate rows, use the oldest revision
	// and keep the others visible for normal, usage-checked retirement.
	sort.Slice(items, func(i, j int) bool {
		if !items[i].Created.Equal(items[j].Created) {
			return items[i].Created.Before(items[j].Created)
		}
		return items[i].SecretRef < items[j].SecretRef
	})
	revisions := make([]Revision, 0, len(items))
	for _, item := range items {
		revision, err := c.verify(ctx, item, revisionDigest(item.Name, prefix))
		if err != nil {
			return nil, err
		}
		revisions = append(revisions, revision)
	}
	return revisions, nil
}

func (c keyManager) list(ctx context.Context, name, prefix string) ([]secrets.Secret, error) {
	var items []secrets.Secret
	seen := make(map[string]bool)
	total := -1
	for offset := 0; total == -1 || offset < total; {
		var page struct {
			Secrets []secrets.Secret
			Total   *int
		}
		// The authenticated project is the default scope. Use only the catalog
		// endpoint for paging, and reject an inventory that changes in flight.
		opts := secrets.ListOpts{Name: name, Limit: listLimit, Offset: offset, Sort: "created"}
		query, err := opts.ToSecretListQuery()
		if err != nil {
			return nil, safeError("build Barbican backend CA list query", err)
		}
		_, err = c.service.Get(ctx, c.service.ServiceURL("secrets")+query, &page, &gophercloud.RequestOpts{OkCodes: []int{http.StatusOK}})
		if err != nil {
			return nil, safeError("list Barbican backend CAs", err)
		}
		if page.Secrets == nil || page.Total == nil || *page.Total < offset ||
			(total != -1 && *page.Total != total) || len(page.Secrets) > *page.Total-offset ||
			(len(page.Secrets) == 0 && offset < *page.Total) {
			return nil, cloud.NewProviderError(cloud.ErrorCategoryRetryableService, errors.New("incomplete Barbican backend CA inventory"))
		}
		total = *page.Total
		offset += len(page.Secrets)
		for _, item := range page.Secrets {
			if revisionDigest(item.Name, prefix) == "" ||
				(name != prefix+"%" && item.Name != name) ||
				(item.CreatorID != "" && item.CreatorID != c.creator) {
				continue
			}
			if item.CreatorID == "" || c.secretID(item.SecretRef) == "" || seen[item.SecretRef] {
				return nil, ownershipError("inconclusive Barbican backend CA ownership or inventory")
			}
			seen[item.SecretRef] = true
			items = append(items, item)
		}
	}
	return items, nil
}

func (c keyManager) verify(ctx context.Context, candidate secrets.Secret, expectedDigest string) (Revision, error) {
	if !eligible(candidate) {
		return Revision{}, ownershipError("Barbican backend CA list metadata could not be verified")
	}
	id := c.secretID(candidate.SecretRef)
	observed, err := secrets.Get(ctx, c.service, id).Extract()
	if err != nil {
		return Revision{}, safeError("read Barbican backend CA metadata", err)
	}
	if observed == nil || observed.Name != candidate.Name || observed.CreatorID != c.creator ||
		observed.SecretRef != candidate.SecretRef || !eligible(*observed) {
		return Revision{}, ownershipError("Barbican backend CA metadata could not be verified")
	}
	result := secrets.GetPayload(ctx, c.service, id, secrets.GetPayloadOpts{PayloadContentType: contentType})
	if result.Err != nil {
		return Revision{}, safeError("read Barbican backend CA payload", result.Err)
	}
	defer func() { _ = result.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(result.Body, maxPEMBytes+1))
	if err != nil {
		return Revision{}, safeError("read Barbican backend CA payload", err)
	}
	if digest(body) != expectedDigest || !validPEM(body) {
		return Revision{}, ownershipError("Barbican backend CA payload could not be verified")
	}
	return Revision{name: candidate.Name, digest: expectedDigest, reference: candidate.SecretRef}, nil
}

func eligible(item secrets.Secret) bool {
	return item.SecretType == string(secrets.CertificateSecret) && item.Status == "ACTIVE" &&
		item.Expiration.IsZero() && item.ContentTypes["default"] == contentType
}

func ownershipError(message string) error {
	return cloud.NewProviderError(cloud.ErrorCategoryOwnershipConflict, errors.New(message))
}

func safeError(operation string, err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	message := operation + " failed"
	var response gophercloud.ErrUnexpectedResponseCode
	if errors.As(err, &response) {
		message += " (HTTP " + strconv.Itoa(response.Actual) + ")"
	}
	if category, ok := cloud.ErrorCategoryOf(apierrors.Classify(err)); ok {
		return cloud.NewProviderError(category, errors.New(message))
	}
	return errors.New(message)
}
