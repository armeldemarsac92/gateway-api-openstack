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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/gophercloud/gophercloud/v2/pagination"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia/amphora"
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
	Items                  []pools.Pool
	BackendH2C             map[string]bool
	BackendTLS             map[string]PoolBackendTLS
	BackendRetryBudget     map[string]PoolBackendRetryBudget
	TCPConnectionRejection map[string]PoolTCPConnectionRejection
	UDPPacketDrop          map[string]PoolUDPPacketDrop
	CookieDeadline         map[string]PoolCookieDeadline
	CookiePath             map[string]PoolCookiePath
}

// PoolCookieDeadline preserves an explicit zero and an invalid readback.
type PoolCookieDeadline struct {
	Value   *cloud.HTTPSessionCookieDeadline
	Invalid bool
}

// PoolCookiePath distinguishes an absent default, an explicit empty path and malformed readback.
type PoolCookiePath struct {
	Value   *string
	Invalid bool
}

// PoolBackendTLS records which transport fields the API actually reported.
// Explicit null is a reported absence of a hostname or certificate reference.
type PoolBackendTLS struct {
	SystemCA                bool
	SystemCAObserved        bool
	SystemCASHA256          string
	SystemCADigestObserved  bool
	SystemCAInvalid         bool
	Hostname                string
	SubjectAltNames         []cloud.BackendTLSSubjectAltName
	SubjectAltNamesObserved bool
	SubjectAltNamesInvalid  bool
	HostnameObserved        bool
	CAObserved              bool
	ClientRefObserved       bool
	RequireH2               bool
	RequireH2Observed       bool
	ALPNObserved            bool
	EnabledObserved         bool
	H2CObserved             bool
	AdminObserved           bool
}

func (tls PoolBackendTLS) SystemCAApplied() bool {
	return tls.SystemCAObserved && tls.SystemCA && tls.SystemCADigestObserved && !tls.SystemCAInvalid && tls.SystemCASHA256 != ""
}

func (tls PoolBackendTLS) Complete() bool {
	return tls.HostnameObserved && tls.CAObserved && tls.ClientRefObserved && tls.EnabledObserved && tls.AdminObserved
}

// ListPoolInventory requires an explicit pool collection on every page and
// reads backend transport alongside the stock fields.
func (s *Service) ListPoolInventory(ctx context.Context, opts pools.ListOpts) (PoolInventory, error) {
	inventory := PoolInventory{BackendH2C: make(map[string]bool), BackendTLS: make(map[string]PoolBackendTLS),
		BackendRetryBudget: make(map[string]PoolBackendRetryBudget), TCPConnectionRejection: make(map[string]PoolTCPConnectionRejection),
		UDPPacketDrop: make(map[string]PoolUDPPacketDrop), CookieDeadline: make(map[string]PoolCookieDeadline),
		CookiePath: make(map[string]PoolCookiePath)}
	query, err := opts.ToPoolListQuery()
	if err != nil {
		return inventory, fmt.Errorf("list pools: %w", err)
	}
	var items []pools.Pool
	for pageURL := s.client.ServiceURL("lbaas", "pools") + query; pageURL != ""; {
		var response struct {
			Links []gophercloud.Link `json:"pools_links"`
			Pools []struct {
				pools.Pool
				SessionPersistence     json.RawMessage `json:"session_persistence"`
				BackendRetryBudget     json.RawMessage `json:"backend_retry_budget"`
				TCPConnectionRejection json.RawMessage `json:"tcp_connection_rejection"`
				UDPPacketDrop          json.RawMessage `json:"udp_packet_drop"`
				UpdatedAt              json.RawMessage `json:"updated_at"`
				BackendH2C             *bool           `json:"backend_h2c"`
				Hostname               json.RawMessage `json:"backend_tls_hostname"`
				SystemCA               json.RawMessage `json:"backend_tls_system_ca"`
				SystemCASHA256         json.RawMessage `json:"backend_tls_system_ca_sha256"`
				SubjectAltNames        json.RawMessage `json:"backend_tls_subject_alt_names"`
				CA                     json.RawMessage `json:"ca_tls_container_ref"`
				ClientRef              json.RawMessage `json:"tls_container_ref"`
				RequireH2              *bool           `json:"backend_tls_require_h2"`
				ALPNProtocols          json.RawMessage `json:"alpn_protocols"`
				Enabled                *bool           `json:"tls_enabled"`
				Admin                  *bool           `json:"admin_state_up"`
			} `json:"pools"`
		}
		// Decode directly from the response. The SDK pager first decodes
		// every object into a map, discarding duplicate SAN keys.
		if _, err := s.client.Get(ctx, pageURL, &response, &gophercloud.RequestOpts{OkCodes: []int{200, 204, 300}}); err != nil {
			return inventory, fmt.Errorf("list pools: %w", err)
		}
		if response.Pools == nil {
			return inventory, fmt.Errorf("list pools: %w", missingCollection("pools"))
		}
		for _, pool := range response.Pools {
			tls := PoolBackendTLS{HostnameObserved: len(pool.Hostname) != 0, CAObserved: len(pool.CA) != 0,
				ClientRefObserved: len(pool.ClientRef) != 0,
				RequireH2Observed: pool.RequireH2 != nil, EnabledObserved: pool.Enabled != nil, AdminObserved: pool.Admin != nil,
				H2CObserved: pool.BackendH2C != nil}
			tls.RequireH2 = pool.RequireH2 != nil && *pool.RequireH2
			tls.observeSystemCA(pool.SystemCA, pool.SystemCASHA256)
			// Unavailable ALPN readback must hold only the target pool.
			tls.ALPNObserved = len(pool.ALPNProtocols) != 0 && json.Unmarshal(pool.ALPNProtocols, &pool.Pool.ALPNProtocols) == nil
			if tls.HostnameObserved && json.Unmarshal(pool.Hostname, &tls.Hostname) != nil ||
				tls.CAObserved && json.Unmarshal(pool.CA, &pool.CATLSContainerRef) != nil ||
				tls.ClientRefObserved && json.Unmarshal(pool.ClientRef, &pool.TLSContainerRef) != nil {
				return inventory, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, fmt.Errorf("invalid TLS field in Octavia pool inventory"))
			}
			// A malformed SAN readback must keep this pool closed. Continue
			// reading the inventory so the reconciler can disable it.
			if len(pool.SubjectAltNames) != 0 {
				tls.SubjectAltNamesObserved = true
				var err error
				tls.SubjectAltNames, err = decodeBackendTLSSubjectAltNames(pool.SubjectAltNames)
				tls.SubjectAltNamesInvalid = err != nil
			}
			pool.TLSEnabled = pool.Enabled != nil && *pool.Enabled
			pool.AdminStateUp = pool.Admin != nil && *pool.Admin
			inventory.BackendTLS[pool.ID] = tls
			persistence, deadline, path, err := decodePoolCookie(pool.SessionPersistence)
			if err != nil {
				return inventory, fmt.Errorf("decode Octavia pool session persistence: %w", err)
			}
			pool.Persistence = persistence
			inventory.CookieDeadline[pool.ID] = deadline
			inventory.CookiePath[pool.ID] = path
			inventory.BackendRetryBudget[pool.ID] = decodeBackendRetryBudget(pool.BackendRetryBudget)
			rejection := decodeTCPConnectionRejection(pool.TCPConnectionRejection)
			drop := decodeUDPPacketDrop(pool.UDPPacketDrop)
			if rejection.Present || drop.Present {
				var timestamp gophercloud.JSONRFC3339NoZ
				if json.Unmarshal(pool.UpdatedAt, &timestamp) == nil {
					rejection.UpdatedAt = time.Time(timestamp)
				} else {
					_ = json.Unmarshal(pool.UpdatedAt, &rejection.UpdatedAt)
				}
				drop.UpdatedAt = rejection.UpdatedAt
			}
			inventory.TCPConnectionRejection[pool.ID] = rejection
			inventory.UDPPacketDrop[pool.ID] = drop
			items = append(items, pool.Pool)
			if pool.BackendH2C != nil && *pool.BackendH2C {
				inventory.BackendH2C[pool.ID] = true
			}
		}
		pageURL, err = gophercloud.ExtractNextURL(response.Links)
		if err != nil {
			return inventory, fmt.Errorf("list pools: %w", err)
		}
	}
	inventory.Items = items
	return inventory, nil
}

func decodePoolCookie(raw json.RawMessage) (pools.SessionPersistence, PoolCookieDeadline, PoolCookiePath, error) {
	var persistence struct {
		pools.SessionPersistence
		Timeout   json.RawMessage `json:"cookie_absolute_timeout_ms"`
		Scope     json.RawMessage `json:"cookie_scope"`
		Permanent json.RawMessage `json:"cookie_permanent"`
		Path      json.RawMessage `json:"cookie_path"`
	}
	if len(raw) != 0 {
		if err := json.Unmarshal(raw, &persistence); err != nil {
			return persistence.SessionPersistence, PoolCookieDeadline{}, PoolCookiePath{}, err
		}
	}
	return persistence.SessionPersistence, decodePoolCookieDeadline(persistence.Timeout, persistence.Scope, persistence.Permanent), decodePoolCookiePath(persistence.Path), nil
}

func decodePoolCookiePath(raw json.RawMessage) PoolCookiePath {
	var path *string
	if len(raw) != 0 && (json.Unmarshal(raw, &path) != nil || path != nil && !cloud.ValidHTTPSessionCookiePath(*path)) {
		return PoolCookiePath{Invalid: true}
	}
	return PoolCookiePath{Value: path}
}

func decodePoolCookieDeadline(rawTimeout, rawScope, rawPermanent json.RawMessage) PoolCookieDeadline {
	var permanent *bool
	if len(rawPermanent) != 0 && (json.Unmarshal(rawPermanent, &permanent) != nil || permanent == nil) {
		return PoolCookieDeadline{Invalid: true}
	}
	if permanent == nil && (len(rawTimeout) == 0 && len(rawScope) == 0 ||
		bytes.Equal(rawTimeout, []byte("null")) && bytes.Equal(rawScope, []byte("null"))) {
		return PoolCookieDeadline{}
	}
	var timeout *int64
	var scope string
	if json.Unmarshal(rawTimeout, &timeout) != nil || timeout == nil || json.Unmarshal(rawScope, &scope) != nil {
		return PoolCookieDeadline{Invalid: true}
	}
	deadline := cloud.HTTPSessionCookieDeadline{TimeoutMS: *timeout, Scope: scope, Permanent: permanent != nil && *permanent}
	if !deadline.Valid() {
		return PoolCookieDeadline{Invalid: true}
	}
	return PoolCookieDeadline{Value: &deadline}
}

func (tls *PoolBackendTLS) observeSystemCA(mode, digest json.RawMessage) {
	if len(mode) != 0 {
		switch string(mode) {
		case "true":
			tls.SystemCA, tls.SystemCAObserved = true, true
		case "false":
			tls.SystemCAObserved = true
		default:
			tls.SystemCAInvalid = true
		}
	}
	if len(digest) != 0 {
		tls.SystemCADigestObserved = true
		if string(digest) == "null" {
			return
		}
		err := json.Unmarshal(digest, &tls.SystemCASHA256)
		hash, hexErr := hex.DecodeString(tls.SystemCASHA256)
		if err != nil || hexErr != nil || len(hash) != sha256.Size || tls.SystemCASHA256 != strings.ToLower(tls.SystemCASHA256) {
			tls.SystemCAInvalid = true
		}
	}
}

func decodeBackendTLSSubjectAltNames(raw json.RawMessage) ([]cloud.BackendTLSSubjectAltName, error) {
	invalid := fmt.Errorf("invalid backend TLS subjectAltNames readback")
	var rawNames []json.RawMessage
	if err := json.Unmarshal(raw, &rawNames); err != nil || rawNames == nil {
		return nil, invalid
	}
	names := make([]cloud.BackendTLSSubjectAltName, 0, len(rawNames))
	for _, rawName := range rawNames {
		decoder := json.NewDecoder(bytes.NewReader(rawName))
		if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
			return nil, invalid
		}
		fields := make(map[string]string, 2)
		for decoder.More() {
			token, err := decoder.Token()
			key, ok := token.(string)
			if err != nil || !ok || fields[key] != "" || len(fields) == 2 {
				return nil, invalid
			}
			var value string
			if err := decoder.Decode(&value); err != nil || value == "" {
				return nil, invalid
			}
			fields[key] = value
		}
		name := cloud.BackendTLSSubjectAltName{Type: fields["type"]}
		switch {
		case len(fields) == 2 && name.Type == cloud.BackendTLSSANHostname && fields["hostname"] != "":
			name.Hostname = fields["hostname"]
		case len(fields) == 2 && name.Type == cloud.BackendTLSSANURI && fields["uri"] != "":
			name.URI = fields["uri"]
		default:
			return nil, invalid
		}
		names = append(names, name)
	}
	return amphora.NormalizeBackendTLSSubjectAltNames(names)
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
	CookieDeadline            *cloud.HTTPSessionCookieDeadline
	CookiePath                *string
	BackendRetryBudget        *cloud.BackendRetryBudget
	BackendH2C                bool
	BackendTLSHostname        string
	BackendTLSSubjectAltNames []cloud.BackendTLSSubjectAltName
	BackendTLSRequireH2       bool
	BackendTLSSystemCA        bool
}

func (opts CreatePoolOpts) ToPoolCreateMap() (map[string]any, error) {
	body, err := opts.CreateOpts.ToPoolCreateMap()
	if err != nil {
		return nil, err
	}
	pool := body["pool"].(map[string]any)
	if opts.CookieDeadline != nil || opts.CookiePath != nil {
		pool["session_persistence"], err = extendedCookiePersistence(opts.Persistence, opts.CookieDeadline, opts.CookiePath, false)
		if err != nil {
			return nil, err
		}
	}
	if opts.BackendRetryBudget != nil {
		pool[backendRetryBudgetField] = opts.BackendRetryBudget
	}
	if opts.BackendH2C {
		pool["backend_h2c"] = true
	}
	if opts.BackendTLSHostname != "" {
		pool["backend_tls_hostname"] = opts.BackendTLSHostname
	}
	if len(opts.BackendTLSSubjectAltNames) != 0 {
		pool["backend_tls_subject_alt_names"] = opts.BackendTLSSubjectAltNames
	}
	if opts.BackendTLSSystemCA {
		pool["backend_tls_system_ca"] = true
	}
	if opts.BackendTLSRequireH2 {
		pool["backend_tls_require_h2"] = true
	}
	return body, nil
}

// UpdatePoolOpts adds optional backend transport to a stock pool update request.
type UpdatePoolOpts struct {
	pools.UpdateOpts
	CookieDeadline *cloud.HTTPSessionCookieDeadline
	CookiePath     *string
	// ClearCookiePermanent explicitly restores a session cookie while retaining its deadline.
	ClearCookiePermanent bool
	BackendRetryBudget   *BackendRetryBudgetUpdate
	BackendH2C           *bool
	BackendTLS           *BackendTLSSettings
	ClearTLSClientRef    bool
}

// BackendTLSSettings changes hostname authentication, trust and client identity in one write.
// A zero value clears these settings on a disabled pool. RequireH2 is
// optional so existing HTTP pools do not use the gRPC provider extension.
type BackendTLSSettings struct {
	Hostname                   string
	CAReference                string
	ClientCertificateReference string
	SystemCA                   *bool
	// A nil slice leaves the field absent in the PUT. An empty non-nil
	// slice clears explicit SAN authentication on the disabled pool.
	SubjectAltNames []cloud.BackendTLSSubjectAltName
	RequireH2       *bool
}

func (opts UpdatePoolOpts) ToPoolUpdateMap() (map[string]any, error) {
	body, err := opts.UpdateOpts.ToPoolUpdateMap()
	if err != nil {
		return nil, err
	}
	pool := body["pool"].(map[string]any)
	if opts.ClearCookiePermanent && opts.Persistence == nil && opts.CookieDeadline == nil && opts.CookiePath == nil {
		pool["session_persistence"] = map[string]any{"cookie_permanent": false}
	} else if opts.CookieDeadline != nil || opts.CookiePath != nil || opts.ClearCookiePermanent {
		pool["session_persistence"], err = extendedCookiePersistence(opts.Persistence, opts.CookieDeadline, opts.CookiePath, opts.ClearCookiePermanent)
		if err != nil {
			return nil, err
		}
	}
	if update := opts.BackendRetryBudget; update != nil {
		pool[backendRetryBudgetField] = update.Value
	}
	if opts.BackendH2C != nil {
		pool["backend_h2c"] = *opts.BackendH2C
	}
	if tls := opts.BackendTLS; tls != nil {
		pool["tls_enabled"] = tls.Hostname != ""
		pool["backend_tls_hostname"], pool["ca_tls_container_ref"] = nil, nil
		if tls.Hostname != "" {
			pool["backend_tls_hostname"] = tls.Hostname
			if tls.CAReference != "" {
				pool["ca_tls_container_ref"] = tls.CAReference
			}
		}
		if tls.SystemCA != nil {
			pool["backend_tls_system_ca"] = *tls.SystemCA
		}
		pool["tls_container_ref"], pool["crl_container_ref"] = nil, nil
		if tls.ClientCertificateReference != "" {
			pool["tls_container_ref"] = tls.ClientCertificateReference
		}
		if tls.SubjectAltNames != nil {
			pool["backend_tls_subject_alt_names"] = slices.Clone(tls.SubjectAltNames)
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

func extendedCookiePersistence(persistence *pools.SessionPersistence, deadline *cloud.HTTPSessionCookieDeadline, path *string, clearPermanent bool) (map[string]any, error) {
	if persistence == nil || persistence.Type != "HTTP_COOKIE" || !cloud.ValidHTTPSessionCookieName(persistence.CookieName) {
		return nil, fmt.Errorf("cookie path and absolute timeout require a named HTTP cookie")
	}
	fields := map[string]any{"type": persistence.Type, "cookie_name": persistence.CookieName}
	if path != nil {
		if !cloud.ValidHTTPSessionCookiePath(*path) {
			return nil, fmt.Errorf("invalid HTTP cookie path")
		}
		fields["cookie_path"] = *path
	}
	if deadline != nil {
		if !deadline.Valid() || clearPermanent && deadline.Permanent {
			return nil, fmt.Errorf("invalid HTTP cookie deadline")
		}
		fields["cookie_absolute_timeout_ms"] = deadline.TimeoutMS
		fields["cookie_scope"] = deadline.Scope
		if deadline.Permanent || clearPermanent {
			fields["cookie_permanent"] = deadline.Permanent
		}
	} else if clearPermanent {
		return nil, fmt.Errorf("clearing cookie permanence requires a valid deadline")
	}
	return fields, nil
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

// ListMembersForTransportBatch observes the member selection fields needed to verify a replacement.
func (s *Service) ListMembersForTransportBatch(ctx context.Context, poolID string, opts pools.ListMembersOpts) ([]pools.Member, map[string]struct{}, map[string]struct{}, error) {
	missing := make(map[string]struct{})
	items, missingBackup, err := s.listCompleteMembers(ctx, poolID, opts, "backup", missing)
	return items, missingBackup, missing, err
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
