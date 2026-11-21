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

// Package backendca publishes immutable, Gateway-owned CA bundles in Barbican.
package backendca

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"slices"
	"strings"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/clients"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/keymanager/v1/secrets"
)

const (
	namePrefix  = "gateway-api-openstack-backend-ca-v1-"
	contentType = "application/octet-stream"
	maxPEMBytes = 128 * 1024
)

// Store manages the CA revisions of Gateway graphs. Each graph must have a
// stable cluster ID, Gateway UID, project, and region. Use a dedicated creator
// identity for the controller. Barbican does not reserve names within a creator.
//
// Serialize Ensure, pool writes, and Retire using the same Gateway writer lock.
// Keep each desired revision while its pool changes are in flight. Retirement
// requires an authoritative check of all current Octavia pool references that
// could use the revision, including pools being replaced or deleted.
type Store struct {
	clusterID string
	projectID string
	region    string
	discover  func() (*gophercloud.ServiceClient, string, error)
}

// New uses the controller's authenticated regional catalog. Barbican discovery
// happens only when a CA operation is requested.
func New(clusterID string, services clients.ServiceClients) *Store {
	return &Store{
		clusterID: clusterID,
		projectID: services.ProjectID,
		region:    services.Region,
		discover:  services.NewBackendCAKeyManagerClient,
	}
}

// Revision is a verified immutable CA secret. Keep the reference out of logs,
// Events and Kubernetes status. An Octavia pool can consume it via Reference.
type Revision struct {
	name      string
	digest    string
	reference string
}

// Reference returns the catalog-scoped reference to pass to Octavia.
func (r Revision) Reference() string { return r.reference }

// Digest returns the lowercase SHA-256 of the normalized PEM payload.
func (r Revision) Digest() string { return r.digest }

// MatchesConfigMap identifies the originating public CA revision in this Gateway scope.
func (r Revision) MatchesConfigMap(configMapUID, payloadDigest string) bool {
	return configMapUID != "" && payloadDigest != "" && r.digest == payloadDigest &&
		strings.HasSuffix(r.name, "-"+digest([]byte(configMapUID))+"-"+payloadDigest)
}

// SelectConfigMapRevision picks a verified attached revision when Barbican
// reports duplicate names. Until a pool is attached, keep the oldest match.
func SelectConfigMapRevision(revisions []Revision, configMapUID, digest string, attached func(string) bool) Revision {
	var oldest Revision
	for _, revision := range revisions {
		if !revision.MatchesConfigMap(configMapUID, digest) {
			continue
		}
		if attached(revision.reference) {
			return revision
		}
		if oldest.reference == "" {
			oldest = revision
		}
	}
	return oldest
}

// String redacts the reference in default formatting.
func (r Revision) String() string { return "[backend CA revision]" }

// GoString redacts the reference in Go-syntax formatting.
func (r Revision) GoString() string { return r.String() }

// Find reads a verified revision for the exact ConfigMap UID and normalized
// CA digest. Prefer a verified reference the pool already uses after a retry.
func (s *Store) Find(ctx context.Context, gatewayUID, configMapUID, wantedDigest, preferredReference string) (Revision, error) {
	if configMapUID == "" || !isDigest(wantedDigest) {
		return Revision{}, cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, errors.New("backend CA requires a ConfigMap UID and normalized CA digest"))
	}
	client, prefix, err := s.open(gatewayUID)
	if err != nil {
		return Revision{}, err
	}
	revisions, err := client.verified(ctx, revisionName(prefix, configMapUID, wantedDigest), prefix)
	if err != nil || len(revisions) == 0 {
		return Revision{}, err
	}
	return SelectConfigMapRevision(revisions, configMapUID, wantedDigest, func(ref string) bool {
		return ref == preferredReference
	}), nil
}

// Ensure publishes the normalized, validated ConfigMap CA PEM once and
// returns a verified reference. A retry first looks up the same immutable
// revision in the authenticated project. Barbican can briefly omit a recent
// write and can accept duplicate names. All verified duplicates remain
// eligible for explicit retirement after their own Octavia usage check.
func (s *Store) Ensure(ctx context.Context, gatewayUID, configMapUID string, normalizedPEM []byte) (Revision, error) {
	if configMapUID == "" || !validPEM(normalizedPEM) {
		return Revision{}, cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, errors.New("backend CA requires a ConfigMap UID and normalized CA PEM"))
	}
	client, prefix, err := s.open(gatewayUID)
	if err != nil {
		return Revision{}, err
	}

	name := revisionName(prefix, configMapUID, digest(normalizedPEM))
	revisions, err := client.verified(ctx, name, prefix)
	if err != nil {
		return Revision{}, err
	}
	if len(revisions) > 0 {
		return revisions[0], nil
	}

	result := secrets.Create(ctx, client.service, secrets.CreateOpts{
		Name: name, SecretType: secrets.CertificateSecret,
		PayloadContentType: contentType, PayloadContentEncoding: "base64",
		Payload: base64.StdEncoding.EncodeToString(normalizedPEM),
	})
	created, createErr := result.Extract()
	if createErr == nil && (created == nil || client.secretID(created.SecretRef) == "") {
		return Revision{}, ownershipError("Barbican CA create returned an invalid secret reference")
	}
	// A lost response can follow a committed POST. Rediscover in the project
	// inventory and verify payloads before exposing any reference to Octavia.
	revisions, err = client.verified(ctx, name, prefix)
	if err != nil {
		return Revision{}, err
	}
	if len(revisions) > 0 {
		return revisions[0], nil
	}
	if createErr != nil {
		return Revision{}, safeError("create Barbican backend CA", createErr)
	}
	return Revision{}, cloud.NewProviderError(cloud.ErrorCategoryDependencyNotReady, errors.New("new Barbican backend CA is not yet visible"))
}

// List returns the verified revisions for this Gateway, including individually
// verified duplicate rows. An inconclusive inventory returns an error and no
// revisions. This operation reads payloads only for owned candidates.
func (s *Store) List(ctx context.Context, gatewayUID string) ([]Revision, error) {
	client, prefix, err := s.open(gatewayUID)
	if err != nil {
		return nil, err
	}
	return client.verified(ctx, prefix+"%", prefix)
}

// Retire deletes precisely the supplied revision from this Gateway's scope
// if isUnused confirms the reference is absent from all current Octavia pools.
// The caller must hold the Gateway writer lock and exclude desired revisions.
// This operation rechecks live Barbican ownership and payload first. A false
// result preserves a live revision, and a missing or failed check fails closed.
func (s *Store) Retire(ctx context.Context, gatewayUID string, revision Revision, isUnused func(context.Context, string) (bool, error)) error {
	if isUnused == nil {
		return errors.New("backend CA retirement requires an Octavia usage check")
	}
	client, prefix, err := s.open(gatewayUID)
	if err != nil {
		return err
	}
	id := client.secretID(revision.reference)
	if id == "" || revisionDigest(revision.name, prefix) != revision.digest || revision.digest == "" {
		return ownershipError("backend CA revision does not belong to this Gateway scope")
	}
	candidates, err := client.list(ctx, revision.name, prefix)
	if err != nil {
		return err
	}
	index := slices.IndexFunc(candidates, func(item secrets.Secret) bool { return item.SecretRef == revision.reference })
	if index == -1 {
		return nil
	}
	if _, err := client.verify(ctx, candidates[index], revision.digest); err != nil {
		return err
	}
	unused, err := isUnused(ctx, revision.reference)
	if err != nil {
		return safeError("verify Octavia pool usage of Barbican backend CA", err)
	}
	if !unused {
		return nil
	}
	if err := secrets.Delete(ctx, client.service, id).ExtractErr(); err != nil && !gophercloud.ResponseCodeIs(err, 404) {
		return safeError("delete Barbican backend CA", err)
	}
	return nil
}

func (s *Store) open(gatewayUID string) (keyManager, string, error) {
	scope := []string{s.clusterID, gatewayUID, s.projectID, s.region}
	for _, part := range scope {
		if strings.TrimSpace(part) == "" || strings.ContainsRune(part, 0) {
			return keyManager{}, "", cloud.NewProviderError(cloud.ErrorCategoryTerminalValidation, errors.New("backend CA requires cluster, Gateway, OpenStack project and region identities"))
		}
	}
	service, creator, err := s.discover()
	if err != nil {
		return keyManager{}, "", safeError("open regional Barbican backend CA service", err)
	}
	client, err := newKeyManager(service, creator)
	if err != nil {
		return keyManager{}, "", err
	}
	return client, namePrefix + digest([]byte(strings.Join(scope, "\x00"))) + "-", nil
}

func revisionDigest(name, prefix string) string {
	suffix, ok := strings.CutPrefix(name, prefix)
	if !ok {
		return ""
	}
	sourceDigest, payloadDigest, ok := strings.Cut(suffix, "-")
	if !ok || !isDigest(sourceDigest) || !isDigest(payloadDigest) {
		return ""
	}
	return payloadDigest
}

func revisionName(prefix, configMapUID, pemDigest string) string {
	return prefix + digest([]byte(configMapUID)) + "-" + pemDigest
}

func isDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func validPEM(data []byte) bool {
	if len(data) == 0 || len(data) > maxPEMBytes {
		return false
	}
	for len(data) > 0 {
		block, rest := pem.Decode(data)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 ||
			!bytes.Equal(data[:len(data)-len(rest)], pem.EncodeToMemory(block)) {
			return false
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.BasicConstraintsValid || !cert.IsCA {
			return false
		}
		data = rest
	}
	return true
}
