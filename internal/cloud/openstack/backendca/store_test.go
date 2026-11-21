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
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/google/uuid"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/keymanager/v1/secrets"
)

const (
	cluster = "cluster-a"
	gateway = "gateway-a"
	project = "project-a"
	region  = "region-a"
	source  = "configmap-a"
	creator = "controller-user"
)

func TestEnsureRecoversLostCreateAndPreservesOtherCreator(t *testing.T) {
	fixture, store := newBarbican(t)
	ca := testCA(t, 1)
	_, prefix, err := store.open(gateway)
	if err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	fixture.add(prefix+digest([]byte(source))+"-"+digest(ca), "other-creator", ca)
	fixture.loseCreateResponse = true
	fixture.mu.Unlock()

	first, err := store.Ensure(t.Context(), gateway, source, ca)
	if err != nil || first.Reference() == "" || first.Digest() != digest(ca) {
		t.Fatalf("Ensure after lost response returned an unverified revision: %v", err)
	}
	retry, err := store.Ensure(t.Context(), gateway, source, ca)
	if err != nil || retry != first {
		t.Fatalf("retry did not recover the same verified revision: %v", err)
	}
	items, err := store.List(t.Context(), gateway)
	if err != nil || len(items) != 1 || items[0] != first {
		t.Fatalf("list included a secret from another creator: %v", err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.posts != 1 || fixture.foreignReads != 0 || fixture.deletes != 0 || len(fixture.rows) != 2 {
		t.Fatal("Ensure retried POST or accessed an unrelated secret")
	}
	if strings.Contains(fmt.Sprintf("%+v %#v", first, first), first.Reference()) {
		t.Fatal("formatted revision exposes its reference")
	}
}

func TestRotateAndRetireOnlyUnusedOwnedRevision(t *testing.T) {
	fixture, store := newBarbican(t)
	oldCA, newCA := testCA(t, 1), testCA(t, 2)
	old, err := store.Ensure(t.Context(), gateway, source, oldCA)
	if err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	duplicate := fixture.add(old.name, creator, oldCA)
	fixture.mu.Unlock()
	next, err := store.Ensure(t.Context(), gateway, source, newCA)
	if err != nil || next == old {
		t.Fatalf("rotated CA did not receive a new revision: %v", err)
	}
	replacement, err := store.Ensure(t.Context(), gateway, "recreated-configmap", oldCA)
	if err != nil || replacement == old || replacement.Digest() != old.Digest() {
		t.Fatalf("recreated ConfigMap did not receive a distinct revision: %v", err)
	}
	if !old.MatchesConfigMap(source, old.Digest()) || old.MatchesConfigMap(source, next.Digest()) ||
		old.MatchesConfigMap("recreated-configmap", replacement.Digest()) {
		t.Fatal("CA retention must match both the ConfigMap UID and normalized certificate digest")
	}
	again, err := store.Ensure(t.Context(), gateway, source, oldCA)
	if err != nil || again != old {
		t.Fatalf("duplicate caused unstable CA selection: %v", err)
	}
	inventory, err := store.List(t.Context(), gateway)
	if err != nil || len(inventory) != 4 {
		t.Fatalf("expected all owned revisions, got %d: %v", len(inventory), err)
	}
	attached := func(ref string) bool { return ref == duplicate }
	if selected := SelectConfigMapRevision(inventory, source, old.Digest(), attached); selected.Reference() != duplicate {
		t.Fatal("the older unattached duplicate would survive every retirement sweep")
	}
	if selected := SelectConfigMapRevision(inventory, source, old.Digest(), func(string) bool { return false }); selected != old {
		t.Fatal("lost the oldest revision while waiting for the first pool attachment")
	}

	live := map[string]bool{old.Reference(): true, next.Reference(): true}
	check := func(_ context.Context, ref string) (bool, error) { return !live[ref], nil }
	if err := store.Retire(t.Context(), "other-gateway", old, check); !errors.Is(err, cloud.ErrOwnershipConflict) {
		t.Fatalf("another Gateway could retire an owned CA: %v", err)
	}
	if err := store.Retire(t.Context(), gateway, old, func(context.Context, string) (bool, error) {
		return false, errors.New(old.Reference())
	}); err == nil || strings.Contains(err.Error(), old.Reference()) {
		t.Fatal("unknown pool usage did not fail closed with a redacted error")
	}
	if err := store.Retire(t.Context(), gateway, old, check); err != nil {
		t.Fatal(err)
	}
	for _, revision := range inventory {
		if revision.Reference() == duplicate {
			if err := store.Retire(t.Context(), gateway, revision, check); err != nil {
				t.Fatal(err)
			}
		}
	}
	delete(live, old.Reference())
	if err := store.Retire(t.Context(), gateway, old, check); err != nil {
		t.Fatal(err)
	}
	if err := store.Retire(t.Context(), gateway, old, check); err != nil {
		t.Fatalf("retirement retry failed: %v", err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.deletes != 2 || len(fixture.rows) != 2 ||
		fixture.rows[next.Reference()] == nil || fixture.rows[replacement.Reference()] == nil {
		t.Fatal("retirement did not preserve the remaining CA revisions")
	}
}

func TestUnverifiedInventoryPreventsAdoptionAndRetirement(t *testing.T) {
	for _, failure := range []string{"digest mismatch", "missing creator", "different catalog", "incomplete listing"} {
		t.Run(failure, func(t *testing.T) {
			fixture, store := newBarbican(t)
			ca := testCA(t, 1)
			revision, err := store.Ensure(t.Context(), gateway, source, ca)
			if err != nil {
				t.Fatal(err)
			}
			fixture.mu.Lock()
			row := fixture.rows[revision.Reference()]
			switch failure {
			case "digest mismatch":
				row.payload = testCA(t, 2)
			case "missing creator":
				row.CreatorID = ""
			case "different catalog":
				row.SecretRef = "https://untrusted.invalid/v1/secrets/" + uuid.NewString()
			case "incomplete listing":
				fixture.incompleteList = true
			}
			fixture.mu.Unlock()

			_, err = store.Ensure(t.Context(), gateway, source, ca)
			if err == nil || strings.Contains(err.Error(), revision.Reference()) ||
				strings.Contains(err.Error(), "untrusted.invalid") {
				t.Fatal("Ensure accepted an unverified revision or leaked its reference")
			}
			err = store.Retire(t.Context(), gateway, revision, func(context.Context, string) (bool, error) {
				t.Error("retirement consulted pool usage before verifying ownership")
				return true, nil
			})
			if err == nil {
				t.Fatal("retirement accepted an unverified inventory")
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if fixture.posts != 1 || fixture.deletes != 0 {
				t.Fatal("unverified inventory caused a mutation")
			}
		})
	}
}

type barbicanRow struct {
	secrets.Secret
	created string
	payload []byte
}

func (r *barbicanRow) metadata() map[string]any {
	return map[string]any{
		"name": r.Name, "creator_id": r.CreatorID, "secret_ref": r.SecretRef,
		"status": r.Status, "secret_type": r.SecretType, "content_types": r.ContentTypes,
		"created": r.created, "expiration": nil,
	}
}

type barbicanFixture struct {
	mu                 sync.Mutex
	rows               map[string]*barbicanRow
	base               string
	loseCreateResponse bool
	incompleteList     bool
	posts              int
	deletes            int
	foreignReads       int
}

func newBarbican(t *testing.T) (*barbicanFixture, *Store) {
	t.Helper()
	fixture := &barbicanFixture{rows: make(map[string]*barbicanRow)}
	server := httptest.NewServer(http.HandlerFunc(fixture.serve))
	t.Cleanup(server.Close)
	fixture.base = server.URL + "/v1/"
	return fixture, &Store{
		clusterID: cluster, projectID: project, region: region,
		discover: func() (*gophercloud.ServiceClient, string, error) {
			return &gophercloud.ServiceClient{
				ProviderClient: &gophercloud.ProviderClient{HTTPClient: *server.Client()},
				Endpoint:       fixture.base,
				ResourceBase:   fixture.base + "v1/",
			}, creator, nil
		},
	}
}

func (f *barbicanFixture) add(name, secretCreator string, payload []byte) string {
	ref := f.base + "secrets/" + uuid.NewString()
	f.rows[ref] = &barbicanRow{
		Secret: secrets.Secret{
			Name: name, CreatorID: secretCreator, SecretRef: ref, Status: "ACTIVE",
			SecretType: string(secrets.CertificateSecret), ContentTypes: map[string]string{"default": contentType},
		},
		created: time.Date(2026, 1, 1, 0, 0, len(f.rows), 0, time.UTC).Format(gophercloud.RFC3339NoZ),
		payload: bytes.Clone(payload),
	}
	return ref
}

func (f *barbicanFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/v1/secrets" {
		switch r.Method {
		case http.MethodGet:
			f.list(w, r)
		case http.MethodPost:
			f.create(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		return
	}

	path, payload := strings.CutSuffix(r.URL.Path, "/payload")
	ref := strings.TrimSuffix(f.base, "/v1/") + path
	row := f.rows[ref]
	if row == nil {
		http.NotFound(w, r)
		return
	}
	if row.CreatorID != creator {
		f.foreignReads++
	}
	switch {
	case r.Method == http.MethodDelete && !payload:
		f.deletes++
		delete(f.rows, ref)
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodGet && payload && r.Header.Get("Accept") == contentType:
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(row.payload)
	case r.Method == http.MethodGet && !payload:
		_ = json.NewEncoder(w).Encode(row.metadata())
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (f *barbicanFixture) list(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	prefix, wildcard := strings.CutSuffix(name, "%")
	rows := make([]*barbicanRow, 0)
	for _, row := range f.rows {
		if row.Name == name || (wildcard && strings.HasPrefix(row.Name, prefix)) {
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].created < rows[j].created })
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	page := make([]map[string]any, 0, 1)
	if offset >= 0 && offset < len(rows) && !f.incompleteList {
		// Exercise paging with a lower server-side limit than the request.
		page = append(page, rows[offset].metadata())
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"secrets": page, "total": len(rows)})
}

func (f *barbicanFixture) create(w http.ResponseWriter, r *http.Request) {
	f.posts++
	var fields secrets.CreateOpts
	if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	payload, err := base64.StdEncoding.DecodeString(fields.Payload)
	if err != nil || fields.SecretType != secrets.CertificateSecret ||
		fields.PayloadContentType != contentType || fields.PayloadContentEncoding != "base64" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ref := f.add(fields.Name, creator, payload)
	if f.loseCreateResponse {
		f.loseCreateResponse = false
		connection, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			_ = connection.Close()
		}
		return
	}
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{"secret_ref": ref})
}

func testCA(t *testing.T, serial int64) []byte {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "test CA"},
		BasicConstraintsValid: true, IsCA: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
