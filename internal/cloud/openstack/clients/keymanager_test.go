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

package clients

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gophercloud/gophercloud/v2"
	tokensv3 "github.com/gophercloud/gophercloud/v2/openstack/identity/v3/tokens"
)

func TestKeyManagerClientPreservesEndpointSelection(t *testing.T) {
	var selected gophercloud.EndpointOpts
	provider := &gophercloud.ProviderClient{EndpointLocator: func(options gophercloud.EndpointOpts) (string, error) {
		selected = options
		return "https://barbican.invalid/v1/", nil
	}}
	serviceClients := ServiceClients{
		Network:      &gophercloud.ServiceClient{ProviderClient: provider},
		endpointOpts: gophercloud.EndpointOpts{Region: "test-region", Availability: gophercloud.AvailabilityInternal},
	}
	keyManager, err := serviceClients.NewKeyManagerClient()
	if err != nil {
		t.Fatal(err)
	}
	if selected.Region != "test-region" || selected.Availability != gophercloud.AvailabilityInternal || selected.Type != "key-manager" {
		t.Fatalf("endpoint selection was not preserved: %+v", selected)
	}
	if keyManager.ProviderClient != provider {
		t.Fatal("Barbican did not reuse the authenticated provider and rate limiter")
	}
	if _, err := (ServiceClients{}).NewKeyManagerClient(); err == nil {
		t.Fatal("missing authenticated provider was accepted")
	}
}

func TestBackendCAKeyManagerPinsPrincipalAndRejectsRedirects(t *testing.T) {
	var redirected atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirected.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer other.Close()
	var requests atomic.Int64
	barbican := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("X-Auth-Token") != "initial-token" {
			t.Error("request did not use the originally authenticated creator")
		}
		if r.URL.Path == "/v1/redirect" {
			http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer barbican.Close()

	result := func(token, project, user string) tokensv3.GetResult {
		var result tokensv3.GetResult
		result.Header = http.Header{"X-Subject-Token": {token}}
		result.Body = map[string]any{"token": map[string]any{
			"project": map[string]string{"id": project},
			"user":    map[string]string{"id": user},
		}}
		return result
	}
	region := "test-region"
	provider := &gophercloud.ProviderClient{
		HTTPClient: *barbican.Client(),
		EndpointLocator: func(opts gophercloud.EndpointOpts) (string, error) {
			if opts.Region != region || opts.Type != "key-manager" {
				t.Error("Barbican discovery changed the selected catalog scope")
			}
			return barbican.URL + "/", nil
		},
	}
	provider.UseTokenLock()
	if err := provider.SetTokenAndAuthResult(result("initial-token", "project-a", "creator-a")); err != nil {
		t.Fatal("seed Keystone authentication failed")
	}
	provider.ReauthFunc = func(context.Context) error {
		return provider.SetTokenAndAuthResult(result("changed-token", "project-a", "creator-b"))
	}
	clients := ServiceClients{
		Network: &gophercloud.ServiceClient{ProviderClient: provider}, ProjectID: "project-a",
		Region: region, endpointOpts: gophercloud.EndpointOpts{Region: region},
	}
	client, creator, err := clients.NewBackendCAKeyManagerClient()
	if err != nil || creator != "creator-a" {
		t.Fatalf("backend CA identity is unavailable: %v", err)
	}
	_, err = client.Get(t.Context(), client.ServiceURL("redirect"), nil, nil)
	if !gophercloud.ResponseCodeIs(err, http.StatusTemporaryRedirect) || redirected.Load() {
		t.Fatal("Barbican redirect was followed")
	}
	_, err = client.Get(t.Context(), client.ServiceURL("require-auth"), nil, nil)
	if err == nil || requests.Load() != 2 {
		t.Fatal("Barbican retried the request with a different creator")
	}

	clients.ProjectID = "other-project"
	if _, _, err := clients.NewBackendCAKeyManagerClient(); err == nil {
		t.Fatal("client accepted a project differing from Keystone scope")
	}
}
