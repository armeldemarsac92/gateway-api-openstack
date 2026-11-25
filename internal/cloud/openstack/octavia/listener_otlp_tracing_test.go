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
	"encoding/json"
	"net/http"
	"testing"

	"github.com/gophercloud/gophercloud/v2"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"

	"github.com/stretchr/testify/require"
)

func TestOTLPAttributeReadbackKeepsOwnedCollectorForWithdrawal(t *testing.T) {
	const pool = "975c99aa-e26a-4818-8391-b704f0712d1f"
	read := func(attrs string) ListenerOTLPTracingObservation {
		raw := json.RawMessage(`{"collector_pool_id":"` + pool + `","service_name":"apps","span_name":"request","attributes":` + attrs + `}`)
		return decodeListenerOTLPTracing(raw, json.RawMessage(`"ACTIVE"`))
	}
	good := read(`[{"name":"tenant","source_type":"Header","header_name":"X-Tenant"},{"name":"result","source_type":"Attribute","attribute_key":"http.response.status_code"}]`)
	require.True(t, good.Readable)
	require.Equal(t, "result", good.Value.Attributes[0].Name)
	require.Equal(t, "x-tenant", good.Value.Attributes[1].HeaderName)
	unicode := read(`[{"name":"label","source_type":"Literal","literal_value":"\ud83d\udc2c\ufffd"}]`)
	require.True(t, unicode.Readable)
	require.Equal(t, "🐬�", unicode.Value.Attributes[0].LiteralValue)
	for _, malformed := range []string{
		`[{"name":"label","source_type":"Literal","literal_value":"\ud800"}]`,
		`[{"name":"result","source_type":"Attribute","attribute_key":"unknown.key"}]`,
		`[{"name":"tenant","source_type":"Header","header_name":"X-Tenant","literal_value":"extra"}]`,
		`[{"name":"tenant","source_type":"Literal","literal_value":"blue"},{"name":"tenant","source_type":"Literal","literal_value":"green"}]`,
	} {
		observed := read(malformed)
		require.False(t, observed.Readable)
		require.NotNil(t, observed.Value)
		require.Equal(t, pool, observed.Value.CollectorPoolID)
	}
}

func TestOTLPExporterHeaderReadbackUsesOnlyTheDigest(t *testing.T) {
	const pool = "975c99aa-e26a-4818-8391-b704f0712d1f"
	headers, valid := cloud.CanonicalTelemetryExporterHeaders(cloud.TelemetryExporterHeaders{
		{Name: "X-Scope", Value: "region west"}, {Name: "X-Label", Value: "run:%/1"},
	})
	require.True(t, valid)
	require.Equal(t, "x-label", headers[0].Name)
	require.Equal(t, "ecbe16e2728f39bc74a4f99a19dd0f48f71d7f8a63d8f7743aa727682b1e24b0", headers.Digest())
	read := func(extension string) ListenerOTLPTracingObservation {
		raw := json.RawMessage(`{"collector_pool_id":"` + pool + `","service_name":"apps","span_name":"request",` + extension + `}`)
		return decodeListenerOTLPTracing(raw, json.RawMessage(`"ACTIVE"`))
	}
	current := read(`"headers_sha256":"` + headers.Digest() + `"`)
	require.True(t, current.Readable)
	require.Equal(t, headers.Digest(), current.HeadersSHA256)
	require.Empty(t, current.Value.Headers)
	for _, extension := range []string{`"headers_sha256":null`, `"headers_sha256":"bad"`, `"headers":[{"name":"x-label","value":"blue"}]`} {
		invalid := read(extension)
		require.True(t, invalid.HeadersMalformed)
		require.False(t, invalid.Readable)
		require.Equal(t, pool, invalid.Value.CollectorPoolID)
		require.Empty(t, invalid.Value.Headers)
	}
}

func TestOTLPExporterHeaderUpdateRedactsProviderErrors(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Error(w, "collector rejected fixture-label", http.StatusBadRequest)
	})
	service := NewService(&gophercloud.ServiceClient{
		ProviderClient: &gophercloud.ProviderClient{HTTPClient: http.Client{Transport: loadBalancerTestTransport{handler}}},
		Endpoint:       "http://octavia.test/",
	})
	err := service.SetListenerOTLPTracing(t.Context(), "listener", &ListenerOTLPTracing{
		Headers: cloud.TelemetryExporterHeaders{{Name: "x-label", Value: "fixture-label"}},
	}, nil)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "fixture-label")
	category, ok := cloud.ErrorCategoryOf(err)
	require.True(t, ok)
	require.Equal(t, cloud.ErrorCategoryTerminalValidation, category)
}
