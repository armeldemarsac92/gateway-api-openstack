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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListenerRequestFiltersWireOrderAndClearing(t *testing.T) {
	body, err := (listenerRequestFilterUpdate{{
		Hostname: "example.test",
		Requests: []ListenerRequestFilter{
			{Type: "ExternalAuth", HTTP: &ListenerHTTPExternalAuth{PoolID: "two", AllowedResponseHeaders: []string{"x-identity"}}},
			{Type: "ExternalAuth", HTTP: &ListenerHTTPExternalAuth{PoolID: "one", PathPrefix: "/check", ForwardBodyMaxSize: 4096}},
		},
	}}).ToListenerUpdateMap()
	require.NoError(t, err)
	wire, err := json.Marshal(body)
	require.NoError(t, err)
	require.JSONEq(t, `{"listener":{"request_filter_sections":[{"hostname":"example.test","requests":[{"type":"ExternalAuth","http":{"pool_id":"two","allowed_response_headers":["x-identity"]}},{"type":"ExternalAuth","http":{"pool_id":"one","path_prefix":"/check","forward_body_max_size":4096}}]}]}}`, string(wire))
	observation := decodeListenerRequestFilters(json.RawMessage(`[{"hostname":"example.test","requests":[{"type":"ExternalAuth","http":{"pool_id":"one","forward_body_max_size":4096}}]}]`))
	require.True(t, observation.Readable)
	require.EqualValues(t, 4096, observation.Sections[0].Requests[0].HTTP.ForwardBodyMaxSize)
	var clear listenerRequestFilterUpdate
	body, err = clear.ToListenerUpdateMap()
	require.NoError(t, err)
	wire, err = json.Marshal(body)
	require.NoError(t, err)
	require.JSONEq(t, `{"listener":{"request_filter_sections":[]}}`, string(wire))
	require.True(t, decodeListenerRequestFilters(json.RawMessage(`[]`)).Readable)
	require.False(t, decodeListenerRequestFilters(json.RawMessage(`null`)).Readable)
}

func TestListenerGRPCExternalAuthWireUnion(t *testing.T) {
	const expected = `[{"hostname":"auth.example.test","requests":[{"type":"ExternalAuth","grpc":{"pool_id":"authorizer","allowed_request_headers":[":authority"],"forward_body_max_size":1024}},{"type":"ExternalAuth","http":{"pool_id":"http-authorizer"}}]}]`
	observation := decodeListenerRequestFilters(json.RawMessage(expected))
	require.True(t, observation.Readable)
	require.Len(t, observation.Sections[0].Requests, 2)
	grpc := observation.Sections[0].Requests[0]
	require.Nil(t, grpc.HTTP)
	require.Equal(t, &ListenerGRPCExternalAuth{PoolID: "authorizer", AllowedRequestHeaders: []string{":authority"}, ForwardBodyMaxSize: 1024}, grpc.GRPC)
	wire, err := json.Marshal(observation.Sections)
	require.NoError(t, err)
	require.JSONEq(t, expected, string(wire))
	for _, invalid := range []string{
		`{"type":"ExternalAuth","http":{"pool_id":"http"},"grpc":{"pool_id":"grpc"}}`,
		`{"type":"ExternalAuth","grpc":{"pool_id":"grpc","forward_body_max_size":2147483647}}`,
		`{"type":"ExternalAuth","grpc":{"pool_id":"grpc","allowed_response_headers":["x-user"]}}`,
	} {
		require.False(t, decodeListenerRequestFilters(json.RawMessage(`[{"hostname":"*","requests":[`+invalid+`]}]`)).Readable)
	}
}
