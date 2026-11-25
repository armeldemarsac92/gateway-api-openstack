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
			{Type: "ExternalAuth", HTTP: ListenerHTTPExternalAuth{PoolID: "two", AllowedResponseHeaders: []string{"x-identity"}}},
			{Type: "ExternalAuth", HTTP: ListenerHTTPExternalAuth{PoolID: "one", PathPrefix: "/check"}},
		},
	}}).ToListenerUpdateMap()
	require.NoError(t, err)
	wire, err := json.Marshal(body)
	require.NoError(t, err)
	require.JSONEq(t, `{"listener":{"request_filter_sections":[{"hostname":"example.test","requests":[{"type":"ExternalAuth","http":{"pool_id":"two","allowed_response_headers":["x-identity"]}},{"type":"ExternalAuth","http":{"pool_id":"one","path_prefix":"/check"}}]}]}}`, string(wire))
	var clear listenerRequestFilterUpdate
	body, err = clear.ToListenerUpdateMap()
	require.NoError(t, err)
	wire, err = json.Marshal(body)
	require.NoError(t, err)
	require.JSONEq(t, `{"listener":{"request_filter_sections":[]}}`, string(wire))
	require.True(t, decodeListenerRequestFilters(json.RawMessage(`[]`)).Readable)
	require.False(t, decodeListenerRequestFilters(json.RawMessage(`null`)).Readable)
}
