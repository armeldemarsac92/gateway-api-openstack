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
