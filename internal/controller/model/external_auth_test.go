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

package model

import (
	"strings"
	"testing"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/stretchr/testify/require"
)

func TestListenerFiltersValidateOctaviaHeaderSelection(t *testing.T) {
	require.True(t, cloud.ValidExternalAuthHeaders([]string{strings.Repeat("x", 256), "X-Identity", "Host", ":authority"}, true))
	require.False(t, cloud.ValidExternalAuthHeaders([]string{strings.Repeat("x", 257)}, true))
	require.False(t, cloud.ValidExternalAuthHeaders([]string{"X Identity"}, true))
	require.False(t, cloud.ValidExternalAuthHeaders([]string{"X-K"}, true))
	require.Nil(t, normalizeAuthHeaders(nil, true))
	require.Equal(t, []string{"host"}, normalizeAuthHeaders([]string{":authority", "Host"}, true))
	require.Equal(t, []string{"authority", "host", "x-identity"}, normalizeAuthHeaders([]string{"X-Identity", "Host", "Authority"}, true))
}
