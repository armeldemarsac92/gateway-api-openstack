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
	"errors"

	"github.com/google/uuid"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

const requestMirrorsField = "request_mirrors"

// HTTPRequestMirror describes an independent sample at a request processing phase.
type HTTPRequestMirror struct {
	PoolID      string                `json:"pool_id"`
	Numerator   int32                 `json:"numerator"`
	Denominator int32                 `json:"denominator"`
	Phase       cloud.HTTPMirrorPhase `json:"phase"`
}

func (m HTTPRequestMirror) valid() bool {
	return uuid.Validate(m.PoolID) == nil && cloud.ValidHTTPMirrorRatio(m.Numerator, m.Denominator) && m.Phase.Valid()
}

func (m *HTTPRequestMirror) UnmarshalJSON(data []byte) error {
	var value struct {
		PoolID      string                 `json:"pool_id"`
		Numerator   *int32                 `json:"numerator"`
		Denominator *int32                 `json:"denominator"`
		Phase       *cloud.HTTPMirrorPhase `json:"phase"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	if value.Numerator == nil || value.Denominator == nil {
		return errors.New("request mirror requires an explicit numerator and denominator")
	}
	decoded := HTTPRequestMirror{PoolID: value.PoolID, Numerator: *value.Numerator, Denominator: *value.Denominator,
		Phase: cloud.HTTPMirrorAfterRewrite}
	if value.Phase != nil {
		decoded.Phase = *value.Phase
	}
	if !decoded.valid() {
		return errors.New("invalid request mirror destination, ratio or phase")
	}
	*m = decoded
	return nil
}

func validateRequestMirrors(mirrors []HTTPRequestMirror) error {
	for _, mirror := range mirrors {
		if !mirror.valid() {
			return errors.New("invalid request mirror destination, ratio or phase")
		}
	}
	return nil
}

// RequestMirrorsUpdate preserves omission independently from clearing a list.
type RequestMirrorsUpdate struct {
	Value []HTTPRequestMirror
}

func (update RequestMirrorsUpdate) write(body map[string]any) error {
	if err := validateRequestMirrors(update.Value); err != nil {
		return err
	}
	body["l7policy"].(map[string]any)[requestMirrorsField] = update.Value
	return nil
}
