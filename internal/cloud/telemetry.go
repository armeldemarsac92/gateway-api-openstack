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

package cloud

import (
	"bytes"
	"encoding/json"
	"errors"
)

// TelemetrySampling applies Base to new traces. A configured Parent respects
// the incoming sampled bit and applies its rate to sampled parents.
type TelemetrySampling struct {
	Base   TelemetryRate  `json:"base"`
	Parent *TelemetryRate `json:"parent,omitempty"`
}

type TelemetryRate struct {
	Numerator   int32 `json:"numerator"`
	Denominator int32 `json:"denominator"`
}

func (rate TelemetryRate) Valid() bool {
	return rate.Numerator >= 0 && rate.Denominator > 0 && rate.Numerator <= rate.Denominator
}

// An omitted or null numerator must not confirm a zero sampling rate.
func (rate *TelemetryRate) UnmarshalJSON(data []byte) error {
	type fields TelemetryRate
	raw := fields{Numerator: -1}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	if !TelemetryRate(raw).Valid() {
		return errors.New("invalid telemetry sampling fraction")
	}
	*rate = TelemetryRate(raw)
	return nil
}
