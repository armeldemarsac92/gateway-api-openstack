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
	"cmp"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"golang.org/x/net/http/httpguts"
)

type TelemetryAttribute struct {
	Name         string `json:"name"`
	SourceType   string `json:"source_type"`
	LiteralValue string `json:"literal_value,omitempty"`
	HeaderName   string `json:"header_name,omitempty"`
	AttributeKey string `json:"attribute_key,omitempty"`
}

var telemetryAttributeName = regexp.MustCompile(`^[a-zA-Z0-9_.:/-]+$`)

// CanonicalTelemetryAttributes orders the Gateway API map by output name.
// Only header names are case-insensitive. Literal values are preserved as given.
func CanonicalTelemetryAttributes(attrs []TelemetryAttribute) ([]TelemetryAttribute, bool) {
	if len(attrs) == 0 {
		return nil, true
	}
	ordered := slices.Clone(attrs)
	slices.SortFunc(ordered, func(a, b TelemetryAttribute) int { return cmp.Compare(a.Name, b.Name) })
	for i := range ordered {
		a := &ordered[i]
		if len(a.Name) > 256 || !telemetryAttributeName.MatchString(a.Name) || i > 0 && ordered[i-1].Name == a.Name {
			return nil, false
		}
		if a.Name == "http.response.status_code" && (a.SourceType != "Attribute" || a.AttributeKey != a.Name) {
			return nil, false
		}
		switch a.SourceType {
		case "Literal":
			if a.LiteralValue == "" || !utf8.ValidString(a.LiteralValue) || utf8.RuneCountInString(a.LiteralValue) > 1024 || a.HeaderName != "" || a.AttributeKey != "" {
				return nil, false
			}
		case "Header":
			if len(a.HeaderName) > 256 || !httpguts.ValidHeaderFieldName(a.HeaderName) || a.LiteralValue != "" || a.AttributeKey != "" {
				return nil, false
			}
			a.HeaderName = strings.ToLower(a.HeaderName)
		case "Attribute":
			if a.LiteralValue != "" || a.HeaderName != "" {
				return nil, false
			}
			switch a.AttributeKey {
			case "http.request.method", "http.response.status_code", "url.path", "url.scheme":
			default:
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return ordered, true
}

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
