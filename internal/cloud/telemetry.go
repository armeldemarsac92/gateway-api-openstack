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
	"crypto/sha256"
	hexencoding "encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"golang.org/x/net/http/httpguts"
)

// TelemetryExporterHeader is written only to the collector's OTLP requests.
type TelemetryExporterHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type TelemetryExporterHeaders []TelemetryExporterHeader

var exporterHeaderValue = regexp.MustCompile(`^[!-~]+([ \t][!-~]+)*$`)

// CanonicalTelemetryExporterHeaders validates this Gateway API list-map and
// orders it by case-insensitive field name before the provider sees it.
func CanonicalTelemetryExporterHeaders(input TelemetryExporterHeaders) (TelemetryExporterHeaders, bool) {
	if len(input) == 0 {
		return nil, true
	}
	if len(input) > 16 {
		return nil, false
	}
	headers := make(TelemetryExporterHeaders, 0, len(input))
	seen := make(map[string]bool, len(input))
	for _, h := range input {
		if !httpHeaderName.MatchString(h.Name) {
			return nil, false
		}
		h.Name = strings.ToLower(h.Name)
		if seen[h.Name] {
			continue
		}
		if len(h.Value) > 4096 || !exporterHeaderValue.MatchString(h.Value) {
			return nil, false
		}
		switch h.Name {
		case "host", "connection", "content-length", "content-type", "content-encoding", "transfer-encoding",
			"te", "trailer", "upgrade", "proxy-connection", "keep-alive", "expect":
			return nil, false
		}
		seen[h.Name] = true
		headers = append(headers, h)
	}
	slices.SortFunc(headers, func(a, b TelemetryExporterHeader) int { return cmp.Compare(a.Name, b.Name) })
	return headers, true
}

// Digest matches Octavia's public readback for a canonical header set. NUL
// separates fields and values and is excluded from both by the schema.
func (headers TelemetryExporterHeaders) Digest() string {
	if len(headers) == 0 {
		return ""
	}
	hash := sha256.New()
	for _, header := range headers {
		hash.Write([]byte(header.Name + "\x00" + header.Value + "\x00"))
	}
	return hexencoding.EncodeToString(hash.Sum(nil))
}

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
