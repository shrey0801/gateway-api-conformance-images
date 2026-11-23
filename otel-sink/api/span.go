/*
Copyright The Kubernetes Authors.

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

// Package api defines the stable, line-oriented log format emitted by the
// otel-sink image. Conformance tests read the sink's pod logs, select the
// lines starting with LogPrefix and decode the remainder of each line with
// ParseLogLine.
//
// The format is intentionally flat: every OTLP AnyValue is rendered as a
// string, ids are lowercase hex, and the resource, instrumentation scope and
// export request headers are inlined into every span so that a single line is
// self-describing.
package api

import (
	"encoding/json"
	"strings"
)

// LogPrefix is written in front of every JSON-encoded Span on stdout.
// It is followed by exactly one JSON object and a newline.
const LogPrefix = "SPAN "

// Span is the sink's flattened representation of a single OTLP span.
//
// This struct is the wire contract of the log line. Fields are only ever
// added, never renamed or removed.
type Span struct {
	// TraceID is the lowercase hex encoding of the 16-byte OTLP trace id.
	TraceID string `json:"traceId"`
	// SpanID is the lowercase hex encoding of the 8-byte OTLP span id.
	SpanID string `json:"spanId"`
	// ParentSpanID is the lowercase hex encoding of the parent span id, or
	// "" for a root span.
	ParentSpanID string `json:"parentSpanId"`
	// Name is the span name.
	Name string `json:"name"`
	// Kind is the OTLP SpanKind without the SPAN_KIND_ prefix, i.e. one of
	// UNSPECIFIED, INTERNAL, SERVER, CLIENT, PRODUCER or CONSUMER.
	Kind string `json:"kind"`
	// StartTimeUnixNano is the span start time in nanoseconds since the Unix
	// epoch.
	StartTimeUnixNano uint64 `json:"startTimeUnixNano"`
	// EndTimeUnixNano is the span end time in nanoseconds since the Unix epoch.
	EndTimeUnixNano uint64 `json:"endTimeUnixNano"`
	// Status is the OTLP status code without the STATUS_CODE_ prefix, i.e.
	// one of UNSET, OK or ERROR.
	Status string `json:"status"`
	// Attributes are the span attributes with every value stringified
	// (see the package documentation for the exact rules).
	Attributes map[string]string `json:"attributes"`
	// Resource are the resource attributes of the ResourceSpans this span
	// belonged to, stringified like Attributes.
	Resource map[string]string `json:"resource"`
	// Scope is the name of the instrumentation scope, or "".
	Scope string `json:"scope"`
	// Transport is the transport the span was exported over: "grpc" for
	// OTLP/gRPC and "http" for OTLP/HTTP.
	Transport string `json:"transport"`
	// ExportHeaders are the gRPC metadata (OTLP/gRPC) or HTTP request
	// headers (OTLP/HTTP) of the export request that carried this span.
	// Keys are lowercase; multiple values are joined with ", ". Nothing is
	// redacted: the sink is a test fixture, not a production collector.
	ExportHeaders map[string]string `json:"exportHeaders"`
}

// Attribute returns the value of the span attribute key and whether it was
// present.
func (s *Span) Attribute(key string) (string, bool) {
	if s == nil || s.Attributes == nil {
		return "", false
	}
	v, ok := s.Attributes[key]
	return v, ok
}

// ResourceAttribute returns the value of the resource attribute key and
// whether it was present.
func (s *Span) ResourceAttribute(key string) (string, bool) {
	if s == nil || s.Resource == nil {
		return "", false
	}
	v, ok := s.Resource[key]
	return v, ok
}

// ExportHeader returns the value of the (case-insensitive) export request
// header key and whether it was present.
func (s *Span) ExportHeader(key string) (string, bool) {
	if s == nil || s.ExportHeaders == nil {
		return "", false
	}
	v, ok := s.ExportHeaders[strings.ToLower(key)]
	return v, ok
}

// Contains reports whether needle is a substring of the span name or of any
// span attribute value. It is the loosest correlation available to a test
// that can only plant a token in the request path.
func (s *Span) Contains(needle string) bool {
	if s == nil || needle == "" {
		return false
	}
	if strings.Contains(s.Name, needle) {
		return true
	}
	for _, v := range s.Attributes {
		if strings.Contains(v, needle) {
			return true
		}
	}
	return false
}

// FormatLogLine renders s as the sink does: LogPrefix followed by the compact
// JSON encoding of s. The returned string has no trailing newline.
func FormatLogLine(s *Span) (string, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return LogPrefix + string(b), nil
}

// ParseLogLine decodes a single log line. It returns false when the line does
// not start with LogPrefix (after trimming surrounding whitespace, so that
// lines read back through `kubectl logs --timestamps` or similar still match
// once the timestamp is stripped) or when the JSON payload is malformed.
func ParseLogLine(line string) (*Span, bool) {
	line = strings.TrimSpace(line)
	payload, ok := strings.CutPrefix(line, LogPrefix)
	if !ok {
		return nil, false
	}
	s := &Span{}
	if err := json.Unmarshal([]byte(payload), s); err != nil {
		return nil, false
	}
	return s, true
}

// ParseLogLines decodes every line of logs that ParseLogLine accepts, in
// order, skipping all other lines.
func ParseLogLines(logs string) []*Span {
	var spans []*Span
	for line := range strings.SplitSeq(logs, "\n") {
		if s, ok := ParseLogLine(line); ok {
			spans = append(spans, s)
		}
	}
	return spans
}
