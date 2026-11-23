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

package api

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const exampleLine = `SPAN {"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba902b7","parentSpanId":"","name":"ingress","kind":"SERVER","startTimeUnixNano":1700000000000000000,"endTimeUnixNano":1700000000001000000,"status":"UNSET","attributes":{"gateway_api_conformance.token":"6f1c2a3b-0000-4000-8000-000000000001","http.request.method":"GET","url.path":"/telemetrypolicy/6f1c2a3b-0000-4000-8000-000000000001"},"resource":{"service.name":"gateway"},"scope":"envoy","transport":"grpc","exportHeaders":{"authorization":"Bearer abc","content-type":"application/grpc"}}`

func TestParseLogLine(t *testing.T) {
	s, ok := ParseLogLine(exampleLine)
	require.True(t, ok)
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", s.TraceID)
	assert.Equal(t, "00f067aa0ba902b7", s.SpanID)
	assert.Empty(t, s.ParentSpanID)
	assert.Equal(t, "ingress", s.Name)
	assert.Equal(t, "SERVER", s.Kind)
	assert.Equal(t, uint64(1700000000000000000), s.StartTimeUnixNano)
	assert.Equal(t, uint64(1700000000001000000), s.EndTimeUnixNano)
	assert.Equal(t, "UNSET", s.Status)
	assert.Equal(t, "envoy", s.Scope)
	assert.Equal(t, "grpc", s.Transport)
	assert.Equal(t, map[string]string{"service.name": "gateway"}, s.Resource)

	v, ok := s.Attribute("gateway_api_conformance.token")
	assert.True(t, ok)
	assert.Equal(t, "6f1c2a3b-0000-4000-8000-000000000001", v)
	_, ok = s.Attribute("missing")
	assert.False(t, ok)

	v, ok = s.ResourceAttribute("service.name")
	assert.True(t, ok)
	assert.Equal(t, "gateway", v)

	v, ok = s.ExportHeader("Authorization")
	assert.True(t, ok)
	assert.Equal(t, "Bearer abc", v)
	_, ok = s.ExportHeader("x-missing")
	assert.False(t, ok)

	assert.True(t, s.Contains("6f1c2a3b-0000-4000-8000-000000000001"))
	assert.True(t, s.Contains("ingress"))
	assert.False(t, s.Contains("nope"))
	assert.False(t, s.Contains(""))
}

func TestParseLogLineTolerance(t *testing.T) {
	// `kubectl logs --timestamps` prefixes every line with an RFC3339Nano
	// timestamp and a space; callers strip that before parsing.
	_, timestamped, found := strings.Cut("2026-01-01T00:00:00.000000000Z "+exampleLine, " ")
	require.True(t, found)
	for name, line := range map[string]string{
		"leading whitespace":  "  " + exampleLine,
		"trailing newline":    exampleLine + "\n",
		"trailing CRLF":       exampleLine + "\r\n",
		"timestamp stripped":  timestamped,
		"unknown extra field": strings.Replace(exampleLine, `"scope":"envoy",`, `"scope":"envoy","futureField":1,`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			s, ok := ParseLogLine(line)
			require.True(t, ok)
			assert.Equal(t, "ingress", s.Name)
		})
	}
}

func TestParseLogLineRejects(t *testing.T) {
	for name, line := range map[string]string{
		"empty":            "",
		"unrelated":        "listening on :4317",
		"prefix only":      "SPAN ",
		"no space":         "SPAN{}",
		"lowercase prefix": "span {}",
		"bad json":         `SPAN {"traceId":`,
		"wrong type":       `SPAN {"startTimeUnixNano":"notanumber"}`,
		"json array":       `SPAN []`,
	} {
		t.Run(name, func(t *testing.T) {
			s, ok := ParseLogLine(line)
			assert.False(t, ok)
			assert.Nil(t, s)
		})
	}
}

func TestParseLogLines(t *testing.T) {
	logs := "starting otel-sink\n" + exampleLine + "\nnoise\n" + strings.Replace(exampleLine, `"name":"ingress"`, `"name":"egress"`, 1) + "\n"
	spans := ParseLogLines(logs)
	require.Len(t, spans, 2)
	assert.Equal(t, "ingress", spans[0].Name)
	assert.Equal(t, "egress", spans[1].Name)
	assert.Empty(t, ParseLogLines(""))
}

func TestFormatLogLineRoundTrip(t *testing.T) {
	in, ok := ParseLogLine(exampleLine)
	require.True(t, ok)
	line, err := FormatLogLine(in)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(line, LogPrefix))
	assert.NotContains(t, line, "\n")
	out, ok := ParseLogLine(line)
	require.True(t, ok)
	assert.Equal(t, in, out)
}

func TestNilSpan(t *testing.T) {
	var s *Span
	_, ok := s.Attribute("k")
	assert.False(t, ok)
	_, ok = s.ResourceAttribute("k")
	assert.False(t, ok)
	_, ok = s.ExportHeader("k")
	assert.False(t, ok)
	assert.False(t, s.Contains("k"))
}
