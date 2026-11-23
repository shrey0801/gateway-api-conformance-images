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

package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"sigs.k8s.io/gateway-api-conformance-images/otel-sink/api"
)

const (
	testTraceID       = "4bf92f3577b34da6a3ce929d0e0e4736"
	testSpanID        = "00f067aa0ba902b7"
	testCorrelationID = "6f1c2a3b-0000-4000-8000-000000000001"
)

// syncBuffer is a goroutine-safe bytes.Buffer.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func str(s string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
}

func kv(k string, v *commonpb.AnyValue) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: v}
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b := make([]byte, len(s)/2)
	_, err := fmt.Sscanf(s, "%x", &b)
	require.NoError(t, err)
	return b
}

func exportRequest(t *testing.T) *coltracepb.ExportTraceServiceRequest {
	t.Helper()
	return &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				kv("service.name", str("gateway")),
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Scope: &commonpb.InstrumentationScope{Name: "envoy"},
				Spans: []*tracepb.Span{{
					TraceId:           mustHex(t, testTraceID),
					SpanId:            mustHex(t, testSpanID),
					Name:              "ingress",
					Kind:              tracepb.Span_SPAN_KIND_SERVER,
					StartTimeUnixNano: 1700000000000000000,
					EndTimeUnixNano:   1700000000001000000,
					Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
					Attributes: []*commonpb.KeyValue{
						kv("http.request.method", str("GET")),
						kv("gateway_api_conformance.token", str(testCorrelationID)),
						kv("http.response.status_code", &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 200}}),
						kv("sampled", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}),
					},
				}},
			}},
		}},
	}
}

func assertExampleSpan(t *testing.T, s *api.Span, transport string) {
	t.Helper()
	assert.Equal(t, testTraceID, s.TraceID)
	assert.Equal(t, testSpanID, s.SpanID)
	assert.Empty(t, s.ParentSpanID)
	assert.Equal(t, "ingress", s.Name)
	assert.Equal(t, "SERVER", s.Kind)
	assert.Equal(t, uint64(1700000000000000000), s.StartTimeUnixNano)
	assert.Equal(t, uint64(1700000000001000000), s.EndTimeUnixNano)
	assert.Equal(t, "OK", s.Status)
	assert.Equal(t, map[string]string{
		"http.request.method":           "GET",
		"gateway_api_conformance.token": testCorrelationID,
		"http.response.status_code":     "200",
		"sampled":                       "true",
	}, s.Attributes)
	assert.Equal(t, map[string]string{"service.name": "gateway"}, s.Resource)
	assert.Equal(t, "envoy", s.Scope)
	assert.Equal(t, transport, s.Transport)
}

// spanLines returns the parsed SPAN lines currently in out.
func spanLines(out *syncBuffer) []*api.Span {
	return api.ParseLogLines(out.String())
}

func TestOTLPHTTPProtobuf(t *testing.T) {
	out := &syncBuffer{}
	s := New(Options{Out: out})
	srv := httptest.NewServer(s.OTLPHTTPHandler())
	defer srv.Close()

	body, err := proto.Marshal(exportRequest(t))
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/v1/traces", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Add("X-Multi", "a")
	req.Header.Add("X-Multi", "b")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/x-protobuf", resp.Header.Get("Content-Type"))
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, proto.Unmarshal(respBody, &coltracepb.ExportTraceServiceResponse{}))

	// Exactly one line, prefixed, no embedded newlines.
	logged := out.String()
	require.Equal(t, 1, strings.Count(logged, "\n"))
	require.True(t, strings.HasPrefix(logged, api.LogPrefix), logged)

	spans := spanLines(out)
	require.Len(t, spans, 1)
	assertExampleSpan(t, spans[0], TransportHTTP)
	assert.Equal(t, "Bearer secret", spans[0].ExportHeaders["authorization"])
	assert.Equal(t, "a, b", spans[0].ExportHeaders["x-multi"])
	assert.Equal(t, "application/x-protobuf", spans[0].ExportHeaders["content-type"])

	// The ring buffer holds the same span.
	buffered := s.Spans()
	require.Len(t, buffered, 1)
	assert.Equal(t, spans[0], buffered[0])
}

func TestOTLPHTTPJSONGzip(t *testing.T) {
	out := &syncBuffer{}
	s := New(Options{Out: out})
	srv := httptest.NewServer(s.OTLPHTTPHandler())
	defer srv.Close()

	jsonBody, err := protojson.Marshal(exportRequest(t))
	require.NoError(t, err)
	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	_, err = w.Write(jsonBody)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/v1/traces", &gz)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.JSONEq(t, "{}", string(respBody))

	spans := spanLines(out)
	require.Len(t, spans, 1)
	assertExampleSpan(t, spans[0], TransportHTTP)
	assert.Equal(t, "gzip", spans[0].ExportHeaders["content-encoding"])
}

func TestOTLPHTTPErrors(t *testing.T) {
	s := New(Options{})
	srv := httptest.NewServer(s.OTLPHTTPHandler())
	defer srv.Close()

	do := func(method, contentType, encoding string, body []byte) int {
		req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+"/v1/traces", bytes.NewReader(body))
		require.NoError(t, err)
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		if encoding != "" {
			req.Header.Set("Content-Encoding", encoding)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		return resp.StatusCode
	}

	assert.Equal(t, http.StatusMethodNotAllowed, do(http.MethodGet, "application/x-protobuf", "", nil))
	assert.Equal(t, http.StatusUnsupportedMediaType, do(http.MethodPost, "text/plain", "", []byte("{}")))
	assert.Equal(t, http.StatusUnsupportedMediaType, do(http.MethodPost, "", "", []byte("{}")))
	assert.Equal(t, http.StatusBadRequest, do(http.MethodPost, "application/json", "", []byte("{not json")))
	assert.Equal(t, http.StatusBadRequest, do(http.MethodPost, "application/x-protobuf", "", []byte{0xff, 0xff}))
	assert.Equal(t, http.StatusBadRequest, do(http.MethodPost, "application/json", "br", []byte("{}")))
	assert.Equal(t, http.StatusBadRequest, do(http.MethodPost, "application/json", "gzip", []byte("not gzip")))
	// An empty export is valid and logs nothing.
	assert.Equal(t, http.StatusOK, do(http.MethodPost, "application/json", "", []byte("{}")))
	assert.Empty(t, s.Spans())
}

func TestOTLPGRPC(t *testing.T) {
	out := &syncBuffer{}
	s := New(Options{Out: out})

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(grpcServer, s)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer secret", "x-multi", "a", "x-multi", "b")
	resp, err := coltracepb.NewTraceServiceClient(conn).Export(ctx, exportRequest(t))
	require.NoError(t, err)
	assert.Nil(t, resp.GetPartialSuccess())

	spans := spanLines(out)
	require.Len(t, spans, 1)
	assertExampleSpan(t, spans[0], TransportGRPC)
	assert.Equal(t, "Bearer secret", spans[0].ExportHeaders["authorization"])
	assert.Equal(t, "a, b", spans[0].ExportHeaders["x-multi"])
	assert.Equal(t, "application/grpc", spans[0].ExportHeaders["content-type"])
}

func TestQueryHandler(t *testing.T) {
	out := &syncBuffer{}
	s := New(Options{Out: out, BufferSize: 3})
	srv := httptest.NewServer(s.QueryHandler())
	defer srv.Close()

	get := func(path string) (int, string) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+path, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(b)
	}

	code, body := get("/healthz")
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "ok\n", body)

	code, body = get("/spans")
	assert.Equal(t, http.StatusOK, code)
	assert.JSONEq(t, "[]", body)

	// Fill past the buffer size: 5 spans into a 3-slot ring.
	for i := range 5 {
		req := exportRequest(t)
		span := req.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0]
		span.Name = fmt.Sprintf("span-%d", i)
		if i == 4 {
			span.TraceId = mustHex(t, "0123456789abcdef0123456789abcdef")
		}
		s.Record(req.GetResourceSpans(), TransportHTTP, nil)
	}
	// Everything was logged, regardless of the buffer.
	assert.Len(t, spanLines(out), 5)

	code, body = get("/spans")
	assert.Equal(t, http.StatusOK, code)
	var spans []*api.Span
	require.NoError(t, json.Unmarshal([]byte(body), &spans))
	require.Len(t, spans, 3)
	assert.Equal(t, []string{"span-2", "span-3", "span-4"}, []string{spans[0].Name, spans[1].Name, spans[2].Name})

	_, body = get("/spans?limit=1")
	require.NoError(t, json.Unmarshal([]byte(body), &spans))
	require.Len(t, spans, 1)
	assert.Equal(t, "span-4", spans[0].Name)

	_, body = get("/spans?traceId=" + testTraceID)
	require.NoError(t, json.Unmarshal([]byte(body), &spans))
	require.Len(t, spans, 2)

	_, body = get("/spans?contains=span-3")
	require.NoError(t, json.Unmarshal([]byte(body), &spans))
	require.Len(t, spans, 1)

	code, _ = get("/spans?limit=x")
	assert.Equal(t, http.StatusBadRequest, code)

	code, body = get("/stats")
	assert.Equal(t, http.StatusOK, code)
	assert.JSONEq(t, `{"exports":5,"spans":5,"buffered":3}`, body)

	code, _ = get("/nope")
	assert.Equal(t, http.StatusNotFound, code)
}

func TestStringify(t *testing.T) {
	arr := &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{
		str("a"),
		{Value: &commonpb.AnyValue_IntValue{IntValue: 1}},
		{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}},
	}}}}
	kvl := &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: []*commonpb.KeyValue{
		kv("k", str("v")),
		kv("n", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 1.5}}),
	}}}}
	for name, tc := range map[string]struct {
		in   *commonpb.AnyValue
		want string
	}{
		"nil":    {nil, ""},
		"empty":  {&commonpb.AnyValue{}, ""},
		"string": {str("<a&b>"), "<a&b>"},
		"bytes":  {&commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{0, 1, 2}}}, "AAEC"},
		"bool":   {&commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: false}}, "false"},
		"int":    {&commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: -42}}, "-42"},
		"double": {&commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 0.25}}, "0.25"},
		"array":  {arr, `["a",1,true]`},
		"kvlist": {kvl, `{"k":"v","n":1.5}`},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, Stringify(tc.in))
		})
	}
}

func TestCaps(t *testing.T) {
	out := &syncBuffer{}
	s := New(Options{Out: out, MaxAttributes: 2, MaxValueLength: 8})

	req := exportRequest(t)
	span := req.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0]
	span.Attributes = []*commonpb.KeyValue{
		kv("a", str("0123456789")),
		kv("b", str("héllo wörld")),
		kv("c", str("dropped")),
		kv("a", str("dup")), // duplicate keys keep the last value and do not count twice
	}
	headers := map[string]string{"z": "1", "y": "2", "x": "3"}
	s.Record(req.GetResourceSpans(), TransportHTTP, headers)

	spans := spanLines(out)
	require.Len(t, spans, 1)
	assert.Equal(t, map[string]string{"a": "dup", "b": "héllo w..."}, spans[0].Attributes)
	assert.Equal(t, map[string]string{"x": "3", "y": "2"}, spans[0].ExportHeaders)
}

func TestNoParentNoStatus(t *testing.T) {
	out := &syncBuffer{}
	s := New(Options{Out: out})
	req := exportRequest(t)
	span := req.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0]
	span.Status = nil
	span.ParentSpanId = mustHex(t, "0102030405060708")
	span.Kind = tracepb.Span_SpanKind(99)
	req.GetResourceSpans()[0].ScopeSpans[0].Scope = nil
	s.Record(req.GetResourceSpans(), TransportGRPC, nil)

	spans := spanLines(out)
	require.Len(t, spans, 1)
	assert.Equal(t, "UNSET", spans[0].Status)
	assert.Equal(t, "0102030405060708", spans[0].ParentSpanID)
	assert.Equal(t, "99", spans[0].Kind)
	assert.Empty(t, spans[0].Scope)
	assert.Empty(t, spans[0].ExportHeaders)
	assert.NotNil(t, spans[0].ExportHeaders)
}

func TestConcurrentExportsDoNotInterleave(t *testing.T) {
	out := &syncBuffer{}
	s := New(Options{Out: out})
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			req := exportRequest(t)
			req.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0].Name = strings.Repeat(fmt.Sprintf("%d", i%10), 100)
			s.Record(req.GetResourceSpans(), TransportGRPC, nil)
		})
	}
	wg.Wait()
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	require.Len(t, lines, 50)
	for _, l := range lines {
		_, ok := api.ParseLogLine(l)
		assert.True(t, ok, l)
	}
}
