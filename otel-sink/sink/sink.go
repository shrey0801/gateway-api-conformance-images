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

// Package sink implements the OTLP trace receiver behind the otel-sink image.
//
// It accepts OTLP/gRPC (TraceService/Export) and OTLP/HTTP (POST /v1/traces,
// protobuf or JSON, optionally gzip-encoded) export requests, flattens every
// span into an api.Span, writes it as one `SPAN {...}` line to the configured
// writer (stdout in the image) and keeps the most recent spans in a bounded
// in-memory ring buffer that is served by the query handler.
package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"mime"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"sigs.k8s.io/gateway-api-conformance-images/otel-sink/api"
)

const (
	// TransportGRPC is the api.Span.Transport value for OTLP/gRPC.
	TransportGRPC = "grpc"
	// TransportHTTP is the api.Span.Transport value for OTLP/HTTP.
	TransportHTTP = "http"

	// DefaultBufferSize is the default number of spans retained in memory.
	DefaultBufferSize = 10000
	// DefaultMaxAttributes is the default cap on the number of attributes
	// kept per map (span attributes, resource attributes, export headers).
	DefaultMaxAttributes = 64
	// DefaultMaxValueLength is the default cap, in bytes, on a single
	// stringified attribute or header value. Longer values are truncated and
	// suffixed with "...".
	DefaultMaxValueLength = 256

	// maxHTTPBodyBytes bounds the (decompressed) size of an OTLP/HTTP export.
	maxHTTPBodyBytes = 32 << 20

	contentTypeProto = "application/x-protobuf"
	contentTypeJSON  = "application/json"

	truncationMarker = "..."
)

// Options configures a Sink. The zero value is usable: output goes to
// io.Discard and the defaults above apply.
type Options struct {
	// Out receives one api.LogPrefix-prefixed JSON line per span.
	Out io.Writer
	// BufferSize is the ring buffer capacity; <= 0 selects DefaultBufferSize.
	BufferSize int
	// MaxAttributes caps attributes per map; <= 0 selects DefaultMaxAttributes.
	MaxAttributes int
	// MaxValueLength caps stringified values; <= 0 selects DefaultMaxValueLength.
	MaxValueLength int
}

// Sink is an OTLP trace receiver. It is safe for concurrent use.
type Sink struct {
	coltracepb.UnimplementedTraceServiceServer

	out            io.Writer
	maxAttributes  int
	maxValueLength int

	mu     sync.Mutex
	ring   []*api.Span
	next   int
	filled bool

	received atomic.Uint64
	exports  atomic.Uint64
}

// New returns a Sink configured by opts.
func New(opts Options) *Sink {
	s := &Sink{
		out:            opts.Out,
		maxAttributes:  opts.MaxAttributes,
		maxValueLength: opts.MaxValueLength,
	}
	if s.out == nil {
		s.out = io.Discard
	}
	if s.maxAttributes <= 0 {
		s.maxAttributes = DefaultMaxAttributes
	}
	if s.maxValueLength <= 0 {
		s.maxValueLength = DefaultMaxValueLength
	}
	size := opts.BufferSize
	if size <= 0 {
		size = DefaultBufferSize
	}
	s.ring = make([]*api.Span, size)
	return s
}

// Export implements the OTLP/gRPC TraceService. It never rejects a request
// that could be decoded; the response always reports full success.
func (s *Sink) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	headers := map[string]string{}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for k, vs := range md {
			headers[strings.ToLower(k)] = strings.Join(vs, ", ")
		}
	}
	s.Record(req.GetResourceSpans(), TransportGRPC, headers)
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// OTLPHTTPHandler returns the handler for the OTLP/HTTP traces endpoint.
// Mount it at /v1/traces.
func (s *Sink) OTLPHTTPHandler() http.Handler {
	return http.HandlerFunc(s.serveOTLPHTTP)
}

func (s *Sink) serveOTLPHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || (contentType != contentTypeProto && contentType != contentTypeJSON) {
		http.Error(w, fmt.Sprintf("unsupported Content-Type %q; use %s or %s", r.Header.Get("Content-Type"), contentTypeProto, contentTypeJSON), http.StatusUnsupportedMediaType)
		return
	}

	body, err := readBody(r)
	if err != nil {
		writeHTTPError(w, contentType, http.StatusBadRequest, err)
		return
	}

	req := &coltracepb.ExportTraceServiceRequest{}
	switch contentType {
	case contentTypeProto:
		err = proto.Unmarshal(body, req)
	case contentTypeJSON:
		err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(body, req)
	}
	if err != nil {
		writeHTTPError(w, contentType, http.StatusBadRequest, fmt.Errorf("decoding %s body: %w", contentType, err))
		return
	}

	headers := make(map[string]string, len(r.Header))
	for k, vs := range r.Header {
		headers[strings.ToLower(k)] = strings.Join(vs, ", ")
	}
	s.Record(req.GetResourceSpans(), TransportHTTP, headers)

	writeHTTPMessage(w, contentType, http.StatusOK, &coltracepb.ExportTraceServiceResponse{})
}

func readBody(r *http.Request) ([]byte, error) {
	var reader io.Reader = r.Body
	switch enc := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))); enc {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, fmt.Errorf("invalid gzip body: %w", err)
		}
		defer gz.Close()
		reader = gz
	default:
		return nil, fmt.Errorf("unsupported Content-Encoding %q", enc)
	}
	body, err := io.ReadAll(io.LimitReader(reader, maxHTTPBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}
	if len(body) > maxHTTPBodyBytes {
		return nil, errors.New("body exceeds 32 MiB")
	}
	return body, nil
}

func writeHTTPError(w http.ResponseWriter, contentType string, code int, err error) {
	writeHTTPMessage(w, contentType, code, status.New(codes.InvalidArgument, err.Error()).Proto())
}

func writeHTTPMessage(w http.ResponseWriter, contentType string, code int, msg proto.Message) {
	var (
		b   []byte
		err error
	)
	if contentType == contentTypeJSON {
		b, err = protojson.Marshal(msg)
	} else {
		b, err = proto.Marshal(msg)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(code)
	_, _ = w.Write(b) //nolint:gosec // b is a serialized protobuf/JSON message served with an explicit Content-Type.
}

// QueryHandler returns the debugging/health handler served on the sink's
// plain HTTP port:
//
//	GET /healthz                  -> 200 "ok"
//	GET /spans[?traceId=&contains=&limit=] -> JSON array of the retained spans, oldest first
//	GET /stats                    -> {"exports":N,"spans":N,"buffered":N}
func (s *Sink) QueryHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]uint64{
			"exports":  s.exports.Load(),
			"spans":    s.received.Load(),
			"buffered": uint64(len(s.Spans())), //nolint:gosec // len is never negative.
		})
	})
	mux.HandleFunc("GET /spans", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		traceID := strings.ToLower(q.Get("traceId"))
		contains := q.Get("contains")
		limit := 0
		if l := q.Get("limit"); l != "" {
			n, err := strconv.Atoi(l)
			if err != nil || n < 0 {
				http.Error(w, "limit must be a non-negative integer", http.StatusBadRequest)
				return
			}
			limit = n
		}
		spans := s.Spans()
		filtered := make([]*api.Span, 0, len(spans))
		for _, sp := range spans {
			if traceID != "" && sp.TraceID != traceID {
				continue
			}
			if contains != "" && !sp.Contains(contains) {
				continue
			}
			filtered = append(filtered, sp)
		}
		if limit > 0 && len(filtered) > limit {
			filtered = filtered[len(filtered)-limit:]
		}
		writeJSON(w, filtered)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", contentTypeJSON)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		log.Printf("otel-sink: writing response: %v", err)
	}
}

// Record flattens every span in resourceSpans, logs it and stores it in the
// ring buffer. It returns the number of spans recorded.
func (s *Sink) Record(resourceSpans []*tracepb.ResourceSpans, transport string, exportHeaders map[string]string) int {
	headers := s.capMap(exportHeaders)
	n := 0
	for _, rs := range resourceSpans {
		resource := s.flattenAttributes(rs.GetResource().GetAttributes())
		for _, ss := range rs.GetScopeSpans() {
			scope := ss.GetScope().GetName()
			for _, span := range ss.GetSpans() {
				s.record(s.flatten(span, resource, scope, transport, headers))
				n++
			}
		}
	}
	s.exports.Add(1)
	s.received.Add(uint64(n)) //nolint:gosec // n is never negative.
	return n
}

func (s *Sink) record(span *api.Span) {
	line, err := api.FormatLogLine(span)
	if err != nil {
		log.Printf("otel-sink: encoding span %s/%s: %v", span.TraceID, span.SpanID, err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ring[s.next] = span
	s.next++
	if s.next == len(s.ring) {
		s.next = 0
		s.filled = true
	}
	// Hold the lock while writing so lines from concurrent exports never
	// interleave.
	_, _ = fmt.Fprintln(s.out, line)
}

// Spans returns a snapshot of the retained spans, oldest first.
func (s *Sink) Spans() []*api.Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.filled {
		return append([]*api.Span(nil), s.ring[:s.next]...)
	}
	out := make([]*api.Span, 0, len(s.ring))
	out = append(out, s.ring[s.next:]...)
	out = append(out, s.ring[:s.next]...)
	return out
}

func (s *Sink) flatten(span *tracepb.Span, resource map[string]string, scope, transport string, headers map[string]string) *api.Span {
	st := "UNSET"
	if span.GetStatus() != nil {
		st = enumName(tracepb.Status_StatusCode_name, int32(span.GetStatus().GetCode()), "STATUS_CODE_")
	}
	return &api.Span{
		TraceID:           hex.EncodeToString(span.GetTraceId()),
		SpanID:            hex.EncodeToString(span.GetSpanId()),
		ParentSpanID:      hex.EncodeToString(span.GetParentSpanId()),
		Name:              s.truncate(span.GetName()),
		Kind:              enumName(tracepb.Span_SpanKind_name, int32(span.GetKind()), "SPAN_KIND_"),
		StartTimeUnixNano: span.GetStartTimeUnixNano(),
		EndTimeUnixNano:   span.GetEndTimeUnixNano(),
		Status:            st,
		Attributes:        s.flattenAttributes(span.GetAttributes()),
		Resource:          resource,
		Scope:             scope,
		Transport:         transport,
		ExportHeaders:     headers,
	}
}

func enumName(names map[int32]string, value int32, prefix string) string {
	if name, ok := names[value]; ok {
		return strings.TrimPrefix(name, prefix)
	}
	return strconv.Itoa(int(value))
}

// flattenAttributes stringifies kvs into a map. Duplicate keys keep the last
// value; at most maxAttributes keys (in first-seen order) are retained.
func (s *Sink) flattenAttributes(kvs []*commonpb.KeyValue) map[string]string {
	out := make(map[string]string, min(len(kvs), s.maxAttributes))
	for _, kv := range kvs {
		key := kv.GetKey()
		if _, seen := out[key]; !seen && len(out) >= s.maxAttributes {
			continue
		}
		out[key] = s.truncate(Stringify(kv.GetValue()))
	}
	return out
}

// capMap applies the attribute count and value length caps to an already
// flat map, keeping the lexicographically smallest keys for determinism.
func (s *Sink) capMap(in map[string]string) map[string]string {
	out := make(map[string]string, min(len(in), s.maxAttributes))
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if len(out) >= s.maxAttributes {
			break
		}
		out[k] = s.truncate(in[k])
	}
	return out
}

func (s *Sink) truncate(v string) string {
	if len(v) <= s.maxValueLength {
		return v
	}
	cut := s.maxValueLength
	// Do not split a multi-byte rune.
	for cut > 0 && !isRuneStart(v[cut]) {
		cut--
	}
	return v[:cut] + truncationMarker
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }

// Stringify renders an OTLP AnyValue as a string:
//
//   - string values are returned verbatim;
//   - bytes values are base64 (standard encoding);
//   - bool, int and double values are their canonical decimal text
//     ("true", "42", "1.5");
//   - array and kvlist values are compact JSON built from the same rules
//     applied recursively, except that nested strings/bytes are JSON strings
//     and nested bool/int/double keep their JSON types
//     (`["a",1,true]`, `{"k":"v"}`);
//   - an unset value is "".
func Stringify(v *commonpb.AnyValue) string {
	switch x := v.GetValue().(type) {
	case nil:
		return ""
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BytesValue:
		return base64.StdEncoding.EncodeToString(x.BytesValue)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	default:
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(toGo(v)); err != nil {
			return fmt.Sprintf("%v", v)
		}
		return strings.TrimSuffix(buf.String(), "\n")
	}
}

// toGo converts an AnyValue into plain Go values suitable for encoding/json.
func toGo(v *commonpb.AnyValue) any {
	switch x := v.GetValue().(type) {
	case nil:
		return nil
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BytesValue:
		return base64.StdEncoding.EncodeToString(x.BytesValue)
	case *commonpb.AnyValue_BoolValue:
		return x.BoolValue
	case *commonpb.AnyValue_IntValue:
		return x.IntValue
	case *commonpb.AnyValue_DoubleValue:
		// NaN/Inf are not representable in JSON; fall back to text.
		if f := x.DoubleValue; math.IsNaN(f) || math.IsInf(f, 0) {
			return strconv.FormatFloat(f, 'g', -1, 64)
		}
		return x.DoubleValue
	case *commonpb.AnyValue_ArrayValue:
		vals := x.ArrayValue.GetValues()
		out := make([]any, 0, len(vals))
		for _, e := range vals {
			out = append(out, toGo(e))
		}
		return out
	case *commonpb.AnyValue_KvlistValue:
		kvs := x.KvlistValue.GetValues()
		out := make(map[string]any, len(kvs))
		for _, kv := range kvs {
			out[kv.GetKey()] = toGo(kv.GetValue())
		}
		return out
	default:
		return fmt.Sprintf("%v", v)
	}
}
