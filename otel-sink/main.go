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

// Command otel-sink is a minimal OTLP trace receiver for Gateway API
// conformance tests. Every received span is written to stdout as one
// `SPAN {...}` JSON line (see the api package for the schema) so that a test
// can correlate requests it sent through a Gateway with the spans the
// implementation exported, using nothing but pod logs.
//
// Listeners (all plaintext):
//
//	OTLP_GRPC_PORT (default 4317): OTLP/gRPC TraceService/Export
//	OTLP_HTTP_PORT (default 4318): OTLP/HTTP POST /v1/traces (protobuf or JSON, optional gzip)
//	HTTP_PORT      (default 8080): GET /healthz, GET /spans, GET /stats
//
// OTEL_SINK_BUFFER_SIZE (default 10000) bounds the in-memory span buffer
// served by /spans; it never affects what is logged.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // Enable gzip decompression of OTLP/gRPC requests.

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"sigs.k8s.io/gateway-api-conformance-images/otel-sink/sink"
)

const (
	defaultOTLPGRPCPort = "4317"
	defaultOTLPHTTPPort = "4318"
	defaultHTTPPort     = "8080"

	maxGRPCMessageBytes = 32 << 20
	shutdownTimeout     = 5 * time.Second
)

func main() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	bufferSize := sink.DefaultBufferSize
	if v := os.Getenv("OTEL_SINK_BUFFER_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			log.Fatal("otel-sink: OTEL_SINK_BUFFER_SIZE must be a positive integer")
		}
		bufferSize = n
	}

	s := sink.New(sink.Options{Out: os.Stdout, BufferSize: bufferSize})

	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(maxGRPCMessageBytes))
	coltracepb.RegisterTraceServiceServer(grpcServer, s)

	otlpMux := http.NewServeMux()
	otlpMux.Handle("/v1/traces", s.OTLPHTTPHandler())
	otlpHTTPServer := newHTTPServer(otlpMux)
	queryServer := newHTTPServer(s.QueryHandler())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	grpcListener := listen(ctx, envOr("OTLP_GRPC_PORT", defaultOTLPGRPCPort))
	otlpHTTPListener := listen(ctx, envOr("OTLP_HTTP_PORT", defaultOTLPHTTPPort))
	queryListener := listen(ctx, envOr("HTTP_PORT", defaultHTTPPort))

	errCh := make(chan error, 3)
	go func() {
		log.Printf("otel-sink: OTLP/gRPC listening on %s", grpcListener.Addr())
		errCh <- grpcServer.Serve(grpcListener)
	}()
	go func() {
		log.Printf("otel-sink: OTLP/HTTP listening on %s (POST /v1/traces)", otlpHTTPListener.Addr())
		errCh <- otlpHTTPServer.Serve(otlpHTTPListener)
	}()
	go func() {
		log.Printf("otel-sink: query API listening on %s (GET /healthz, /spans, /stats)", queryListener.Addr())
		errCh <- queryServer.Serve(queryListener)
	}()

	select {
	case <-ctx.Done():
		log.Print("otel-sink: shutting down")
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("otel-sink: server failed: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	grpcServer.GracefulStop()
	_ = otlpHTTPServer.Shutdown(shutdownCtx)
	_ = queryServer.Shutdown(shutdownCtx)
}

// newHTTPServer returns a plaintext server speaking HTTP/1.1 and unencrypted
// HTTP/2 (h2c), which OTLP/HTTP exporters may use.
func newHTTPServer(h http.Handler) *http.Server {
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	return &http.Server{
		Handler:           h,
		Protocols:         protocols,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

func listen(ctx context.Context, port string) net.Listener {
	addr := fmt.Sprintf(":%s", port)
	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		log.Fatalf("otel-sink: listening on %s: %v", addr, err)
	}
	return l
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
