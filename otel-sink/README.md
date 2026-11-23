# otel-sink

`otel-sink` is a minimal [OTLP](https://opentelemetry.io/docs/specs/otlp/)
trace receiver used by the Gateway API conformance suite to verify
`TelemetryPolicy` tracing. It is a test fixture, not a collector: it accepts
every export, writes **one JSON line per span to stdout** and keeps the most
recent spans in memory for debugging. Conformance tests read the pod logs, pick
the lines that start with `SPAN ` and correlate them with the requests they
sent through the Gateway under test.

Image: `registry.k8s.io/gateway-api/conformance/otel-sink:<tag>`

## Listeners

| Port (env var)             | Protocol  | Endpoint                                                              |
|----------------------------|-----------|-----------------------------------------------------------------------|
| `4317` (`OTLP_GRPC_PORT`)  | OTLP/gRPC | `opentelemetry.proto.collector.trace.v1.TraceService/Export`, plaintext, gzip accepted |
| `4318` (`OTLP_HTTP_PORT`)  | OTLP/HTTP | `POST /v1/traces` with `Content-Type: application/x-protobuf` or `application/json`, optional `Content-Encoding: gzip`; HTTP/1.1 and h2c |
| `8080` (`HTTP_PORT`)       | HTTP      | `GET /healthz`, `GET /spans[?traceId=&contains=&limit=]`, `GET /stats` |

`OTEL_SINK_BUFFER_SIZE` (default `10000`) bounds the in-memory buffer served by
`/spans`; it does not affect what is logged. Every export succeeds (no partial
success is ever reported) so an implementation never retries into the sink.

## Log line format

Each span produces exactly one line on stdout:

```
SPAN {"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba902b7","parentSpanId":"","name":"ingress","kind":"SERVER","startTimeUnixNano":1700000000000000000,"endTimeUnixNano":1700000000001000000,"status":"UNSET","attributes":{"gateway_api_conformance.token":"6f1c2a3b-0000-4000-8000-000000000001","http.request.method":"GET","url.path":"/telemetrypolicy/6f1c2a3b-0000-4000-8000-000000000001"},"resource":{"service.name":"gateway"},"scope":"envoy","transport":"grpc","exportHeaders":{"authorization":"Bearer abc","content-type":"application/grpc"}}
```

The payload is the compact JSON encoding of
[`api.Span`](api/span.go):

| Field               | Type                 | Meaning |
|---------------------|----------------------|---------|
| `traceId`           | string               | lowercase hex of the 16-byte trace id |
| `spanId`            | string               | lowercase hex of the 8-byte span id |
| `parentSpanId`      | string               | lowercase hex of the parent span id, `""` for a root span |
| `name`              | string               | span name |
| `kind`              | string               | `UNSPECIFIED`, `INTERNAL`, `SERVER`, `CLIENT`, `PRODUCER` or `CONSUMER` |
| `startTimeUnixNano` | number (uint64)      | start time, ns since the Unix epoch |
| `endTimeUnixNano`   | number (uint64)      | end time, ns since the Unix epoch |
| `status`            | string               | `UNSET`, `OK` or `ERROR` |
| `attributes`        | object string→string | span attributes, values stringified (see below) |
| `resource`          | object string→string | resource attributes, values stringified |
| `scope`             | string               | instrumentation scope name, `""` if absent |
| `transport`         | string               | `grpc` or `http` |
| `exportHeaders`     | object string→string | gRPC metadata / HTTP headers of the export request, keys lowercased, multiple values joined with `", "`. Nothing is redacted. |

Value stringification (`sink.Stringify`): strings verbatim; bytes base64;
bool/int/double as `true`, `42`, `1.5`; arrays and key-value lists as compact
JSON (`["a",1,true]`, `{"k":"v"}`). To keep the log stream small (kubelet
rotates container logs at 10 MiB) at most 64 keys are kept per map and values
longer than 256 bytes are truncated and suffixed with `...`.

Fields are only ever added to this schema, never renamed or removed; parsers
should ignore unknown fields.

### Parsing from Go

```go
import "sigs.k8s.io/gateway-api-conformance-images/otel-sink/api"

for _, line := range strings.Split(podLogs, "\n") {
	span, ok := api.ParseLogLine(line)
	if !ok {
		continue // not a SPAN line
	}
	if token, ok := span.Attribute("gateway_api_conformance.token"); ok && token == want {
		// found the span for our request
	}
}
```

## Running locally

```bash
go run ./otel-sink
# in another shell, post a span over OTLP/HTTP JSON:
curl -s -X POST localhost:4318/v1/traces -H 'Content-Type: application/json' -d '{
  "resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"gw"}}]},
  "scopeSpans":[{"spans":[{"traceId":"S/kvNXezTaajzpKdDg5HNg==","spanId":"APBnqgupArc=","name":"ingress","kind":2}]}]}]}'
curl -s localhost:8080/spans
```

Build the image with `docker build -f docker/Dockerfile.otel-sink .`.
