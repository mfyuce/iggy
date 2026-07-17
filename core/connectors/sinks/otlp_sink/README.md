# OTLP Sink

Consumes OpenTelemetry logs, metrics, and traces from Iggy topics and forwards them to a downstream OTLP-compatible receiver (such as Quickwit, OpenTelemetry Collector, Jaeger, or Datadog) over gRPC or HTTP.

## How it works

The connector supports both JSON and Protobuf formats and can export data via OTLP/gRPC or OTLP/HTTP. It reconstructs the telemetry payloads and batches them to the destination.

### Storage Formats

- `json` (default): Messages in the Iggy topic are stored as flat JSON telemetry records. The sink parses the JSON and builds the corresponding OTLP protobuf requests.
- `proto`: Messages in the Iggy topic are stored as raw prost-encoded OTLP export requests. The sink forwards the raw payloads directly to the destination with zero deserialization overhead.

### Transports

- `grpc` (default): Forwards via OTLP/gRPC. Supports gzip compression and custom gRPC metadata.
- `http`: Forwards via OTLP/HTTP (`POST /v1/{traces,metrics,logs}`). Custom headers and gzip body encoding are supported.

## Configuration

- `endpoint`: The target gRPC or HTTP OTLP receiver endpoint (e.g., `"http://localhost:4317"` or `"http://quickwit:7281"`).
- `signal`: Telemetry signal type. Options: `"traces"`, `"metrics"`, `"logs"`.
- `transport`: Wire transport type. Options: `"grpc"`, `"http"`. Defaults to `"grpc"`.
- `format`: Storage format of messages in the Iggy topic. Options: `"json"`, `"proto"`. Defaults to `"json"`.
- `compression`: Enable gzip body compression on export requests. Defaults to `false`.
- `headers`: Map of custom headers (HTTP) or metadata entries (gRPC) sent with every export request.
- `request_timeout`: Timeout for each export request in humantime format (e.g. `"30s"`). Defaults to `"30s"`.

```toml
[plugin_config]
endpoint = "http://localhost:4317"
signal = "traces"
transport = "grpc"
format = "json"
compression = true
request_timeout = "30s"

[plugin_config.headers]
# Quickwit index routing headers example:
# qw-otel-traces-index = "flows"
```
