# OTLP Sink

The OTLP sink forwards logs, metrics and traces from Iggy topics to any OpenTelemetry receiver, such as an OpenTelemetry Collector, Quickwit or Tempo. It speaks OTLP/gRPC by default and OTLP/HTTP (`POST /v1/{traces,metrics,logs}`, `application/x-protobuf`) on request. One sink instance serves one signal.

It pairs with an OTLP source connector, but accepts any topic that carries the formats below.

## Storage formats

| `format` | Stream `schema` | Message content | Behavior |
| --- | --- | --- | --- |
| `json` (default) | `json` | One JSON document per record, as written by the source's JSON path | The sink rebuilds `Export*ServiceRequest` messages, grouped by full resource, and sends up to `batch_size` records per request. |
| `proto` | `raw` | One `Export*ServiceRequest` per message, as written by the source's proto path | Each message is forwarded unchanged, one request per message. Nothing is decoded. |

A message with the wrong payload variant for the configured format is dropped and reported (see [Delivery semantics](#delivery-semantics)). `Payload::Proto` text that holds a JSON document is accepted in JSON mode.

The JSON path reconstructs what the source's JSON documents carry. Instrumentation scope, span events and links, exemplars and histogram buckets are not in those documents, so they are not re-created: histograms and summaries are sent as gauges holding their count, sums are sent cumulative. Use `format = "proto"` when full fidelity matters. Trace and span IDs must be hex of exactly 16 and 8 bytes. A span whose `trace_id` or `span_id` is missing or malformed is dropped, which fails the batch (see [Delivery semantics](#delivery-semantics)). A malformed `parent_span_id`, or a malformed ID on a log record, is cleared and a warning is logged.

## Configuration

| Key | Default | Description |
| --- | --- | --- |
| `endpoint` | Required | Receiver URL with an `http` or `https` scheme and a host. For `transport = "http"` it is the base URL and `/v1/<signal>` is appended. `https` uses the bundled web PKI roots. |
| `signal` | Required | `traces`, `metrics` or `logs`. |
| `transport` | `"grpc"` | `grpc` or `http`. |
| `format` | `"json"` | `json` or `proto`. |
| `compression` | `false` | Gzip requests: gRPC message compression, or HTTP `Content-Encoding: gzip`. |
| `headers` | `{}` | Sent with every request as gRPC metadata or HTTP headers. Values are `SecretString`s: they are validated in `open()` and never appear in logs or `Debug` output. Quickwit routes OTLP data to an index with `qw-otel-<signal>-index`. |
| `verbose_logging` | `false` | Log per-batch counts at `info` instead of `debug`. |
| `batch_size` | `1000` | JSON mode: records per export request. |
| `max_retries` | `3` | Total export attempts including the first; `0` and `1` both allow one attempt. |
| `retry_delay` | `"1s"` | Base exponential delay between attempts. |
| `retry_max_delay` | `"5s"` | Cap for calculated delays. For OTLP/HTTP a valid `Retry-After` on 429 or any 5xx can exceed it, up to one hour. |
| `max_open_retries` | `10` | Startup connectivity attempts including the first; `0` and `1` disable retries. |
| `open_retry_max_delay` | `"30s"` | Cap for delays between startup attempts. |
| `timeout` | `"30s"` | Timeout per export request and per connection attempt. `request_timeout` is accepted as an alias. |

Duration values need units, such as `250ms` or `30s`. Invalid or zero durations, malformed headers and unknown keys prevent initialization. The example in [config.toml](config.toml) is a complete sink entry; a runtime-ready copy lives at `core/connectors/runtime/example_config/connectors/otlp_sink.toml`.

The runtime refuses the stream setting `poll_interval`. Use `max_wait`, as in the example.

## Startup

`open()` fails with `InitError` when the receiver is unreachable after `max_open_retries` attempts. For gRPC it opens the channel. OTLP/HTTP has no health endpoint, so the sink only checks that a TCP connection can be made. A reachable port does not prove the receiver accepts OTLP, and a missing index or wrong path shows up on the first export.

## Retries and errors

gRPC statuses `Unavailable`, `DeadlineExceeded`, `Cancelled`, `Unknown`, `ResourceExhausted`, `Aborted`, `OutOfRange` and `DataLoss` are retried with jittered exponential backoff. Every other status, such as `InvalidArgument` or `Unauthenticated`, fails at once as `PermanentHttpError`. The sink does not read the `RetryInfo` detail of a gRPC status.

OTLP/HTTP retries 429, 5xx and network errors through the shared SDK retry middleware and honors `Retry-After`. Any other non-2xx status is a `PermanentHttpError`, including 4xx, and redirects are not followed.

A successful response is inspected. If `partial_success.rejected_*` is above zero, the rejected records are counted and the batch fails with `PermanentHttpError`, since the OTLP specification forbids retrying them. A `partial_success` with only an `error_message` is logged as a warning and the batch succeeds.

## Delivery semantics

The runtime commits offsets when it polls, so sink delivery is at-most-once. An error returned from `consume` is logged and counted by the runtime, but the batch is not redelivered. The sink therefore does as much as it can before it reports a failure:

- JSON mode sends every convertible record, then fails the batch if any message was dropped or rejected. Requests are independent: a failed chunk does not stop the next one.
- Proto mode attempts every message even when an earlier one failed. Messages already exported are not rolled back, so a failed batch can have been partly delivered. The returned error is the last one seen, and the summary line lists the failed, dropped and rejected counts.
- A batch where nothing can be exported, for example one holding only wrong payloads, returns an error without calling the receiver.
- Retries can duplicate data the receiver already accepted before a timeout. OTLP has no deduplication key.

The counters `messages_sent`, `messages_dropped`, `messages_failed`, `records_rejected`, `batches_sent` and `batches_failed` are logged when the sink closes.

## Known limits

- gRPC `https` endpoints use the built-in web PKI roots. Private CAs and client certificates are not configurable.
- Proto mode makes one request per Iggy message. A source that writes small messages produces many calls.
