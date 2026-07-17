# Changelog

## [Session 17] - 2026-07-17
### Added
- Direct vcluster control-plane access by patching the `yucemonitoring` service to `NodePort` (`31633`) and generating a dedicated kubeconfig at `~/.kube/yucemonitoring.config` from the host cluster's `vc-yucemonitoring` secret.
- Documented direct vcluster access guide in Confluence page ID 422805516 (`MIL` space).

### Fixed
- Changed `otlp_sink_metrics` configuration to use `signal = "traces"` and header `"qw-otel-traces-index" = "metrics3"`. This resolves the Quickwit `unimplemented` gRPC error, as the `metrics` topic in the `otel` stream actually carries OTLP traces data, and Quickwit 0.8.2 does not support OTLP metrics.
- Redeployed and rolled out `iggy-connectors` pod with the updated configuration. All OTLP source and Quickwit sink connectors are now healthy and consuming/exporting telemetry successfully.

## [Session 16] - 2026-07-17

### Added
- Unit tests for trace and log sanitization inside `core/connectors/sinks/otlp_sink/src/lib.rs`.
- `invalidoffset_timeline_analysis.md` timeline and root-cause analysis for SDK recovery failures.
- `core/connectors/sinks/otlp_sink/config.toml` template configuration file.

### Fixed
- Sanitized OTLP `trace_id` and `span_id` lengths in `otlp_sink` before sending batches to downstream gRPC endpoints to prevent `InvalidArgument` blocker bugs.
- Cleared malformed `parent_span_id` fields instead of dropping the trace.
- Updated Jenkins pipeline logging to include `connector_id` context.
