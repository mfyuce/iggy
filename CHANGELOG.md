# Changelog

## [Session 18] - 2026-07-28
### Added
- Pre-allocated NDJSON buffer size in `quickwit_sink` with `messages.len() * 256` heuristic to eliminate heap reallocations without doubling RAM usage (resolving PR #3523 feedback).
- Configurable `timeout` parameter (default 30s) and `/health/readyz` readiness checks to prevent indefinite hangs in `quickwit_sink`.
- Safe reference serialization (`LogRecordDoc`, `SpanDoc`, `MetricDoc`) in `otlp_source` to prevent deep metadata clones (resolving PR #3516 feedback).
- Explicit gRPC `invalid_argument` validation errors and backpressure check in `otlp_source` (returns `RESOURCE_EXHAUSTED` instead of dropping parts of a batch).
- Synced branches locally, merged to local `master`, and pushed local `master` to GitLab `ulak/master` and GitHub `fork/master`.
- Updated GitHub PR #3516 and PR #3523 branches (`feat/otlp-source-connector` and `fix/quickwit-sink-convention`) on `fork` remote.

### Fixed
- Bypassed the Linux Kernel 6.8 / BuildKit `/proc/acpi` procfs masking runc bug on the Jenkins agent by patching `push.sh` in the `anomaly` repository to use the host's native Docker build.
- Fixed Quickwit log overcounting in success logs by keeping a separate, verified counter for successfully serialized records.

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
