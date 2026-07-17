# Changelog

## [Session 16] - 2026-07-17
### Added
- Unit tests for trace and log sanitization inside `core/connectors/sinks/otlp_sink/src/lib.rs`.
- `invalidoffset_timeline_analysis.md` timeline and root-cause analysis for SDK recovery failures.
- `core/connectors/sinks/otlp_sink/config.toml` template configuration file.

### Fixed
- Sanitized OTLP `trace_id` and `span_id` lengths in `otlp_sink` before sending batches to downstream gRPC endpoints to prevent `InvalidArgument` blocker bugs.
- Cleared malformed `parent_span_id` fields instead of dropping the trace.
- Updated Jenkins pipeline logging to include `connector_id` context.
