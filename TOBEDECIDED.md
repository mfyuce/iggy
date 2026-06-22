# To Be Decided

## `PollingKind::First` TCP bug (open since 2026-06-22)

HTTP `GET ?strategy=first` returns the first available message fine. TCP binary `PollingKind::First` returns `InvalidOffset(0)` even when the topic has messages from offset 0. Hypothesis: auto_commit or TCP handler applies stricter validation. Once understood, PR #3525 can switch from `last()` back to `first()` to avoid skipping history.

## `otlp_source` backpressure — address in PR #3516

Reviewer `atharvalade` flagged two issues in `core/connectors/sources/otlp_source/src/server.rs` (comments are on the old PR #3525 diff, now stale — fix belongs in #3516):
1. `try_send` silently drops gRPC export calls under backpressure. Should be `send().await` + timeout + `Status::RESOURCE_EXHAUSTED` so OTel clients retry.
2. Response missing OTLP `partial_success` field — callers cannot tell how many items were accepted.
