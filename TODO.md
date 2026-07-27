# TODO

## Open PRs (apache/iggy)

- [ ] **#3516** `S-waiting-on-review` — OTLP gRPC source connector
  - Local branch: [feat/otlp-source-connector](file:///home/fatihyuce/work/projects/maya3/ng_sdn/iggy/core/connectors/sources/otlp_source/)
  - Notes: Address backpressure comments (using `send().await` + `RESOURCE_EXHAUSTED` instead of `try_send` dropping, and add `partial_success` handling).
- [ ] **#3523** `S-waiting-on-review` — quickwit_sink convention fix
  - Local branch: [fix/quickwit-sink-convention](file:///home/fatihyuce/work/projects/maya3/ng_sdn/iggy/core/connectors/sinks/quickwit_sink/)

## Closed / Merged PRs (Cleaned up & Synced)

- [x] **#3517** `Merged` — COOP_TASKRUN / thread_pool decoupling
- [x] **#3525** `Merged` — IggyConsumer InvalidOffset recovery (per-partition DashMap rebase)
- [x] **#3529** `Merged` — OTLP/gRPC sink connector
  - [ ] **Follow-up PR: Trace/Span ID validation fix** — Open a new PR to upstream GitHub `apache/iggy` master with the trace/span ID length sanitization changes (filtering out invalid trace/span ID sizes to avoid gRPC InvalidArgument blocks).

## Pending PRs to be opened on GitHub (apache/iggy)

- [ ] **New PR: OTLP Sink Trace/Span Sanitization** — Open a new PR on GitHub to upstream the trace/span ID validation changes from the local `feat/otlp-sink-connector` branch to the remote `apache/iggy` master.

## High Priority Issues / Investigations

- [ ] **`PollingKind::First` returns `InvalidOffset(0)` over TCP but HTTP works** (InvalidOffset Bug)
  - Bug description: HTTP `?strategy=first` works fine, but TCP binary protocol returns error 4100 (`InvalidOffset`) even when offset 0 is valid.
  - Plan:
    1. Reproduce deterministically (create topic, send messages, delete first segment, poll first over TCP vs HTTP).
    2. Check `validate_checksums_and_offsets` behavior.
    3. Resolve the `from_repr` u64 value loss in TCP client response deserialization.
    4. Once fixed, update recovery path in PR #3525 to use `first()` instead of `last()`.

## Infrastructure

- [ ] **Enable Iggy segment cleaner** in production: set `IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED=true` on iggy-server deployment. Without it, expired segments accumulate indefinitely even with `message_expiry` set.
- [ ] **QW 0.9 upgrade** — nanosecond timestamp input_format + retention on `iggy-{metrics,flows,logs,apigw}` indices. Currently 0.8.2, mode:dynamic, no retention.
- [ ] **Deploy otlp_source proto format** — switch K8s config to `format = "proto"` for 4-5x storage reduction once old JSON messages age out or CG offsets reset past them.

## TBD (open investigations)

- [ ] **OTLP sink proto decode spam** — after `last()` recovery, otlp_sink on `format=proto` hits old JSON messages: `invalid wire type`. Options: reset CG offset past JSON era, wait for retention, or add format auto-detection. See [[invalid-offset-tbd]].
- [ ] **Iggy storage compression** — upstream feature request (gzip/zstd/snappy at segment level). See [[project-todo-iggy-compression]].

## Tracking

- [ ] **`server-ng`** (Viewstamped Replication) maturity — for HA consideration.

