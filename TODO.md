# TODO

## Open PRs (apache/iggy)

- [ ] **#3516** `S-waiting-on-review` — OTLP gRPC source connector
  - `atharvalade` comments (stale diff, on old PR version): `try_send` drops data under backpressure — should be `send().await` + `RESOURCE_EXHAUSTED`; response missing `partial_success`. Address before pushing for re-review.
- [ ] **#3517** `S-waiting-on-review` — COOP_TASKRUN / thread_pool decoupling
- [ ] **#3523** `S-waiting-on-review` — quickwit_sink convention fix (409 absorb + request_timeout addressed)
- [ ] **#3525** `S-waiting-on-review` — IggyConsumer InvalidOffset recovery
  - `atharvalade` CHANGES_REQUESTED: per-partition concern addressed (DashMap); "5 features in one PR" addressed (rebased to SDK-only). **Reply on PR explaining this, request re-review.**
- [ ] **#3529** `S-waiting-on-review` — OTLP/gRPC sink connector

## Infrastructure

- [ ] **Enable Iggy segment cleaner** in production: set `IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED=true` on iggy-server deployment. Without it, expired segments accumulate indefinitely even with `message_expiry` set.
- [ ] **QW 0.9 upgrade** — nanosecond timestamp input_format + retention on `iggy-{metrics,flows,logs,apigw}` indices. Currently 0.8.2, mode:dynamic, no retention.
- [ ] **Deploy otlp_source proto format** — switch K8s config to `format = "proto"` for 4-5x storage reduction once old JSON messages age out or CG offsets reset past them.

## TBD (open investigations)

- [ ] **`PollingKind::First` returns `InvalidOffset(0)` over TCP but HTTP works** — HTTP `?strategy=first` returns offset 0 fine; TCP binary returns error 4100 even when offset 0 exists. Hypothesis: auto_commit + TCP handler interaction. See [[invalid-offset-tbd]]. Fix would let #3525 use `first()` instead of `last()`.
- [ ] **OTLP sink proto decode spam** — after `last()` recovery, otlp_sink on `format=proto` hits old JSON messages: `invalid wire type`. Options: reset CG offset past JSON era, wait for retention, or add format auto-detection. See [[invalid-offset-tbd]].
- [ ] **Iggy storage compression** — upstream feature request (gzip/zstd/snappy at segment level). See [[project-todo-iggy-compression]].

## Tracking

- [ ] **`server-ng`** (Viewstamped Replication) maturity — for HA consideration.
