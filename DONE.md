# Done

Completed work, archived from CLAUDE.md handover blocks.

---

## Sessions 1–4 (2026-06 infra + OTLP pipeline)

- **OTLP→Iggy pipeline live** in `yucemonitoring` vcluster. 4 topics (`otel/logs`, `otel/apigw`, `otel/metrics`, `otel/flows`), stream `otel`. 1-day retention → lowered to 6h. PVC 5Gi → 20Gi.
- **`iggy-connectors` image** built from maya fork with `otlp_source` plugin; 4 instances via ConfigMap. `iggy-server` image (0.10-edge) replaces stock 0.4 (protocol incompatibility).
- **Build pipeline**: Jenkins `maya-anomaly-platform`, `type=iggy` / `type=iggy-server`. Registry `maya-nexus:35000`.
- **COOP_TASKRUN fix** (`0f0e8d24`): `create_shard_executor` takes `keep_worker_pool: bool`; `thread_pool_limit(0)` only called when `coop_taskrun && !keep_worker_pool`. Both server and server-ng pass tcp/http/ws enabled as `keep_pool`. Pushed to `fork/fix/coop-taskrun-thread-pool` and `ulak/master`.
- **Kernel upgrades**: all 5 nodes master1/2/3 + worker1/2 upgraded 5.15 → 6.8.0-124. master2 had corrupt dpkg format file; fix: `rm /var/lib/dpkg/info/format` + `--force-confdef --force-confold`.
- **nodeSelector removed** from iggy deploy; pod schedulable on any node.
- **gzip enabled** on two layers:
  - OTel collector → iggy-connectors gRPC: `compression: gzip` in `maya-collector-configmap`.
  - iggy topic storage: `compression_algorithm: gzip` on all 4 topics via HTTP API.
- **Bench 1** (PinnedProducerAndConsumer, 8P+8C, 1 GiB, NodePort/LAN): 49 MB/s, p50=150 ms, p99=439 ms.
- **Bench 2** (OTLP load, RATE=10): ~1100 msg/s, iggy CPU <1%, zero errors.
- **iggy-bench added then reverted** from Docker image (charming/ssr → v8 → Alpine/musl build fails). Lives at `/home/fatihyuce/.cargo-target/release/iggy-bench`; run via NodePort `10.20.4.31:31921`.

---

## Sessions 5–10 (2026-06-21, iggy→QW sink pipeline + SDK fix)

- **quickwit_sink connector** built and merged into iggy-connectors image (`5ce5f21b`). Convention-fixed in `fbe8de90`. PR #3523 upstream.
- **4 QW sink connectors deployed** to `yucemonitoring`: consumer groups `qw_sink_{metrics,flows,logs,apigw}`. Configs in `anomaly/deploy/iggy-connectors.yaml`.
- **QW indices pre-created** via REST API with `mode: dynamic` (no `timestamp_field`, no retention) because QW 0.8.2 only supports `unix_timestamp` (seconds) as datetime input_format; nanosecond support requires 0.9.
- **Collector cutover applied** (`collector-iggy-only.yaml`): direct collector→QW exporters disabled; all OTLP traffic now flows collector→iggy-connectors→Iggy→QW sink. `maya-monitoring-config-management` distributes config to CPE devices.
- **iggy-metrics + iggy-flows ingesting** ~3k docs/s confirmed post-cutover; 350k+ docs in indices.
- **InvalidOffset bug discovered**: new/deleted consumer groups get offset=0; if topic retention has purged offset 0, `poll_messages` returns `IggyError::InvalidOffset` and the runtime loops forever. Workaround: delete consumer group via iggy HTTP API + restart pod.
- **SDK fix committed** (`b22e1d6a`): `IggyConsumer` gains `fallback_to_first: Arc<AtomicBool>`; on `InvalidOffset` the flag is set + `warn!` emitted; next poll uses `PollingStrategy::first()`; flag cleared after first successful non-empty poll. PR #3525 upstream.

---

## Session 11 (2026-06-21, quickwit_sink review fixes)

- **#3523 review by ryerraguntla** — two issues addressed in `4aa51d4d`:
  - `create_index()` now absorbs `409 CONFLICT` and `400 BAD_REQUEST + "already exists"` as `Ok(())` — fixes concurrent-open race and retry-after-succeeded-5xx.
  - Added `request_timeout: Option<String>` (default 30s) to `QuickwitSinkConfig`; `reqwest::Client` built with `.timeout()` — fixes indefinite hang under network partition.
- **Circuit breaker** — reviewer asked for judgment call; decided existing `HttpRetryMiddleware` + `PermanentHttpError` mapping is sufficient for an internal cluster service. No code change.

---

## Sessions 12–13 (2026-06-22, SDK iterations + otlp_sink + proto format)

- **IggyConsumer SDK fix iterated** (3 additional commits on fix branch, merged to master):
  - `abd030597`: per-partition tracking — replaced `Arc<AtomicBool>` with `Arc<DashMap<u32, ()>>` keyed by `partition_id`. InvalidOffset on partition A no longer forces first()-recovery on unrelated partitions.
  - `e2bcc2be5` / `88b4838fb`: pre-arm fallback for new consumer groups — `initialize_consumer_group` returns `true` when group is freshly created; callers set fallback immediately so the very first poll uses first-available instead of Next, eliminating the guaranteed startup error.
  - `e72898b38`: switched recovery from `first()` to `last()` — `PollingKind::First` returns `InvalidOffset(0)` over TCP even when offset 0 exists (HTTP works). TBD investigation; `last()` always succeeds.
- **Segment cleaner discovery**: Iggy `cleaner_enabled = false` by default; expired segments accumulate without `IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED=true`. Segment deletion also blocked by any consumer group with offset in the segment.
- **`feat(connectors/otlp_sink)`** (`b535b80ed`, PR #3529): OTLP/gRPC sink connector — reads JSON or raw proto messages from Iggy and forwards to any OTLP backend (QW, Jaeger, Tempo) via gRPC. JSON mode reconstructs OTel proto from otlp_source JSON output.
- **`feat(connectors/otlp_source): add proto storage format`** (`5ba2be566`): `format = "proto"` mode prost-encodes entire Export*ServiceRequest as single raw message. 4-5x storage reduction vs JSON. Paired with otlp_sink proto mode.

---

## Session 16 (2026-07-16/17, OTLP ID Validation & local testing)

- **OTLP validation bug solved (LOCAL ✓)**: Downstream OTLP gRPC endpoint (Quickwit) returned `InvalidArgument` when span/trace IDs were empty or not exactly 8/16 bytes (caused by older flow generator bugs). Implemented client-side sanitizers in `otlp_sink`:
  - `sanitize_trace_request` drops spans with invalid `trace_id` (not 16 bytes) or `span_id` (not 8 bytes) and clears invalid `parent_span_id` (not 8 bytes) while logging warnings with the connector ID.
  - `sanitize_logs_request` clears invalid `trace_id`/`span_id` in log records.
- **Unit Tests Added**: Added `test_sanitize_trace_request` and `test_sanitize_logs_request` to `otlp_sink/src/lib.rs`. All 14 tests pass successfully.
- **Jenkins Build #178**: Stopped stuck build #177 (network hang) and triggered a new build #178 which successfully completed and produced image:
  `192.168.57.202:35000/iggy-connectors:20260716-155359-bab46328d556`
- **Kubectl deployment blocked**: Actual deployment to `yucemonitoring` is currently pending due to expired Rancher Token in `~/.kube/yucemonitoring.config`. Added instructions for the next session to update the token and apply the manifest.
