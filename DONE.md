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
