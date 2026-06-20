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
