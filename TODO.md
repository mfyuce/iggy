# TODO

## Open PRs (all S-waiting-on-review on apache/iggy)

- [ ] **#3516** — OTLP gRPC source connector
- [ ] **#3517** — COOP_TASKRUN / thread_pool decoupling
- [ ] **#3523** — quickwit_sink convention fix
- [ ] **#3525** — IggyConsumer InvalidOffset recovery (fallback to first)

## Infrastructure

- [ ] **QW 0.9 upgrade** — enables `unix_timestamp_nanos/millis` input_format → add retention policy on `iggy-{metrics,flows,logs,apigw}` indices. Currently on 0.8.2 with mode:dynamic and no retention.

## Tracking

- [ ] **`server-ng`** (Viewstamped Replication) maturity — for HA consideration.
