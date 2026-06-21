# Apache Iggy Agent Guidelines

**Apache Iggy (Incubating)** — persistent message streaming in Rust. Thread-per-core shared-nothing, `io_uring` + `compio`. Transports: QUIC, WebSocket, TCP (binary), HTTP. Connectors subsystem ingests/egresses via dlopened plugins.

> Skills under `.claude/skills/` are scoped to **connectors** (`core/connectors/`). Load [connectors-overview](.claude/skills/connectors-overview/SKILL.md) first for any change there.

## STOP and ask the user before

- Bumping `iggy_connector_sdk` MAJOR version or changing any FFI signature in `core/connectors/sdk/src/{sink,source}.rs`.
- Modifying `core/binary_protocol/` wire-format types tagged `#[repr(C)]`.
- Touching `core/connectors/runtime/src/state.rs` in a way that changes the atomic-rename protocol.
- Renaming default consumer groups, plugin path resolution rules, or any operator-facing identifier.
- Force-pushing to `master`, amending shipped commits, or skipping hooks with `--no-verify` / `--no-gpg-sign`.

## CI (run in order)

```bash
cargo fmt --all
cargo sort --no-format --workspace   # --no-format is mandatory
cargo clippy --all-features --all-targets -- -D warnings
cargo test -p <crate>
./scripts/ci/{taplo,markdownlint,shellcheck,license-headers,trailing-whitespace,trailing-newline}.sh
```

Hooks: `pre-commit install` or `prek install`.

## Key rules

1. **Apache 2.0 header** on every new source file (`licenserc.toml`; `.rs` → `// ...`, TOML/shell → `# ...`).
2. **`cargo sort --no-format`** always — without it feature arrays flatten and taplo re-expands them.
3. **No LLM-slop:** em dashes, hedging narrative, trailing summaries, gratuitous semicolons.
4. **Idiomatic Rust:** `FromStr`/`Display`/`TryFrom`. Structured `Err` enums. No `unwrap()`/`expect()` on external I/O outside tests.
5. **`tokio::sync::Mutex`** wherever a lock is held across `.await`.
6. **Forward-compatible config:** new TOML fields use `#[serde(default)]` or `Option<T>`.
7. **Comments explain WHY only.** Never reference the current task/PR. No comment by default.

## Testing

- Unit: `#[cfg(test)] mod tests` at EOF. Integration: `core/integration/tests/` with `#[iggy_harness]` (Docker required).
- Filter: `cargo test -p integration -- connectors::runtime::benchmark::given_logging_format_json`
- **Integration crate has no `--test` target** — filter by path inside the single `mod.rs` binary.
- **Miri** covers only `binary_protocol` + `consensus` — do not expand to `compio` crates.
- BDD naming: `given_X_when_Y_should_Z`. Driver: `./scripts/run-bdd-tests.sh`.

## Skills (connectors-scoped)

- [connectors-overview](.claude/skills/connectors-overview/SKILL.md) — load first for any connectors change
- [connector-runtime](.claude/skills/connector-runtime/SKILL.md) · [connector-sdk](.claude/skills/connector-sdk/SKILL.md)
- [connector-sink](.claude/skills/connector-sink/SKILL.md) · [connector-source](.claude/skills/connector-source/SKILL.md)
- [connector-transform](.claude/skills/connector-transform/SKILL.md) · [connector-testing](.claude/skills/connector-testing/SKILL.md)

## Commits / PRs

Conventional commits: `feat(connectors): ...`. Subject ≤ 72 chars. Body: why before what.

## Infra quick-ref (yucemonitoring vcluster)

- iggy HTTP: `http://10.20.4.31:31920`. Auth: `POST /users/login {"username":"iggy","password":"iggy"}`.
- iggy-bench: `/home/fatihyuce/.cargo-target/release/iggy-bench`, NodePort `10.20.4.31:31921` (TCP). Not in Docker image (Alpine/musl fails).
- Node SSH: `maya@<ip>`, password `test123`. kernel 6.8.0-124.
- kubeconfig: `~/.kube/yucemonitoring.config`, ns `yucemonitoring`, `--insecure-skip-tls-verify`.
- Jenkins: `maya-anomaly-platform`, `type=iggy`. Script: `anomaly/scripts/trigger_iggy_build.sh`. Trivy CVE scan stage fails by default (false positive) — check `BUILT_IMAGE=` in console log; if present, deploy anyway.
- QW REST: port-forward `svc/maya-quickwit-s3 7280:7280`. QW version: **0.8.2** — only `unix_timestamp` (seconds) supported as datetime input_format; `unix_timestamp_nanos/millis` require 0.9.
- iggy→QW sink: `iggy-{metrics,flows,logs,apigw}` indices (mode:dynamic, no retention). Consumer groups: `qw_sink_{metrics,flows,logs,apigw}`. If connector loops on `InvalidOffset`, delete the consumer group via HTTP API and restart.

## READY FOR HANDOVER (2026-06-21, session 11)

**Current state:** Four PRs open on `apache/iggy`: #3516 (OTLP gRPC source, S-waiting-on-review), #3517 (COOP_TASKRUN, S-waiting-on-review), #3523 (quickwit_sink, S-waiting-on-review — review addressed: 409 absorb + request_timeout), #3525 (IggyConsumer InvalidOffset, S-waiting-on-review). Full Iggy→QW pipeline live in `yucemonitoring`: 4 QW sink connectors active. Collector cutover applied. Working tree clean.

**Next steps:** Monitor PRs for second reviewer. Consider QW upgrade to 0.9 for nanosecond timestamp support and retention on iggy-{metrics,flows,logs,apigw} indices.

**Open decisions:** None.
