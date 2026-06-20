# Apache Iggy Agent Guidelines

**Apache Iggy (Incubating)** — persistent message streaming in Rust. Thread-per-core shared-nothing, `io_uring` + `compio`. Transports: QUIC, WebSocket, TCP (binary), HTTP. A connectors subsystem ingests/egresses via dlopened plugins.

> Skills under `.claude/skills/` are scoped to **connectors** (`core/connectors/`). Load [connectors-overview](.claude/skills/connectors-overview/SKILL.md) first for any change there.

## STOP and ask the user before

- Bumping `iggy_connector_sdk` MAJOR version or changing any FFI signature in `core/connectors/sdk/src/{sink,source}.rs` (breaks every pre-built plugin .so).
- Modifying `core/binary_protocol/` wire-format types tagged `#[repr(C)]` (breaks every running client).
- Touching `core/connectors/runtime/src/state.rs` in a way that changes the atomic-rename protocol (corruption risk).
- Renaming default consumer groups, plugin path resolution rules, or any operator-facing identifier.
- Force-pushing to `master`, amending shipped commits, or skipping hooks with `--no-verify` / `--no-gpg-sign`.

## Quick reference

```bash
# Verification order enforced by CI (.github/actions/rust/pre-merge)
cargo fmt --all                                            # step 1
cargo sort --no-format --workspace                         # step 2  (--no-format mandatory)
cargo clippy --all-features --all-targets -- -D warnings   # step 3
cargo test -p <crate>                                      # step 4

# Other CI checks
./scripts/ci/taplo.sh             # TOML
./scripts/ci/markdownlint.sh      # Markdown
./scripts/ci/shellcheck.sh        # Shell
./scripts/ci/license-headers.sh   # Apache headers
./scripts/ci/trailing-whitespace.sh && ./scripts/ci/trailing-newline.sh

# Run
cargo run --bin iggy-server
cargo run --bin iggy -- <subcommand>
IGGY_CONNECTORS_CONFIG_PATH=... cargo run --bin iggy-connectors
./scripts/run-bdd-tests.sh
```

Pre-commit hooks wire the same scripts. Install: `pre-commit install` or `prek install`.

## Structure

```text
iggy/core/
  server/           server binary
  server-ng/        next-gen server (Viewstamped Replication, WIP)
  sdk/              Rust client SDK
  cli/              iggy CLI
  connectors/       runtime + SDK + sinks/sources
  common/           shared types
  binary_protocol/  wire format (stable, #[repr(C)])
  configs/          config plumbing + ConfigEnv derive
  integration/      cross-crate integration tests
  consensus/        Raft (Miri-checked unsafe iobuf)
iggy/foreign/       Java/.NET/Python/Go/C++/Node.js SDKs
iggy/bdd/           Gherkin BDD (all SDKs)
iggy/scripts/       CI + release + dev scripts
```

## Skills (connectors-scoped)

Load `connectors-overview` first as router.

- [connectors-overview](.claude/skills/connectors-overview/SKILL.md) - router + universal rules
- [connector-runtime](.claude/skills/connector-runtime/SKILL.md) - FFI host, lifecycle, state, metrics
- [connector-sdk](.claude/skills/connector-sdk/SKILL.md) - traits, decoders/encoders, retry
- [connector-sink](.claude/skills/connector-sink/SKILL.md) - sink plugin authoring
- [connector-source](.claude/skills/connector-source/SKILL.md) - source plugin authoring
- [connector-transform](.claude/skills/connector-transform/SKILL.md) - transform authoring
- [connector-testing](.claude/skills/connector-testing/SKILL.md) - unit + integration test patterns

## Repo-wide principles

1. **Apache 2.0 header on every new source file.** Follow `licenserc.toml`; `.rs` uses `// ...`, `Cargo.toml`/shell use `# ...`.
2. **Verification order:** `fmt --all` -> `sort --no-format --workspace` -> `clippy --all-features --all-targets -- -D warnings` -> `test`. CI enforces all.
3. **Always pass `--no-format` to `cargo sort`.** Without it multi-line feature arrays flatten and `taplo` re-expands them.
4. **No LLM-slop:** em dashes, gratuitous semicolons, hedging narrative, trailing summaries.
5. **Idiomatic Rust.** `FromStr` not `parse_foo`, `Display` not `format_foo`, `From`/`TryFrom` not `to_foo_from_bar`. Structured `Err` enums, not `String`.
6. **Imports at the top.** No inline `use` inside functions. Group: std, external, `crate::`.
7. **Precise names.** No `b`, `p`, `t`, `m`. Match literal API field names as log labels.
8. **Comments explain WHY, not WHAT.** No comment by default. Never reference the current task/PR.
9. **Never `cargo install`** without authorization. Toolchain pinned in `rust-toolchain.toml`.
10. **No `unwrap()` / `expect()` on Results from external I/O** outside tests.
11. **`tokio::sync::Mutex`** (not `std::sync::Mutex`) wherever a lock is held across `.await`. Lock, clone, drop guard, then do I/O.
12. **Code reads top to bottom.** Public consts/fns first, helpers below.
13. **Zero clone in hot loops.** `&self`, `&str`, `&[T]`. `Vec::with_capacity(n)` when size is known.
14. **Forward-compatible config.** New TOML fields use `#[serde(default)]` or `Option<T>`.

## Testing

- **Unit:** `#[cfg(test)] mod tests` at EOF of `src/lib.rs` or per-module.
- **In-crate integration:** `<crate>/tests/`.
- **Cross-crate (real infra):** `core/integration/tests/`. `#[iggy_harness]` macro wires `ServerHandle`, `ConnectorsRuntimeHandle`, `ClientHandle`. External backends use `testcontainers-modules`. Logs in `test_logs/`.
- **BDD:** `bdd/`. Driver: `scripts/run-bdd-tests.sh`.

BDD test naming: `given_X_when_Y_should_Z`. 4-part only when a distinct "then" intermediate exists.

Filter single integration test: `cargo test -p integration -- connectors::runtime::benchmark::given_logging_format_json`

## Pitfalls

- **Docker required for most integration tests** (`testcontainers-modules` spawns containers).
- **`cargo sort` without `--no-format` churns the workspace.**
- **Rust toolchain pinned in `rust-toolchain.toml`.** Use rustup so the pin auto-resolves.
- **Miri only covers `binary_protocol` + `consensus`.** Cannot emulate `io_uring`; do not expand to crates pulling `compio`.
- **Integration crate has no `--test` target.** Filter by test path inside the single `mod.rs` binary.
- **`test_logs/` grows quickly.** Wipe between major refactors.

## Local state (gitignored, safe to delete)

`local_data/` - server data | `local_state/` - connectors state | `test_logs/` - per-test logs | `performance_results/` - bench output | `target/` - build

## Commits, PRs, releases

Conventional commits with scope: `feat(connectors): ...`. Subject <= 72 chars. Body: why before what. No bullet lists of files changed. Types/scopes enforced by `.github/workflows/pr-title.yml`.

## READY FOR HANDOVER (2026-06-20, session 4)

**Current state:** All pipeline tasks complete. OTLP→Iggy live in `yucemonitoring` vcluster. All 5 nodes on kernel 6.8.0-124. iggy pod on master2, COOP_TASKRUN=true, no nodeSelector. Working tree clean.

**Completed this session:**
- All 4 remaining nodes (master2/3, worker1/2) upgraded kernel 5.15 → 6.8.0-124 (drain→apt→reboot→uncordon). master2 had corrupt dpkg format file; fixed with `rm /var/lib/dpkg/info/format` + `--force-confdef --force-confold`.
- `nodeSelector: kubernetes.io/hostname=master1` removed from iggy deploy; pod now schedulable on any node (currently master2).
- gzip enabled on two layers:
  - OTel collector → iggy-connectors (gRPC wire): `compression: none` → `compression: gzip` in `maya-collector-configmap`, `maya-monitoring-config-management` restarted.
  - iggy topic storage: `compression_algorithm: none` → `gzip` on all 4 otel topics via HTTP API.
  - otlp_source connector already had `tonic = { features = ["gzip"] }` + `accept_compressed(Gzip)` — no rebuild needed.
- PVC `iggy-data` expanded 5 Gi → 20 Gi; retention 24h → 6h. Expected steady-state ~1.5-2 GB (uncompressed), less with gzip.
- Bench 1 (PinnedProducerAndConsumer, 8P+8C, 1 GiB, NodePort/LAN): Producers 49 MB/s, p50=150 ms, p99=439 ms. NodePort adds ~150 ms RTT; loopback would be 5-10× faster.
- Bench 2 (OTLP load): RATE=10 → ~1100 msg/s, iggy CPU <1%, zero errors.

**Open decision:** Whether to upstream the COOP_TASKRUN/thread_pool decoupling fix to apache/iggy as a separate PR (the OTLP source PR must NOT include it).

**Remaining work:**
- OTLP source PR to apache/iggy (separate from COOP_TASKRUN fix).
- `server-ng` (Viewstamped Replication) maturity tracking for HA.

**Infra notes:**
- iggy-bench: `/home/fatihyuce/.cargo-target/release/iggy-bench` (CARGO_TARGET_DIR). Run via NodePort `10.20.4.31:31921` (TCP). Cannot add to Docker image (charming/ssr → v8 → Alpine/musl fail).
- iggy HTTP API: `http://10.20.4.31:31920` (no `/api` prefix). Auth: POST `/users/login` `{"username":"iggy","password":"iggy"}`.
- Node SSH: `maya@<ip>`, password `test123`. All nodes Ubuntu 22.04.
