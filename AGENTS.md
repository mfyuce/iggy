# Apache Iggy Agent Guidelines

**Apache Iggy (Incubating)** — persistent message streaming in Rust. Thread-per-core shared-nothing, `io_uring` + `compio`. Transports: QUIC, WebSocket, TCP (binary), HTTP. Connectors subsystem ingests/egresses via dlopened plugins.

> Skills under `.claude/skills/` are scoped to **connectors** (`core/connectors/`). Load [connectors-overview](.claude/skills/connectors-overview/SKILL.md) first for any change there.

## STOP and ask the user before

- Bumping `iggy_connector_sdk` MAJOR version or changing any FFI signature in `core/connectors/sdk/src/{sink,source}.rs`.
- Modifying `core/binary_protocol/` wire-format types tagged `#[repr(C)]`.
- Touching `core/connectors/runtime/src/state.rs` in a way that changes the atomic-rename protocol.
- Renaming default consumer groups, plugin path resolution rules, or any operator-facing identifier.
- Force-pushing to `master`, amending shipped commits, or skipping hooks with `--no-verify` / `--no-gpg-sign`.

## Quick reference

```bash
# Pre-merge CI
cargo fmt --all
cargo sort --no-format --workspace    # --no-format mandatory
cargo clippy --all-features --all-targets -- -D warnings
cargo test -p <crate>
# Other CI
./scripts/ci/{taplo,markdownlint,shellcheck,license-headers,trailing-whitespace,trailing-newline}.sh
# Run
cargo run --bin iggy-server
cargo run --bin iggy -- <sub>
IGGY_CONNECTORS_CONFIG_PATH=... cargo run --bin iggy-connectors
./scripts/run-bdd-tests.sh
```

Hooks: `pre-commit install` or `prek install`.

## Structure

```text
core/server/          server binary
core/server-ng/       next-gen server (Viewstamped Replication, WIP)
core/sdk/             Rust client SDK
core/cli/             iggy CLI
core/connectors/      runtime + SDK + sinks/sources
core/integration/     cross-crate integration tests
core/consensus/       Raft (Miri-checked unsafe iobuf)
foreign/              Java/.NET/Python/Go/C++/Node.js SDKs
bdd/                  Gherkin BDD (all SDKs)
```

## Skills (connectors-scoped)

- [connectors-overview](.claude/skills/connectors-overview/SKILL.md) - router (load first)
- [connector-runtime](.claude/skills/connector-runtime/SKILL.md) - FFI host, lifecycle, state, metrics
- [connector-sdk](.claude/skills/connector-sdk/SKILL.md) - traits, decoders/encoders, retry
- [connector-sink](.claude/skills/connector-sink/SKILL.md) - sink plugin authoring
- [connector-source](.claude/skills/connector-source/SKILL.md) - source plugin authoring
- [connector-transform](.claude/skills/connector-transform/SKILL.md) - transform authoring
- [connector-testing](.claude/skills/connector-testing/SKILL.md) - unit + integration test patterns

## Principles

1. **Apache 2.0 header** on every new source file. Follow `licenserc.toml`; `.rs` uses `// ...`, `Cargo.toml`/shell use `# ...`.
2. **Verification order:** `fmt --all` → `sort --no-format --workspace` → `clippy --all-features -- -D warnings` → `test`. CI enforces.
3. **`--no-format` for `cargo sort`** always. Without it feature arrays flatten and `taplo` re-expands them.
4. **No LLM-slop:** em dashes, gratuitous semicolons, hedging narrative, trailing summaries.
5. **Idiomatic Rust.** `FromStr`/`Display`/`From`/`TryFrom`. Structured `Err` enums. No `unwrap()`/`expect()` on external I/O outside tests.
6. **Imports at top.** Group: std → external → `crate::`. No inline `use` inside functions.
7. **Precise names.** No `b`, `p`, `t`, `m`. Match literal API field names as log labels.
8. **Comments explain WHY.** No comment by default. Never reference the current task/PR.
9. **Never `cargo install`** without authorization. Toolchain pinned in `rust-toolchain.toml`.
10. **`tokio::sync::Mutex`** wherever a lock is held across `.await`. Lock, clone, drop guard, then I/O.
11. **Zero clone in hot loops.** `&self`, `&str`, `&[T]`. `Vec::with_capacity(n)` when size is known.
12. **Forward-compatible config.** New TOML fields use `#[serde(default)]` or `Option<T>`.

## Testing

- **Unit:** `#[cfg(test)] mod tests` at EOF. **In-crate:** `<crate>/tests/`. **BDD:** `bdd/`; driver: `scripts/run-bdd-tests.sh`.
- **Cross-crate (real infra):** `core/integration/tests/`. `#[iggy_harness]` → `ServerHandle`, `ConnectorsRuntimeHandle`, `ClientHandle`. External backends via `testcontainers-modules` (Docker required). Logs: `test_logs/`.
- Filter: `cargo test -p integration -- connectors::runtime::benchmark::given_logging_format_json`
- BDD naming: `given_X_when_Y_should_Z`. **Integration crate has no `--test` target** — filter by path inside the single `mod.rs` binary.
- **Miri** only covers `binary_protocol` + `consensus` (cannot emulate `io_uring`; do not expand to `compio` crates).

## Local state (gitignored, safe to delete)

`local_data/` · `local_state/` · `test_logs/` · `performance_results/` · `target/`

## Commits, PRs, releases

Conventional commits: `feat(connectors): ...`. Subject ≤ 72 chars. Body: why before what. Types/scopes enforced by `.github/workflows/pr-title.yml`.

## Infra quick-ref (yucemonitoring vcluster)

- iggy HTTP: `http://10.20.4.31:31920`. Auth: `POST /users/login {"username":"iggy","password":"iggy"}`.
- iggy-bench: `/home/fatihyuce/.cargo-target/release/iggy-bench`, NodePort `10.20.4.31:31921` (TCP). Not in Docker image (Alpine/musl build fails).
- Node SSH: `maya@<ip>`, password `test123`. Ubuntu 22.04, kernel 6.8.0-124.
- kubeconfig: `~/.kube/yucemonitoring.config`, ns `yucemonitoring`, `--insecure-skip-tls-verify`.

## READY FOR HANDOVER (2026-06-20, session 7)

**Current state:** OTLP source PR #3516 submitted to `apache/iggy`, awaiting review. OTLP→Iggy pipeline live in `yucemonitoring`. Working tree clean. All remotes (`fork`, `ulak`) synced. `origin/master` diverges by design (17 local commits ahead).

**Next steps:** Monitor PR #3516 for reviewer feedback and address comments. Decide COOP_TASKRUN upstream PR (see TOBEDECIDED.md). Track `server-ng` maturity for HA (see TODO.md).

**Open decisions:** COOP_TASKRUN upstream PR — upstream as standalone fix or keep only in maya fork. See TOBEDECIDED.md.
