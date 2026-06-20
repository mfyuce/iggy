# To Be Decided

## COOP_TASKRUN fix — upstream or not?

**Fix:** `0f0e8d24` — decouples `thread_pool_limit(0)` from COOP_TASKRUN so TCP/HTTP listeners work on kernels < 5.19.

**Options:**
1. Upstream as a separate PR to `apache/iggy` (independent of OTLP source PR).
2. Keep only in maya fork / not upstream (the OTLP source PR goes upstream without it).

**Consideration:** The fix is generically useful for anyone running iggy on older kernels. But it touches server internals and needs separate review justification.
