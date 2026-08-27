# Backlog

Review follow-ups that are deliberately not in the working tree yet. Each entry
records why it waits, so deferral stays a decision instead of amnesia.

## Deferred decisions

All parked deliberately (2026-08-27); each still needs its stated decision or
runtime verification before landing.

- **Content-Security-Policy** — `app.security.csp` is unset. Proposed:
  `default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:`.
  Must be verified in a packaged run (webview font/style injection, logo data
  URIs) before landing; cannot be validated headless.
- **Opener capability scope** — the webview capability grants `opener:default`.
  Verify whether the frontend actually calls the opener JS API or whether links
  open through a Rust command; narrow or drop the grant accordingly. Needs a
  runtime check.
- **Degraded database mode** — when SQLite fails to open, sync runs
  memory-only and settings/catalog commands fail with generic strings. Decide
  the surfacing: a typed `storageUnavailable` error variant plus a persistent
  UI banner, or a startup-blocking dialog.
- **WebSocket subscription churn** — every universe refresh unsubscribes and
  resubscribes all candle streams. Fix is a pure set-diff of (coin, interval)
  subscriptions; logic is unit-testable, but the behavior change needs one
  live session to validate against Hyperliquid's subscription limits.
- **`disconnect()` status hole** — `HyperliquidAdapter::disconnect` aborts the
  task but leaves `ConnectionState::Live` and skips
  `mark_streamed_for_reconciliation`. No caller exists today; fixing properly
  wants a `Disconnected` variant in the state model, which touches the UI
  status contract.
- **Dialog chrome / pill class extraction** — repeated dialog scaffolding and
  status-pill class strings across components. Mechanical, but purely visual
  and uncovered by tests; extract when next touching those components.

## Deferred with cause

- **Storage insert batching** — batching candle inserts would replace
  compile-time `sqlx::query!` macros with runtime query building. Compile-time
  checking wins until write volume actually hurts.
- **Rate-limiter fairness queue** — the REST limiter is first-come; a fairness
  queue is design work. Revisit with the alerting milestone, which changes the
  request mix anyway.
- **`Chart.svelte` decomposition** — the component is large, but the
  validation-marker UI is still moving; decomposing now would churn twice.
- **`integrity_check` policy** — SQLite integrity is checked at startup but
  the failure path only logs. Needs a policy (rebuild? refuse? warn?) before
  code.
- **Legacy string IPC errors** — older commands return `String` errors; new
  commands are born with typed serde-tagged errors. Convert opportunistically
  when a command is next touched, not wholesale.
- **Rejected as speculative** — signals→hl-feed dependency inversion,
  a `hydrate.rs` extraction, `candle_fetch(SeriesHealth)` parameter object:
  reviewed twice, judged structure-for-structure's-sake. Do not resurrect
  without a concrete driver.

## Planned follow-up work

- **Stable VWAP/OBV semantics** — decide between rolling, session-based, or
  explicitly anchored calculations; only then re-enable both for signals
  (`supports_signals` currently excludes them as anchor-dependent).
- **Asynchronous integration tests** — cancellation arriving during provider
  hydration; a Tauri channel closing mid-run; sequential validations and
  active-slot handoff.
- **Entry-point discoverability** — add "Validate historically" from the
  signal editor with a market selector; Browse stays the chart-focused
  workflow.

## Open exchange questions (need live probes)

- Does Hyperliquid guarantee boundary-frame ordering between the closing final
  candle frame and the first frame of the next bar?
- Is `candleSnapshot`'s `endTime` inclusive of the bar opening at that instant?
- Are zero-volume bars emitted as candles or omitted (gap semantics)?

## Verified non-issues

- `channel-client.ts` pending-cancellation leak on the protocol-error path:
  every session-id assignment flushes the pending cancellation first
  (`handleEvent`, `reconcileId`), and `rejectStart` resolves it on start
  failure; the mismatch branch is only reachable with the pending already
  flushed.
- `spawn_sync` dropping its `JoinHandle` in `lib.rs` setup: tokio detaches on
  drop; the task keeps running.
