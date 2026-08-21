# 🤖 Agent Coding & Engineering Handover Guide

Welcome, Agent! This document is designed specifically for AI coding assistants (Antigravity, Cursor, Claude, Copilot, etc.) interacting with this codebase.

---

## 🎯 Repository Purpose & Agent Context

**This repository is an intentional reference blueprint and proof-of-concept for zero-crash, production-grade Rust systems.** 

While the codebase implements a concrete **Async Task Scheduler & Token-Bucket Rate Limiter**, its primary reason for existing is to demonstrate, benchmark, and preserve tier-1 Rust stability, defensive concurrency, and comprehensive QA toolchain integration.

When working in this repository:
- Treat every pattern as a strict standard, not an incidental implementation detail.
- Never lower quality gates, weaken lint rules, or bypass defensive error handling for convenience.
- Any new features, modules, or refactors must adhere to the same uncompromising resilience standard.

---

## 🛡️ Core Non-Negotiable Invariants

To preserve the extreme quality of this project, you **must** strictly adhere to the following rules:

### 1. Zero Unsafe Code
The crate root ([`src/lib.rs`](src/lib.rs)) enforces `#![forbid(unsafe_code)]`. Never attempt to use `unsafe`, weaken this attribute, or introduce dependencies that circumvent compiler safety guarantees.

### 2. Zero Production Panics
- **Banned in production**: `.unwrap()`, `.expect()`, `panic!`, `todo!`, `unimplemented!`.
- All fallible operations must return a strongly typed `Result<T, SchedulerError>` using variants defined in [`src/error.rs`](src/error.rs).
- Use `?`, `match`, or `if let` to bubble errors to callers.

### 3. Strict Linter Compliance
All targets must compile with zero warnings under `clippy::all`, `clippy::pedantic`, and `clippy::nursery` rules.

### 4. Defensive Concurrency & Time
- **Clock-Warp Safety**: Always compute elapsed time using `now.saturating_duration_since(earlier)`. Never use raw `.duration_since()` as monotonic clocks can jump backwards under VM or NTP syncs.
- **Drift-Free Scheduling**: Recurring tasks must calculate next runs relative to the previous anchor timestamp, not `Instant::now()`.
- **Async Mutex Guard Drops**: When holding a `tokio::sync::Mutex` or `RwLock`, always `drop(guard)` **before** any `.await`, `sleep()`, or long I/O to prevent cooperative task starvation.
- **Task Leak Prevention**: Always attach an explicit `AbortHandle` to long-running tasks wrapped in `tokio::time::timeout`. Dropping a `JoinHandle` on timeout does not abort background work.
- **Panic Boundaries**: Worker tasks executing foreign or user-supplied closures must wrap execution in `std::panic::AssertUnwindSafe(...).catch_unwind()`.

### 5. Zero-Cost Future Desugaring
Traits with async methods (like [`JobStore`](src/store.rs#L13)) should use manual return-position `impl Future<Output = ...> + Send` desugaring rather than heap-allocating `#[async_trait]` macros.

---

## 🏗️ Architecture Quick Reference

| Component | File | Responsibility |
| :--- | :--- | :--- |
| **`Scheduler`** | [`src/scheduler.rs`](src/scheduler.rs#L108) | Cloneable, thread-safe user handle. Registers metadata, stores tasks, notifies runner loop. |
| **`SchedulerBuilder`** | [`src/scheduler.rs`](src/scheduler.rs#L34) | Fluent, type-safe builder for configuring and constructing schedulers. |
| **`SchedulerRunner`** | [`src/scheduler.rs`](src/scheduler.rs#L117) | Background event loop. Manages `JoinSet` concurrency limits, rate limiting, and panic capture. |
| **`JobStore`** | [`src/store.rs`](src/store.rs#L13) | Zero-overhead abstract storage trait. Implemented as [`InMemoryJobStore`](src/store.rs#L46). |
| **`RateLimiter`** | [`src/limiter.rs`](src/limiter.rs#L44) | Asynchronous token-bucket rate limiter with lock-drop safety and `try_new`. |
| **`SchedulerError`** | [`src/error.rs`](src/error.rs#L10) | Comprehensive domain error enum powered by `thiserror`. |

---

## 🧪 Testing & Verification Standard for Agents

Whenever you introduce a new feature or modify existing logic:
1. **Unit & Edge-Case Tests**: Add corresponding test cases in [`tests/`](tests/) covering both success paths and failure injection paths (e.g., using `FailingJobStore`).
2. **Doc-Tests**: All public functions and structs must include runnable doc-tests (`cargo test --doc`).
3. **Property Tests**: If manipulating time, intervals, or state transformations, add a `proptest!` block in [`tests/property_tests.rs`](tests/property_tests.rs).
4. **Mutation Testing**: Ensure any logic assertions are tight enough that `cargo mutants` cannot introduce undetected mutations.

---

## ⚡ Mandatory Pre-Completion Checklist

Before reporting your work as done, you **must execute and pass every step** of this pipeline:

```bash
# 1. Check code formatting
cargo fmt --all -- --check

# 2. Check strict clippy rules (must have 0 warnings)
cargo clippy --all-targets --all-features -- -D warnings

# 3. Run all tests
cargo test

# 4. Run all documentation tests
cargo test --doc

# 5. (Optional / recommended if available) Isolated process tests
cargo nextest run

# 6. Dependency security & policy scan
cargo deny check
```

---

## 📚 Related Documentation

- **[README.md](README.md)**: High-level overview and pattern summary table.
- **[BEST_PRACTICES.md](BEST_PRACTICES.md)**: Exhaustive architectural deep-dive into each stability design pattern.
- **[TOOLING.md](TOOLING.md)**: QA toolchain setup and CI workflow guide.
