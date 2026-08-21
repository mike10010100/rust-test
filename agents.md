# Agent Coding & Handover Guide

Welcome, Agent! This document is designed for AI coding assistants (like yourself) working on this codebase. It provides context on the system architecture, code style mandates, verification commands, and guidelines for adding new features.

---

## 1. Core Principles (Non-Negotiable)

To maintain the extreme quality and stability of this project, you must adhere to the following rules:

1. **Zero Unsafe Code**: The crate is compiled with `#![forbid(unsafe_code)]` in the crate root ([src/lib.rs](src/lib.rs)). Never attempt to bypass this.
2. **Zero Production Panics**: Never use `.unwrap()`, `.expect()`, `panic!`, `todo!`, or `unimplemented!` in production modules. All errors must use typed variants of `SchedulerError` in [src/error.rs](src/error.rs) and bubble up as a `Result`.
3. **Strict Lints**: All target builds must compile clean with zero Clippy warnings under the pedantic and nursery rules defined in [src/lib.rs](src/lib.rs).
4. **Concurrency Safety**: Always verify that shared state is protected by thread-safe primitives (like `tokio::sync::RwLock` or `tokio::sync::Mutex`) and that lock guards are dropped early (`drop(guard)`) before long async yields (such as `.await`) to prevent deadlocks and resource contention.

---

## 2. Architecture Quick Reference

* **[`Scheduler`](src/scheduler.rs#L18)**: The cloneable user-facing handle. It registers job metadata into the store, inserts the executable task closure into the registry, and signals the runner loop.
* **[`SchedulerRunner`](src/scheduler.rs#L28)**: The background driver task. It polls for ready jobs, manages concurrency limits using `tokio::task::JoinSet`, applies `RateLimiter` limits, and captures user-task panics using `catch_unwind`.
* **[`JobStore`](src/store.rs#L13)**: An abstract storage trait using desugared async signatures returning `Send` futures to prevent compiler thread-safety errors. Implemented as `InMemoryJobStore`.
* **[`RateLimiter`](src/limiter.rs#L24)**: A thread-safe, asynchronous token-bucket rate limiter.

---

## 3. Workflow & Verification Commands

Before concluding any work, you **must** execute:
* **Verify Code Style**: `cargo fmt --all -- --check`
* **Verify Lints**: `cargo clippy --all-targets --all-features -- -D warnings`
* **Verify Tests**: `cargo test` (or `cargo nextest run`)

---

## 4. Reference Documentation

For detailed guides on QA tooling configurations (such as cargo-deny, cargo-audit, cargo-llvm-cov, and cargo-mutants), refer to **[TOOLING.md](TOOLING.md)**.

