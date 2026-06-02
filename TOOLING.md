# Crate Quality & Stability Tooling Guide

This document outlines the tools, lints, and testing utilities integrated into this project to ensure production-grade stability, security, and correctness.

---

## 1. Integrated Quality Gates

The following tools and configurations are active in the codebase and run automatically in our CI pipeline:

### Compile-Time Guards & Lints
* **Forbid Unsafe**: Declared via `#![forbid(unsafe_code)]` in the crate root. The compiler will reject any attempt to compile `unsafe` blocks, ensuring 100% memory safety.
* **Strict Clippy Lints**: We deny all warnings, pedantic, and nursery lints. In particular, we forbid panicking constructs (`unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`).

### Dependency Governance & Security
* **[`cargo-deny`](deny.toml)**: Validates licenses, bans duplicate crate versions to avoid binary bloat, and blocks unauthorized registries.
* **`cargo-audit`**: Automatically scans our `Cargo.lock` file against the RustSec Advisory Database to block compilation if any of our dependencies have active security advisories (CVEs) or are yanked.

### Testing & Verification
* **[`cargo-nextest`](.config/nextest.toml)**: A modern, process-isolated test runner. Configured with a `retries = 2` profile to handle minor async scheduling delays and keep CI pipelines stable under heavy load.
* **[`proptest`](tests/property_tests.rs)**: Property-based testing framework. Generates 500+ random variations of input durations and schedules to verify that type conversions and job scheduling boundaries are mathematically sound.
* **[`cargo-mutants`](mutants.toml)**: Mutation testing framework. Automatically injects logical bugs into our source code to verify that our tests actually fail when code is modified. Configured to run using `nextest` and ignore test files.
* **`cargo-llvm-cov`**: Generates source-based line and branch coverage using LLVM compiler instrumentation.

---

## 2. Running the Tools Locally

Use these commands to run quality checks on your local machine:

| Check | Command | Purpose |
| :--- | :--- | :--- |
| **Lints & Style** | `cargo clippy --all-targets --all-features -- -D warnings` | Enforce clean code rules |
| **All Tests** | `cargo test` | Runs unit, integration, and property tests |
| **Resilient Tests** | `cargo nextest run` | Runs tests using nextest runner |
| **Coverage** | `cargo llvm-cov` | Generate code coverage metrics |
| **Vulnerabilities** | `cargo audit` | Scan dependencies for CVE advisories |
| **Governance** | `cargo deny check` | Verify licenses, bans, and sources |
| **Mutation Testing** | `cargo mutants` | Test the strength of your test suite |

---

## 3. Evaluated & Excluded Tools

We evaluated the following tools but chose not to incorporate them into the active quality pipeline for the reasons detailed below:

### `miri` (MIR Interpreter)
* **Rationale for Exclusion**: Miri checks for undefined behavior (UB) and memory aliasing violations. Since we enforce `#![forbid(unsafe_code)]`, the compiler already guarantees safe memory operations. While Miri can catch deadlocks, it runs code thousands of times slower, making it impractical for standard CI/CD loops.
* **When to use**: Run it manually (`cargo miri test`) if unsafe code is ever allowed in the future.

### `cargo-tarpaulin` (Alternative Coverage)
* **Rationale for Exclusion**: Tarpaulin is a line-coverage tool that relies on Linux-specific `ptrace` system calls. This makes it platform-dependent and prone to crashing in Docker containers or containerized CI agents. `cargo-llvm-cov` is preferred as it is cross-platform, faster, and provides LLVM source-level accuracy.

### `cargo-fuzz` (Fuzz Testing)
* **Rationale for Exclusion**: Fuzz testing is designed to feed random byte streams to string or binary parsers to detect boundary panics. Because the task scheduler's APIs are strongly typed (taking structures, durations, and closures) and do not parse raw data inputs, fuzzer mutational algorithms are ineffective. Property testing (`proptest`) provides structured, typed boundary coverage.

### `cargo-semver-checks` (API Version Control)
* **Rationale for Exclusion**: Checks API changes against published crates on crates.io to prevent SemVer breakage. This is essential for public, open-source libraries, but unnecessary for internal application projects or private crates.

### `cargo-udeps` (Unused Dependency Scanner)
* **Rationale for Exclusion**: Finds unused dependencies in `Cargo.toml`. While useful, it requires building on the Rust `nightly` compiler toolchain. We prioritize keeping our CI pipelines anchored to the `stable` toolchain for maximum predictability.

### `cargo-geiger` (Unsafe Scoping)
* **Rationale for Exclusion**: Geiger counts unsafe blocks in a crate and its dependencies. Since unsafe code is forbidden in our own crate, Geiger is redundant; dependency-level vulnerabilities are already caught by `cargo-deny` and `cargo-audit`.
