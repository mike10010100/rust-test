#![deny(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    missing_docs,
    rust_2018_idioms
)]
#![forbid(unsafe_code)]
// Allow documentation of errors to be simpler
#![allow(clippy::missing_errors_doc)]

//! # Async Task Scheduler
//!
//! A resilient, highly concurrent, rate-limited task scheduler built in Rust.
//! Enforces zero-panic task boundaries and clean graceful shutdown.

pub mod error;
pub mod job;
pub mod limiter;
pub mod scheduler;
pub mod store;

pub use error::{Result, SchedulerError};
pub use job::{JobId, JobMetadata, JobSchedule, JobStatus, Task};
pub use limiter::RateLimiter;
pub use scheduler::{Scheduler, SchedulerRunner};
pub use store::{InMemoryJobStore, JobStore};
