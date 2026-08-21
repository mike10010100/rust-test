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

//! # Async Task Scheduler & Production Stability Blueprint
//!
//! A resilient, highly concurrent, rate-limited task scheduler built in Rust.
//! Enforces zero-panic task boundaries, defensive time math, and clean graceful shutdown.
//!
//! ## Example
//!
//! ```rust
//! use rust_best_practices::scheduler::SchedulerBuilder;
//! use rust_best_practices::job::JobSchedule;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let (scheduler, runner) = SchedulerBuilder::new()
//!     .max_concurrent_jobs(4)
//!     .build();
//!
//! scheduler.start(runner).await?;
//!
//! let job_id = scheduler
//!     .add_job(JobSchedule::Immediate, || async {
//!         println!("Executing task safely!");
//!         Ok(())
//!     })
//!     .await?;
//!
//! scheduler.shutdown().await?;
//! # Ok(())
//! # }
//! ```

pub mod error;
pub mod job;
pub mod limiter;
pub mod scheduler;
pub mod store;

pub use error::{Result, SchedulerError};
pub use job::{JobId, JobMetadata, JobSchedule, JobStatus, Task};
pub use limiter::RateLimiter;
pub use scheduler::{Scheduler, SchedulerBuilder, SchedulerRunner};
pub use store::{InMemoryJobStore, JobStore};
