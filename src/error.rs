//! Error types for the task scheduler.

use crate::job::JobId;
use thiserror::Error;

/// Result type alias for scheduler operations.
pub type Result<T, E = SchedulerError> = std::result::Result<T, E>;

/// Error variants that can occur in the task scheduler.
#[derive(Debug, Error)]
pub enum SchedulerError {
    /// The specified job was not found in the storage.
    #[error("Job not found: {0}")]
    JobNotFound(JobId),

    /// A job with the same ID already exists.
    #[error("Duplicate job ID: {0}")]
    DuplicateJob(JobId),

    /// An error occurred in the underlying job store.
    #[error("Storage error: {0}")]
    StoreError(String),

    /// Operation failed because the scheduler has been shut down.
    #[error("Scheduler has been shut down")]
    SchedulerShutdown,

    /// A job panicked during execution.
    #[error("Job panicked: {0}")]
    TaskPanic(String),

    /// A job execution timed out.
    #[error("Job execution timed out")]
    Timeout,

    /// Invalid schedule specification.
    #[error("Invalid schedule: {0}")]
    InvalidSchedule(String),

    /// An internal channel communication error.
    #[error("Internal communication channel error: {0}")]
    ChannelError(String),

    /// Invalid configuration parameter.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}
