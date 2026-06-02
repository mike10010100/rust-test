//! Definition of Jobs, Schedules, and Tasks.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

/// Unique identifier for a Job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JobId(Uuid);

impl JobId {
    /// Generates a new unique `JobId`.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The scheduling policy for a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobSchedule {
    /// Run the job immediately once.
    Immediate,
    /// Run the job once after the specified delay.
    Delayed(std::time::Duration),
    /// Run the job repeatedly at the specified interval.
    Interval(std::time::Duration),
}

/// The execution status of a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStatus {
    /// The job is waiting to be executed or scheduled.
    Pending,
    /// The job is currently running in a worker task.
    Running,
    /// The job completed successfully.
    Completed,
    /// The job failed with the specified error message.
    Failed(String),
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "Pending"),
            Self::Running => write!(f, "Running"),
            Self::Completed => write!(f, "Completed"),
            Self::Failed(err) => write!(f, "Failed: {err}"),
        }
    }
}

/// Metadata describing the state of a job.
#[derive(Debug, Clone)]
pub struct JobMetadata {
    /// Unique identifier for the job.
    pub id: JobId,
    /// The scheduling policy of the job.
    pub schedule: JobSchedule,
    /// The current execution status.
    pub status: JobStatus,
    /// How many times the job has been executed.
    pub run_count: usize,
    /// The start time of the last execution, if any.
    pub last_run_start: Option<Instant>,
    /// The scheduled time for the next execution, if any.
    pub next_run_time: Option<Instant>,
}

impl JobMetadata {
    /// Creates a new `JobMetadata` instance.
    #[must_use]
    pub fn new(id: JobId, schedule: JobSchedule) -> Self {
        let next_run_time = match &schedule {
            JobSchedule::Immediate => Some(Instant::now()),
            JobSchedule::Delayed(delay) => Some(Instant::now() + *delay),
            JobSchedule::Interval(interval) => Some(Instant::now() + *interval),
        };

        Self {
            id,
            schedule,
            status: JobStatus::Pending,
            run_count: 0,
            last_run_start: None,
            next_run_time,
        }
    }
}

/// A trait representing the executable unit of a job.
pub trait Task: Send + Sync + 'static {
    /// Executes the task asynchronously.
    /// Returns `Ok(())` on success, or `Err(String)` on failure.
    fn execute(&self) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
}

// Blanket implementation for any async function/closure that returns `Result<(), String>`.
impl<F, Fut> Task for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), String>> + Send + 'static,
{
    fn execute(&self) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
        Box::pin((self)())
    }
}

/// A handle to a registered task, wrapping it in an Arc.
pub type RegisteredTask = Arc<dyn Task>;
