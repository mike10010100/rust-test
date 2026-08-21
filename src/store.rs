//! Storage abstraction and in-memory implementation for jobs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

use crate::error::{Result, SchedulerError};
use crate::job::{JobId, JobMetadata, JobSchedule, JobStatus};

/// Trait defining storage operations for job metadata.
/// This allows plugging in database stores (e.g., `SQLite`, `Redis`) in the future.
pub trait JobStore: Send + Sync + 'static {
    /// Inserts a new job into the store.
    fn insert(&self, job: JobMetadata) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Retrieves metadata for a specific job.
    fn get(
        &self,
        id: JobId,
    ) -> impl std::future::Future<Output = Result<Option<JobMetadata>>> + Send;

    /// Lists all jobs currently in the store.
    fn list_all(&self) -> impl std::future::Future<Output = Result<Vec<JobMetadata>>> + Send;

    /// Queries which jobs are pending execution and ready to run.
    fn get_runnable_jobs(&self) -> impl std::future::Future<Output = Result<Vec<JobId>>> + Send;

    /// Transitions a job to the `Running` state and records start metadata.
    /// Returns the updated metadata.
    fn start_run(&self, id: JobId)
    -> impl std::future::Future<Output = Result<JobMetadata>> + Send;

    /// Transitions a job from `Running` to either `Completed` or `Failed`.
    /// For periodic jobs, schedules the next execution time.
    /// Returns the updated metadata.
    fn complete_run(
        &self,
        id: JobId,
        result: Result<(), String>,
    ) -> impl std::future::Future<Output = Result<JobMetadata>> + Send;
}

/// An in-memory, thread-safe implementation of `JobStore`.
#[derive(Debug, Default, Clone)]
pub struct InMemoryJobStore {
    jobs: Arc<RwLock<HashMap<JobId, JobMetadata>>>,
}

impl InMemoryJobStore {
    /// Creates a new, empty `InMemoryJobStore`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl JobStore for InMemoryJobStore {
    async fn insert(&self, job: JobMetadata) -> Result<()> {
        let mut jobs = self.jobs.write().await;
        if jobs.contains_key(&job.id) {
            return Err(SchedulerError::DuplicateJob(job.id));
        }
        log::trace!("Stored new job metadata for job {}", job.id);
        jobs.insert(job.id, job);
        drop(jobs);
        Ok(())
    }

    async fn get(&self, id: JobId) -> Result<Option<JobMetadata>> {
        let jobs = self.jobs.read().await;
        let result = jobs.get(&id).cloned();
        drop(jobs);
        Ok(result)
    }

    async fn list_all(&self) -> Result<Vec<JobMetadata>> {
        let jobs = self.jobs.read().await;
        let result = jobs.values().cloned().collect();
        drop(jobs);
        Ok(result)
    }

    async fn get_runnable_jobs(&self) -> Result<Vec<JobId>> {
        let jobs = self.jobs.read().await;
        let now = Instant::now();
        let runnable = jobs
            .values()
            .filter(|job| {
                if job.status != JobStatus::Pending {
                    return false;
                }
                job.next_run_time
                    .is_some_and(|scheduled_time| scheduled_time <= now)
            })
            .map(|job| job.id)
            .collect();
        drop(jobs);
        Ok(runnable)
    }

    async fn start_run(&self, id: JobId) -> Result<JobMetadata> {
        let mut jobs = self.jobs.write().await;
        let job = jobs.get_mut(&id).ok_or(SchedulerError::JobNotFound(id))?;

        job.status = JobStatus::Running;
        job.last_run_start = Some(Instant::now());
        job.run_count += 1;
        job.next_run_time = None; // Reset until run completes

        let result = job.clone();
        drop(jobs);
        Ok(result)
    }

    async fn complete_run(&self, id: JobId, result: Result<(), String>) -> Result<JobMetadata> {
        let mut jobs = self.jobs.write().await;
        let job = jobs.get_mut(&id).ok_or(SchedulerError::JobNotFound(id))?;

        match result {
            Ok(()) => {
                job.status = JobStatus::Completed;
            }
            Err(err) => {
                job.status = JobStatus::Failed(err);
            }
        }

        // If the job is periodic, set it back to Pending and schedule the next run
        if let JobSchedule::Interval(duration) = job.schedule {
            job.status = JobStatus::Pending;
            let base_time = job.last_run_start.unwrap_or_else(Instant::now);
            job.next_run_time = Some(base_time + duration);
        }

        let result_job = job.clone();
        drop(jobs);
        Ok(result_job)
    }
}
