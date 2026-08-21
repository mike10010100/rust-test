//! The core task scheduler implementation.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::error::{Result, SchedulerError};
use crate::job::{JobId, JobMetadata, JobSchedule, JobStatus, RegisteredTask, Task};
use crate::limiter::RateLimiter;
use crate::store::{InMemoryJobStore, JobStore};

/// A handle to the scheduler runner task.
pub type RunnerHandle = tokio::task::JoinHandle<Result<(), SchedulerError>>;

/// A builder for constructing and configuring a [`Scheduler`] and its [`SchedulerRunner`].
///
/// # Examples
///
/// ```rust
/// use rust_best_practices::scheduler::SchedulerBuilder;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let (scheduler, _runner) = SchedulerBuilder::new()
///     .max_concurrent_jobs(8)
///     .rate_limiter(10, 5.0)?
///     .build();
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct SchedulerBuilder<S = InMemoryJobStore> {
    store: S,
    max_concurrent_jobs: usize,
    rate_limiter: Option<RateLimiter>,
}

impl SchedulerBuilder<InMemoryJobStore> {
    /// Creates a new default `SchedulerBuilder` with an [`InMemoryJobStore`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            store: InMemoryJobStore::new(),
            max_concurrent_jobs: 16,
            rate_limiter: None,
        }
    }
}

impl Default for SchedulerBuilder<InMemoryJobStore> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: JobStore + Clone> SchedulerBuilder<S> {
    /// Configures the storage backend for the scheduler.
    #[must_use]
    pub fn with_store<NewStore: JobStore + Clone>(
        self,
        store: NewStore,
    ) -> SchedulerBuilder<NewStore> {
        SchedulerBuilder {
            store,
            max_concurrent_jobs: self.max_concurrent_jobs,
            rate_limiter: self.rate_limiter,
        }
    }

    /// Sets the maximum number of concurrent jobs that can be executed simultaneously.
    #[must_use]
    pub const fn max_concurrent_jobs(mut self, max: usize) -> Self {
        self.max_concurrent_jobs = max;
        self
    }

    /// Attaches an existing [`RateLimiter`] to the scheduler.
    #[must_use]
    pub fn with_rate_limiter(mut self, limiter: RateLimiter) -> Self {
        self.rate_limiter = Some(limiter);
        self
    }

    /// Configures rate limiting parameters directly.
    ///
    /// # Errors
    /// Returns `SchedulerError::InvalidConfig` if rate limiter parameters are invalid.
    pub fn rate_limiter(mut self, capacity: usize, refill_rate: f64) -> Result<Self> {
        let limiter = RateLimiter::try_new(capacity, refill_rate)?;
        self.rate_limiter = Some(limiter);
        Ok(self)
    }

    /// Builds the configured [`Scheduler`] and [`SchedulerRunner`].
    #[must_use]
    pub fn build(self) -> (Scheduler<S>, SchedulerRunner<S>) {
        Scheduler::new(self.store, self.max_concurrent_jobs, self.rate_limiter)
    }
}

/// A thread-safe handle to the task scheduler.
/// Clones of this handle share the same underlying job store and task registry.
#[derive(Clone)]
pub struct Scheduler<S = InMemoryJobStore> {
    store: S,
    tasks: Arc<RwLock<HashMap<JobId, RegisteredTask>>>,
    wakeup_tx: mpsc::Sender<()>,
    shutdown_token: CancellationToken,
    runner_handle: Arc<Mutex<Option<RunnerHandle>>>,
}

/// The execution runner for the scheduler, containing the event loop.
/// This should be started in a background task.
pub struct SchedulerRunner<S> {
    store: S,
    tasks: Arc<RwLock<HashMap<JobId, RegisteredTask>>>,
    wakeup_rx: mpsc::Receiver<()>,
    shutdown_token: CancellationToken,
    max_concurrent_jobs: usize,
    rate_limiter: Option<RateLimiter>,
}

impl<S: JobStore + Clone> Scheduler<S> {
    /// Creates a new `Scheduler` and its associated `SchedulerRunner`.
    ///
    /// For a fluent configuration builder, see [`SchedulerBuilder`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rust_best_practices::scheduler::Scheduler;
    /// use rust_best_practices::store::InMemoryJobStore;
    ///
    /// let store = InMemoryJobStore::new();
    /// let (scheduler, _runner) = Scheduler::new(store, 4, None);
    /// ```
    #[must_use]
    pub fn new(
        store: S,
        max_concurrent_jobs: usize,
        rate_limiter: Option<RateLimiter>,
    ) -> (Self, SchedulerRunner<S>) {
        let (wakeup_tx, wakeup_rx) = mpsc::channel(100);
        let shutdown_token = CancellationToken::new();
        let tasks = Arc::new(RwLock::new(HashMap::new()));

        let scheduler = Self {
            store: store.clone(),
            tasks: tasks.clone(),
            wakeup_tx,
            shutdown_token: shutdown_token.clone(),
            runner_handle: Arc::new(Mutex::new(None)),
        };

        let runner = SchedulerRunner {
            store,
            tasks,
            wakeup_rx,
            shutdown_token,
            max_concurrent_jobs,
            rate_limiter,
        };

        (scheduler, runner)
    }

    /// Spawns the scheduler runner in a background Tokio task.
    ///
    /// # Errors
    /// Returns an error if the scheduler is already running.
    ///
    /// # Cancellation Safety
    /// This method is **cancellation safe**.
    pub async fn start(&self, mut runner: SchedulerRunner<S>) -> Result<()> {
        let mut handle_guard = self.runner_handle.lock().await;
        if handle_guard.is_some() {
            return Err(SchedulerError::StoreError(
                "Scheduler is already running".to_string(),
            ));
        }

        let handle = tokio::spawn(async move { runner.run().await });
        *handle_guard = Some(handle);
        drop(handle_guard);
        Ok(())
    }

    /// Registers a new job and schedule with the scheduler.
    ///
    /// # Errors
    /// Returns an error if the store fails to register the job.
    ///
    /// # Cancellation Safety
    /// This method is **cancellation safe**.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rust_best_practices::scheduler::Scheduler;
    /// use rust_best_practices::job::JobSchedule;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let (scheduler, _runner) = Scheduler::new_in_memory(2, None);
    /// let job_id = scheduler
    ///     .add_job(JobSchedule::Immediate, || async { Ok(()) })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn add_job<T>(&self, schedule: JobSchedule, task: T) -> Result<JobId>
    where
        T: Task,
    {
        let id = JobId::new();
        let metadata = JobMetadata::new(id, schedule);

        log::debug!("Adding job {id} to scheduler");

        // Save metadata to store
        self.store.insert(metadata).await?;

        // Register executable logic
        let mut tasks = self.tasks.write().await;
        tasks.insert(id, Arc::new(task));
        drop(tasks);

        // Notify the scheduler loop to recalculate schedules
        let _ = self.wakeup_tx.send(()).await;

        Ok(id)
    }

    /// Queries the current status of a job.
    ///
    /// # Errors
    /// Returns an error if the store cannot be queried.
    ///
    /// # Cancellation Safety
    /// This method is **cancellation safe**.
    pub async fn get_job_status(&self, id: JobId) -> Result<Option<JobStatus>> {
        if let Some(metadata) = self.store.get(id).await? {
            Ok(Some(metadata.status))
        } else {
            Ok(None)
        }
    }

    /// Queries full metadata of a job.
    ///
    /// # Errors
    /// Returns an error if the store cannot be queried.
    ///
    /// # Cancellation Safety
    /// This method is **cancellation safe**.
    pub async fn get_job_metadata(&self, id: JobId) -> Result<Option<JobMetadata>> {
        self.store.get(id).await
    }

    /// Initiates a graceful shutdown of the scheduler and awaits completion of running jobs.
    ///
    /// # Errors
    /// Returns an error if the runner task failed during shutdown.
    ///
    /// # Cancellation Safety
    /// This method is **cancellation safe**.
    pub async fn shutdown(&self) -> Result<()> {
        log::info!("Initiating graceful scheduler shutdown");
        self.shutdown_token.cancel();

        let mut runner_guard = self.runner_handle.lock().await;
        let handle = runner_guard.take();
        drop(runner_guard);

        if let Some(h) = handle {
            match h.await {
                Ok(res) => res?,
                Err(join_err) => {
                    return Err(SchedulerError::ChannelError(format!(
                        "Scheduler runner task failed to join: {join_err}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Initiates a graceful shutdown of the scheduler and awaits completion up to the specified timeout.
    /// If the timeout expires, any running jobs are aborted.
    ///
    /// # Errors
    /// Returns `SchedulerError::Timeout` if the timeout expires, or other scheduling errors.
    ///
    /// # Cancellation Safety
    /// This method is **cancellation safe**.
    pub async fn shutdown_with_timeout(&self, timeout: Duration) -> Result<()> {
        log::info!("Initiating graceful scheduler shutdown with timeout {timeout:?}");
        self.shutdown_token.cancel();

        let mut runner_guard = self.runner_handle.lock().await;
        let handle = runner_guard.take();
        drop(runner_guard);

        if let Some(h) = handle {
            let abort_handle = h.abort_handle();
            match tokio::time::timeout(timeout, h).await {
                Ok(Ok(res)) => res?,
                Ok(Err(join_err)) => {
                    return Err(SchedulerError::ChannelError(format!(
                        "Scheduler runner task failed to join: {join_err}"
                    )));
                }
                Err(_) => {
                    log::warn!("Shutdown timed out; aborting background runner");
                    abort_handle.abort();
                    return Err(SchedulerError::Timeout);
                }
            }
        }

        Ok(())
    }
}

impl Scheduler<InMemoryJobStore> {
    /// Helper to create a scheduler backed by the standard `InMemoryJobStore`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rust_best_practices::scheduler::Scheduler;
    ///
    /// let (scheduler, _runner) = Scheduler::new_in_memory(4, None);
    /// ```
    #[must_use]
    pub fn new_in_memory(
        max_concurrent_jobs: usize,
        rate_limiter: Option<RateLimiter>,
    ) -> (Self, SchedulerRunner<InMemoryJobStore>) {
        let store = InMemoryJobStore::new();
        Self::new(store, max_concurrent_jobs, rate_limiter)
    }
}

impl<S: JobStore + Clone> SchedulerRunner<S> {
    /// Runs the core scheduler event loop.
    ///
    /// # Errors
    /// Returns an error if database access or loop signaling fails.
    pub async fn run(&mut self) -> Result<()> {
        let mut join_set = JoinSet::new();

        loop {
            if self.shutdown_token.is_cancelled() {
                break;
            }

            // Dispatch ready jobs within concurrency/rate-limit bounds
            self.dispatch_jobs(&mut join_set).await?;

            // Determine when the next scheduled job is ready
            let next_wakeup = self.get_next_wakeup_time().await?;
            let sleep_duration = next_wakeup.map_or_else(
                || Duration::from_secs(3600),
                |time| time.saturating_duration_since(Instant::now()),
            );

            let sleep_fut = tokio::time::sleep(sleep_duration);

            tokio::select! {
                () = self.shutdown_token.cancelled() => {
                    break;
                }
                () = sleep_fut => {
                    // Check for runnable jobs after sleeping
                }
                _ = self.wakeup_rx.recv() => {
                    // New job was added or requested to run
                }
                Some(res) = join_set.join_next(), if !join_set.is_empty() => {
                    // Task finished executing
                    if let Err(err) = res && err.is_panic() {
                        // The inner task future catches panic, but if tokio worker itself fails:
                        log::error!("Tokio task worker panic: {err:?}");
                    }
                }
            }
        }

        // Graceful shutdown phase: stop accepting new jobs, finish current ones
        while !join_set.is_empty() {
            let _ = join_set.join_next().await;
        }

        Ok(())
    }

    /// Dispatches all runnable jobs up to concurrency and rate limit constraints.
    async fn dispatch_jobs(&self, join_set: &mut JoinSet<()>) -> Result<()> {
        loop {
            // Check concurrency limit
            if join_set.len() >= self.max_concurrent_jobs {
                break;
            }

            // Check rate limiter
            if let Some(ref limiter) = self.rate_limiter
                && !limiter.try_acquire().await
            {
                // Rate limit reached. Do not dispatch further in this tick.
                break;
            }

            // Fetch ready jobs from storage
            let runnable = self.store.get_runnable_jobs().await?;
            if runnable.is_empty() {
                break;
            }

            let job_id = runnable[0];

            // Resolve the task implementation
            let tasks_guard = self.tasks.read().await;
            let Some(task) = tasks_guard.get(&job_id).cloned() else {
                // Registered task is missing, record failure
                drop(tasks_guard);
                let _ = self
                    .store
                    .complete_run(
                        job_id,
                        Err("Registered task implementation not found".to_string()),
                    )
                    .await?;
                continue;
            };
            drop(tasks_guard);

            // Transition job state to Running
            self.store.start_run(job_id).await?;
            log::debug!("Dispatched job {job_id} to worker task pool");

            // Spawn execution with panic-catching wrapper
            let store = self.store.clone();
            join_set.spawn(async move {
                use futures_util::FutureExt;
                let catch_fut = std::panic::AssertUnwindSafe(task.execute()).catch_unwind();

                let outcome = match catch_fut.await {
                    Ok(Ok(())) => {
                        log::info!("Task {job_id} completed successfully");
                        Ok(())
                    }
                    Ok(Err(err_msg)) => {
                        log::warn!("Task {job_id} failed with error: {err_msg}");
                        Err(err_msg)
                    }
                    Err(panic_payload) => {
                        let msg = panic_payload
                            .downcast_ref::<&str>()
                            .map(|s| (*s).to_string())
                            .or_else(|| panic_payload.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "Unknown panic".to_string());
                        log::error!("Task {job_id} panicked: {msg}");
                        Err(format!("Task panicked: {msg}"))
                    }
                };

                // Record final execution state
                let _ = store.complete_run(job_id, outcome).await;
            });
        }

        Ok(())
    }

    /// Finds the earliest next execution time among all pending scheduled jobs.
    async fn get_next_wakeup_time(&self) -> Result<Option<Instant>> {
        let jobs = self.store.list_all().await?;
        let next = jobs
            .iter()
            .filter(|job| job.status == JobStatus::Pending)
            .filter_map(|job| job.next_run_time)
            .min();
        Ok(next)
    }
}
