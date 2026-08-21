use std::time::Duration;
use tokio::time::sleep;

use rust_best_practices::{
    InMemoryJobStore, JobId, JobMetadata, JobSchedule, JobStatus, JobStore, RateLimiter, Scheduler,
    SchedulerError,
};

#[tokio::test]
async fn test_cannot_start_scheduler_twice() {
    let (scheduler, runner) = Scheduler::new_in_memory(2, None);

    // First start should succeed
    scheduler.start(runner).await.unwrap();

    // Create another runner (or use a mock) to try to start again
    let (_, runner2) = Scheduler::new_in_memory(2, None);
    let start_res = scheduler.start(runner2).await;

    match start_res {
        Err(SchedulerError::StoreError(msg)) => {
            assert!(msg.contains("Scheduler is already running"));
        }
        other => panic!("Expected StoreError(\"Scheduler is already running\"), got {other:?}"),
    }

    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_missing_task_implementation() {
    // If a job exists in the JobStore but has no registered task closure in the Scheduler's tasks map,
    // the scheduler should transition the job to Failed status with a descriptive error.
    let store = InMemoryJobStore::new();
    let (scheduler, runner) = Scheduler::new(store.clone(), 2, None);

    // Insert metadata directly into store, bypassing scheduler.add_job (so no task is registered)
    let job_id = JobId::new();
    let metadata = JobMetadata::new(job_id, JobSchedule::Immediate);
    store.insert(metadata).await.unwrap();

    // Start the scheduler
    scheduler.start(runner).await.unwrap();

    // Give it a moment to run the dispatcher loop
    sleep(Duration::from_millis(50)).await;

    // Check status: should be Failed because the task closure was missing
    let status = scheduler.get_job_status(job_id).await.unwrap().unwrap();
    match status {
        JobStatus::Failed(err) => {
            assert!(err.contains("Registered task implementation not found"));
        }
        other => panic!("Expected Failed status, got {other:?}"),
    }

    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_shutdown_without_starting() {
    let (scheduler, _runner) = Scheduler::new_in_memory(2, None);

    // Shutdown without starting should return Ok(()) immediately
    let res = scheduler.shutdown().await;
    assert!(res.is_ok());

    let res_timeout = scheduler
        .shutdown_with_timeout(Duration::from_millis(100))
        .await;
    assert!(res_timeout.is_ok());
}

#[tokio::test]
async fn test_shutdown_with_timeout_succeeds_fast() {
    let (scheduler, runner) = Scheduler::new_in_memory(2, None);
    scheduler.start(runner).await.unwrap();

    // Add a very quick task
    scheduler
        .add_job(JobSchedule::Immediate, || async {
            sleep(Duration::from_millis(5)).await;
            Ok(())
        })
        .await
        .unwrap();

    sleep(Duration::from_millis(10)).await;

    // Shutdown with timeout should succeed immediately since no jobs are running
    let res = scheduler
        .shutdown_with_timeout(Duration::from_secs(1))
        .await;
    assert!(res.is_ok());
}

#[test]
#[should_panic(expected = "Capacity must be greater than zero")]
fn test_rate_limiter_zero_capacity_panics() {
    let _ = RateLimiter::new(0, 1.0);
}

#[test]
#[should_panic(expected = "Refill rate must be positive")]
fn test_rate_limiter_zero_rate_panics() {
    let _ = RateLimiter::new(1, 0.0);
}

#[test]
#[should_panic(expected = "Refill rate must be positive")]
fn test_rate_limiter_negative_rate_panics() {
    let _ = RateLimiter::new(1, -0.5);
}

#[test]
fn test_error_formatting() {
    let job_id = JobId::new();

    assert_eq!(
        format!("{}", SchedulerError::JobNotFound(job_id)),
        format!("Job not found: {job_id}")
    );
    assert_eq!(
        format!("{}", SchedulerError::DuplicateJob(job_id)),
        format!("Duplicate job ID: {job_id}")
    );
    assert_eq!(
        format!("{}", SchedulerError::StoreError("db error".to_string())),
        "Storage error: db error"
    );
    assert_eq!(
        format!("{}", SchedulerError::SchedulerShutdown),
        "Scheduler has been shut down"
    );
    assert_eq!(
        format!("{}", SchedulerError::TaskPanic("panic message".to_string())),
        "Job panicked: panic message"
    );
    assert_eq!(
        format!("{}", SchedulerError::Timeout),
        "Job execution timed out"
    );
    assert_eq!(
        format!(
            "{}",
            SchedulerError::InvalidSchedule("bad cron".to_string())
        ),
        "Invalid schedule: bad cron"
    );
    assert_eq!(
        format!("{}", SchedulerError::ChannelError("closed".to_string())),
        "Internal communication channel error: closed"
    );
}

#[test]
fn test_job_id_and_status_formatting() {
    let job_id = JobId::new();
    // Test JobId formatting: should be a non-empty string representing uuid
    let job_id_str = format!("{job_id}");
    assert!(!job_id_str.is_empty());
    assert_eq!(job_id_str.len(), 36); // standard UUID format is 36 chars

    // Test JobStatus formatting for all variants
    assert_eq!(format!("{}", JobStatus::Pending), "Pending");
    assert_eq!(format!("{}", JobStatus::Running), "Running");
    assert_eq!(format!("{}", JobStatus::Completed), "Completed");
    assert_eq!(
        format!("{}", JobStatus::Failed("error detail".to_string())),
        "Failed: error detail"
    );
}

#[test]
fn test_job_id_default() {
    let job_id = JobId::default();
    assert!(!format!("{job_id}").is_empty());
}

#[tokio::test]
async fn test_get_nonexistent_job_status() {
    let (scheduler, _runner) = Scheduler::new_in_memory(2, None);
    let status = scheduler.get_job_status(JobId::new()).await.unwrap();
    assert!(status.is_none());
}

#[tokio::test]
async fn test_task_returns_error() {
    let (scheduler, runner) = Scheduler::new_in_memory(2, None);
    scheduler.start(runner).await.unwrap();

    let job_id = scheduler
        .add_job(JobSchedule::Immediate, || async {
            Err("custom execution failure".to_string())
        })
        .await
        .unwrap();

    sleep(Duration::from_millis(50)).await;

    let status = scheduler.get_job_status(job_id).await.unwrap().unwrap();
    match status {
        JobStatus::Failed(err) => {
            assert!(err.contains("custom execution failure"));
        }
        other => panic!("Expected Failed status, got {other:?}"),
    }

    scheduler.shutdown().await.unwrap();
}

#[derive(Clone)]
struct PanickingRunnerStore;

impl JobStore for PanickingRunnerStore {
    async fn insert(&self, _job: JobMetadata) -> Result<(), SchedulerError> {
        Ok(())
    }
    async fn get(&self, _id: JobId) -> Result<Option<JobMetadata>, SchedulerError> {
        Ok(None)
    }
    async fn list_all(&self) -> Result<Vec<JobMetadata>, SchedulerError> {
        Ok(vec![])
    }
    async fn get_runnable_jobs(&self) -> Result<Vec<JobId>, SchedulerError> {
        panic!("Simulated runner store panic");
    }
    async fn start_run(&self, _id: JobId) -> Result<JobMetadata, SchedulerError> {
        Err(SchedulerError::StoreError("not implemented".to_string()))
    }
    async fn complete_run(
        &self,
        _id: JobId,
        _result: Result<(), String>,
    ) -> Result<JobMetadata, SchedulerError> {
        Err(SchedulerError::StoreError("not implemented".to_string()))
    }
}

#[tokio::test]
async fn test_runner_panics_on_shutdown() {
    let store = PanickingRunnerStore;
    let (scheduler, runner) = Scheduler::new(store, 2, None);
    scheduler.start(runner).await.unwrap();

    // Wait a brief moment for the runner to execute its loop once and panic
    sleep(Duration::from_millis(30)).await;

    // Shutdown should join the panicked task and return a ChannelError
    let shutdown_res = scheduler.shutdown().await;
    match shutdown_res {
        Err(SchedulerError::ChannelError(msg)) => {
            assert!(msg.contains("failed to join"));
        }
        other => panic!("Expected ChannelError, got {other:?}"),
    }
}

#[tokio::test]
async fn test_runner_panics_on_shutdown_timeout() {
    let store = PanickingRunnerStore;
    let (scheduler, runner) = Scheduler::new(store, 2, None);
    scheduler.start(runner).await.unwrap();

    // Wait a brief moment for the runner to execute its loop once and panic
    sleep(Duration::from_millis(30)).await;

    // Shutdown with timeout should join the panicked task and return a ChannelError
    let shutdown_res = scheduler
        .shutdown_with_timeout(Duration::from_secs(1))
        .await;
    match shutdown_res {
        Err(SchedulerError::ChannelError(msg)) => {
            assert!(msg.contains("failed to join"));
        }
        other => panic!("Expected ChannelError, got {other:?}"),
    }
}

#[derive(Clone)]
struct PanickingCompleteStore {
    inner: InMemoryJobStore,
}

impl PanickingCompleteStore {
    fn new() -> Self {
        Self {
            inner: InMemoryJobStore::new(),
        }
    }
}

impl JobStore for PanickingCompleteStore {
    async fn insert(&self, job: JobMetadata) -> Result<(), SchedulerError> {
        self.inner.insert(job).await
    }
    async fn get(&self, id: JobId) -> Result<Option<JobMetadata>, SchedulerError> {
        self.inner.get(id).await
    }
    async fn list_all(&self) -> Result<Vec<JobMetadata>, SchedulerError> {
        self.inner.list_all().await
    }
    async fn get_runnable_jobs(&self) -> Result<Vec<JobId>, SchedulerError> {
        self.inner.get_runnable_jobs().await
    }
    async fn start_run(&self, id: JobId) -> Result<JobMetadata, SchedulerError> {
        self.inner.start_run(id).await
    }
    async fn complete_run(
        &self,
        _id: JobId,
        _result: Result<(), String>,
    ) -> Result<JobMetadata, SchedulerError> {
        panic!("Simulated complete_run panic");
    }
}

#[tokio::test]
async fn test_spawned_task_panics_on_complete_run() {
    let store = PanickingCompleteStore::new();
    let (scheduler, runner) = Scheduler::new(store, 2, None);
    scheduler.start(runner).await.unwrap();

    // Add a job that runs immediately.
    // When the job completes, the spawned task will run complete_run, which will panic.
    // The runner will catch this panic from join_set and print it.
    let _job_id = scheduler
        .add_job(JobSchedule::Immediate, || async { Ok(()) })
        .await
        .unwrap();

    // Give the task and runner loop time to execute
    sleep(Duration::from_millis(50)).await;

    // Shutdown should succeed cleanly
    let shutdown_res = scheduler.shutdown().await;
    assert!(shutdown_res.is_ok());
}

#[derive(Clone, Default)]
struct FailingJobStore {
    inner: InMemoryJobStore,
    fail_insert: bool,
    fail_get: bool,
    fail_list: bool,
    fail_runnable: bool,
    fail_start: bool,
    fail_complete: bool,
}

impl FailingJobStore {
    fn new() -> Self {
        Self {
            inner: InMemoryJobStore::new(),
            ..Default::default()
        }
    }
}

impl JobStore for FailingJobStore {
    async fn insert(&self, job: JobMetadata) -> Result<(), SchedulerError> {
        if self.fail_insert {
            return Err(SchedulerError::StoreError("insert failed".to_string()));
        }
        self.inner.insert(job).await
    }
    async fn get(&self, id: JobId) -> Result<Option<JobMetadata>, SchedulerError> {
        if self.fail_get {
            return Err(SchedulerError::StoreError("get failed".to_string()));
        }
        self.inner.get(id).await
    }
    async fn list_all(&self) -> Result<Vec<JobMetadata>, SchedulerError> {
        if self.fail_list {
            return Err(SchedulerError::StoreError("list failed".to_string()));
        }
        self.inner.list_all().await
    }
    async fn get_runnable_jobs(&self) -> Result<Vec<JobId>, SchedulerError> {
        if self.fail_runnable {
            return Err(SchedulerError::StoreError("runnable failed".to_string()));
        }
        self.inner.get_runnable_jobs().await
    }
    async fn start_run(&self, id: JobId) -> Result<JobMetadata, SchedulerError> {
        if self.fail_start {
            return Err(SchedulerError::StoreError("start failed".to_string()));
        }
        self.inner.start_run(id).await
    }
    async fn complete_run(
        &self,
        id: JobId,
        result: Result<(), String>,
    ) -> Result<JobMetadata, SchedulerError> {
        if self.fail_complete {
            return Err(SchedulerError::StoreError("complete failed".to_string()));
        }
        self.inner.complete_run(id, result).await
    }
}

#[tokio::test]
async fn test_failing_store_insert() {
    let mut store = FailingJobStore::new();
    store.fail_insert = true;
    let (scheduler, _runner) = Scheduler::new(store, 2, None);

    let add_res = scheduler
        .add_job(JobSchedule::Immediate, || async { Ok(()) })
        .await;
    assert!(matches!(add_res, Err(SchedulerError::StoreError(_))));
}

#[tokio::test]
async fn test_failing_store_get() {
    let mut store = FailingJobStore::new();
    store.fail_get = true;
    let (scheduler, _runner) = Scheduler::new(store, 2, None);

    let status_res = scheduler.get_job_status(JobId::new()).await;
    assert!(matches!(status_res, Err(SchedulerError::StoreError(_))));
}

#[tokio::test]
async fn test_failing_store_list() {
    let mut store = FailingJobStore::new();
    store.fail_list = true;
    let (scheduler, runner) = Scheduler::new(store, 2, None);
    scheduler.start(runner).await.unwrap();

    sleep(Duration::from_millis(30)).await;

    let shutdown_res = scheduler
        .shutdown_with_timeout(Duration::from_secs(1))
        .await;
    assert!(matches!(shutdown_res, Err(SchedulerError::StoreError(_))));
}

#[tokio::test]
async fn test_failing_store_runnable() {
    let mut store = FailingJobStore::new();
    store.fail_runnable = true;
    let (scheduler, runner) = Scheduler::new(store, 2, None);
    scheduler.start(runner).await.unwrap();

    sleep(Duration::from_millis(30)).await;

    let shutdown_res = scheduler.shutdown().await;
    assert!(matches!(shutdown_res, Err(SchedulerError::StoreError(_))));
}

#[tokio::test]
async fn test_failing_store_start() {
    let mut store = FailingJobStore::new();
    store.fail_start = true;

    let (scheduler, runner) = Scheduler::new(store, 2, None);
    // Add job cleanly since fail_insert is false
    let _job_id = scheduler
        .add_job(JobSchedule::Immediate, || async { Ok(()) })
        .await
        .unwrap();

    scheduler.start(runner).await.unwrap();

    sleep(Duration::from_millis(30)).await;

    let shutdown_res = scheduler.shutdown().await;
    assert!(matches!(shutdown_res, Err(SchedulerError::StoreError(_))));
}

#[tokio::test]
async fn test_failing_store_complete_on_missing_task() {
    let mut store = FailingJobStore::new();
    // Pre-insert metadata directly so it looks runnable, but DO NOT register task in scheduler
    let job_id = JobId::new();
    let metadata = JobMetadata::new(job_id, JobSchedule::Immediate);
    store.inner.insert(metadata).await.unwrap();
    store.fail_complete = true;

    let (scheduler, runner) = Scheduler::new(store, 2, None);
    scheduler.start(runner).await.unwrap();

    sleep(Duration::from_millis(30)).await;

    let shutdown_res = scheduler.shutdown().await;
    assert!(matches!(shutdown_res, Err(SchedulerError::StoreError(_))));
}

#[tokio::test]
async fn test_task_panic_with_string() {
    let (scheduler, runner) = Scheduler::new_in_memory(2, None);
    scheduler.start(runner).await.unwrap();

    let job_id = scheduler
        .add_job(JobSchedule::Immediate, || async {
            panic!("{}", "Panic with a String payload".to_string());
        })
        .await
        .unwrap();

    sleep(Duration::from_millis(50)).await;

    let status = scheduler.get_job_status(job_id).await.unwrap().unwrap();
    match status {
        JobStatus::Failed(err) => {
            assert!(err.contains("Panic with a String payload"));
        }
        other => panic!("Expected Failed status, got {other:?}"),
    }

    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_task_panic_with_any() {
    let (scheduler, runner) = Scheduler::new_in_memory(2, None);
    scheduler.start(runner).await.unwrap();

    let job_id = scheduler
        .add_job(JobSchedule::Immediate, || async {
            std::panic::panic_any(42);
        })
        .await
        .unwrap();

    sleep(Duration::from_millis(50)).await;

    let status = scheduler.get_job_status(job_id).await.unwrap().unwrap();
    match status {
        JobStatus::Failed(err) => {
            assert!(err.contains("Unknown panic"));
        }
        other => panic!("Expected Failed status, got {other:?}"),
    }

    scheduler.shutdown().await.unwrap();
}
