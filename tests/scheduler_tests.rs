use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;

use rust_best_practices::{JobSchedule, JobStatus, RateLimiter, Scheduler};

#[tokio::test]
async fn test_immediate_job_execution() {
    let (scheduler, runner) = Scheduler::new_in_memory(2, None);
    scheduler.start(runner).await.unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    let job_id = scheduler
        .add_job(JobSchedule::Immediate, move || {
            let inner_counter = counter_clone.clone();
            async move {
                inner_counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();

    // Give it a moment to run
    sleep(Duration::from_millis(50)).await;

    // Check execution
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    let status = scheduler.get_job_status(job_id).await.unwrap().unwrap();
    assert_eq!(status, JobStatus::Completed);

    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_delayed_job_execution() {
    let (scheduler, runner) = Scheduler::new_in_memory(2, None);
    scheduler.start(runner).await.unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    let delay = Duration::from_millis(100);
    let job_id = scheduler
        .add_job(JobSchedule::Delayed(delay), move || {
            let inner_counter = counter_clone.clone();
            async move {
                inner_counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();

    // Check immediately: shouldn't have run yet
    sleep(Duration::from_millis(20)).await;
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    let status = scheduler.get_job_status(job_id).await.unwrap().unwrap();
    assert_eq!(status, JobStatus::Pending);

    // Sleep long enough for the delay to expire
    sleep(Duration::from_millis(120)).await;
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    let status = scheduler.get_job_status(job_id).await.unwrap().unwrap();
    assert_eq!(status, JobStatus::Completed);

    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_interval_job_execution() {
    let (scheduler, runner) = Scheduler::new_in_memory(2, None);
    scheduler.start(runner).await.unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    let interval = Duration::from_millis(30);
    let job_id = scheduler
        .add_job(JobSchedule::Interval(interval), move || {
            let inner_counter = counter_clone.clone();
            async move {
                inner_counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();

    // Wait for ~3 executions
    sleep(Duration::from_millis(110)).await;

    let run_count = counter.load(Ordering::SeqCst);
    assert!(run_count >= 3, "Expected at least 3 runs, got {run_count}");

    let metadata = scheduler.get_job_metadata(job_id).await.unwrap().unwrap();
    // Interval jobs reschedule themselves back to Pending status
    assert_eq!(metadata.status, JobStatus::Pending);
    assert!(metadata.run_count >= 3);

    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_panic_resilience() {
    let (scheduler, runner) = Scheduler::new_in_memory(2, None);
    scheduler.start(runner).await.unwrap();

    // Register a panicking job
    let panic_job_id = scheduler
        .add_job(JobSchedule::Immediate, || async {
            panic!("Expected test panic");
        })
        .await
        .unwrap();

    // Register a normal job to ensure scheduler continues working after panic
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    let normal_job_id = scheduler
        .add_job(JobSchedule::Immediate, move || {
            let inner_counter = counter_clone.clone();
            async move {
                inner_counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();

    sleep(Duration::from_millis(50)).await;

    // First job status should be Failed with panic message
    let panic_status = scheduler
        .get_job_status(panic_job_id)
        .await
        .unwrap()
        .unwrap();
    match panic_status {
        JobStatus::Failed(err) => {
            assert!(err.contains("Task panicked: Expected test panic"));
        }
        other => panic!("Expected Failed status, got {other:?}"),
    }

    // Second job must have finished successfully, showing workers survived
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    let normal_status = scheduler
        .get_job_status(normal_job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(normal_status, JobStatus::Completed);

    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_rate_limiting() {
    // Capacity 2, refill 5 per second (1 token every 200ms)
    let limiter = RateLimiter::new(2, 5.0);
    let (scheduler, runner) = Scheduler::new_in_memory(10, Some(limiter));
    scheduler.start(runner).await.unwrap();

    let counter = Arc::new(AtomicUsize::new(0));

    // Submit 5 jobs that want to run immediately
    for _ in 0..5 {
        let counter_clone = counter.clone();
        scheduler
            .add_job(JobSchedule::Immediate, move || {
                let inner_counter = counter_clone.clone();
                async move {
                    inner_counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await
            .unwrap();
    }

    let start_time = Instant::now();

    // Wait until all 5 jobs complete
    loop {
        if counter.load(Ordering::SeqCst) == 5 {
            break;
        }
        sleep(Duration::from_millis(10)).await;
        if start_time.elapsed() > Duration::from_secs(2) {
            panic!("Test timed out waiting for rate limited jobs to finish");
        }
    }

    let elapsed = start_time.elapsed();
    // 5 jobs. Initial capacity = 2.
    // Job 1 & 2 run immediately.
    // Job 3 runs after 1 refill (200ms).
    // Job 4 runs after 2 refills (400ms).
    // Job 5 runs after 3 refills (600ms).
    // Thus, elapsed time must be at least 500-600ms.
    assert!(
        elapsed >= Duration::from_millis(500),
        "Expected rate limiting to delay jobs, but they finished in {elapsed:?}"
    );

    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_graceful_shutdown() {
    let (scheduler, runner) = Scheduler::new_in_memory(2, None);
    scheduler.start(runner).await.unwrap();

    let job_started = Arc::new(AtomicUsize::new(0));
    let job_finished = Arc::new(AtomicUsize::new(0));

    let job_started_clone = job_started.clone();
    let job_finished_clone = job_finished.clone();

    // Register a long running job
    scheduler
        .add_job(JobSchedule::Immediate, move || {
            let started = job_started_clone.clone();
            let finished = job_finished_clone.clone();
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(80)).await;
                finished.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();

    // Let it start
    sleep(Duration::from_millis(20)).await;
    assert_eq!(job_started.load(Ordering::SeqCst), 1);
    assert_eq!(job_finished.load(Ordering::SeqCst), 0);

    // Call shutdown: this should wait for the active job to finish
    let shutdown_start = Instant::now();
    scheduler.shutdown().await.unwrap();
    let shutdown_elapsed = shutdown_start.elapsed();

    // The job should have finished during shutdown
    assert_eq!(job_finished.load(Ordering::SeqCst), 1);
    // Shutdown should have blocked for at least the remaining duration of the job (~60ms)
    assert!(shutdown_elapsed >= Duration::from_millis(50));
}

#[tokio::test]
async fn test_scheduler_no_drift() {
    let (scheduler, runner) = Scheduler::new_in_memory(2, None);
    scheduler.start(runner).await.unwrap();

    let run_times = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let run_times_clone = run_times.clone();

    let interval = Duration::from_millis(30);

    scheduler
        .add_job(JobSchedule::Interval(interval), move || {
            let times = run_times_clone.clone();
            async move {
                times.lock().await.push(Instant::now());
                sleep(Duration::from_millis(8)).await; // Simulate work
                Ok(())
            }
        })
        .await
        .unwrap();

    // Wait enough for 5 runs
    sleep(Duration::from_millis(160)).await;
    scheduler.shutdown().await.unwrap();

    let times = run_times.lock().await;
    assert!(
        times.len() >= 4,
        "Expected at least 4 runs, got {}",
        times.len()
    );

    let start = times[0];
    for (i, &time) in times.iter().enumerate().skip(1) {
        let expected_elapsed = interval * (i as u32);
        let actual_elapsed = time.duration_since(start);
        let diff = actual_elapsed.abs_diff(expected_elapsed);

        // Allow up to 10ms of scheduler jitter (very safe),
        // but if there was drift, the 4th run (index 3) would have drifted by at least 3 * 8ms = 24ms.
        assert!(
            diff < Duration::from_millis(10),
            "Run {} drifted by {:?}. Expected elapsed: {:?}, actual: {:?}",
            i,
            diff,
            expected_elapsed,
            actual_elapsed
        );
    }
}

#[tokio::test]
async fn test_shutdown_timeout_aborts_jobs() {
    let (scheduler, runner) = Scheduler::new_in_memory(2, None);
    scheduler.start(runner).await.unwrap();

    let job_completed = Arc::new(AtomicUsize::new(0));
    let job_completed_clone = job_completed.clone();

    scheduler
        .add_job(JobSchedule::Immediate, move || {
            let completed = job_completed_clone.clone();
            async move {
                sleep(Duration::from_millis(500)).await;
                completed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();

    sleep(Duration::from_millis(20)).await;

    let shutdown_res = scheduler
        .shutdown_with_timeout(Duration::from_millis(50))
        .await;

    assert!(
        matches!(
            shutdown_res,
            Err(rust_best_practices::SchedulerError::Timeout)
        ),
        "Expected Timeout error, got {:?}",
        shutdown_res
    );

    sleep(Duration::from_millis(500)).await;
    assert_eq!(
        job_completed.load(Ordering::SeqCst),
        0,
        "Aborted job should not complete"
    );
}

#[tokio::test]
async fn test_concurrent_job_registration() {
    let (scheduler, runner) = Scheduler::new_in_memory(10, None);
    scheduler.start(runner).await.unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..8 {
        let scheduler_clone = scheduler.clone();
        let counter_clone = counter.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..15 {
                let inner_counter = counter_clone.clone();
                scheduler_clone
                    .add_job(JobSchedule::Immediate, move || {
                        let c = inner_counter.clone();
                        async move {
                            c.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }
                    })
                    .await
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let start = Instant::now();
    loop {
        if counter.load(Ordering::SeqCst) == 120 {
            break;
        }
        sleep(Duration::from_millis(10)).await;
        if start.elapsed() > Duration::from_secs(2) {
            panic!(
                "Timed out waiting for concurrent jobs. Completed: {}",
                counter.load(Ordering::SeqCst)
            );
        }
    }

    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_scheduler_builder_and_try_new() {
    use rust_best_practices::scheduler::SchedulerBuilder;

    // Test successful builder with rate_limiter
    let (scheduler, runner) = SchedulerBuilder::new()
        .max_concurrent_jobs(4)
        .rate_limiter(5, 10.0)
        .unwrap()
        .build();

    scheduler.start(runner).await.unwrap();

    let job_id = scheduler
        .add_job(JobSchedule::Immediate, || async { Ok(()) })
        .await
        .unwrap();

    sleep(Duration::from_millis(50)).await;
    let status = scheduler.get_job_status(job_id).await.unwrap().unwrap();
    assert_eq!(status, JobStatus::Completed);

    scheduler.shutdown().await.unwrap();

    // Test invalid rate limiter configurations in try_new
    assert!(RateLimiter::try_new(0, 10.0).is_err());
    assert!(RateLimiter::try_new(5, -1.0).is_err());
    assert!(RateLimiter::try_new(5, f64::NAN).is_err());
}
