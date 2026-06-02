use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use rust_test::{InMemoryJobStore, JobId, JobMetadata, JobSchedule, JobStatus, JobStore, SchedulerError};

#[tokio::test]
async fn test_store_basic_insert_and_get() {
    let store = InMemoryJobStore::new();
    let id = JobId::new();
    let metadata = JobMetadata::new(id, JobSchedule::Immediate);

    // Initial get should be None
    let get_res = store.get(id).await.unwrap();
    assert!(get_res.is_none());

    // Insert job
    store.insert(metadata.clone()).await.unwrap();

    // Retrieve and verify
    let get_res = store.get(id).await.unwrap().unwrap();
    assert_eq!(get_res.id, id);
    assert_eq!(get_res.schedule, JobSchedule::Immediate);
    assert_eq!(get_res.status, JobStatus::Pending);

    // Double insert should fail with DuplicateJob
    let dup_res = store.insert(metadata).await;
    match dup_res {
        Err(SchedulerError::DuplicateJob(dup_id)) => assert_eq!(dup_id, id),
        other => panic!("Expected DuplicateJob error, got {other:?}"),
    }
}

#[tokio::test]
async fn test_store_list_all() {
    let store = InMemoryJobStore::new();
    
    let id1 = JobId::new();
    let id2 = JobId::new();
    
    store.insert(JobMetadata::new(id1, JobSchedule::Immediate)).await.unwrap();
    store.insert(JobMetadata::new(id2, JobSchedule::Delayed(Duration::from_secs(10)))).await.unwrap();

    let list = store.list_all().await.unwrap();
    assert_eq!(list.len(), 2);
    
    let ids: Vec<JobId> = list.iter().map(|job| job.id).collect();
    assert!(ids.contains(&id1));
    assert!(ids.contains(&id2));
}

#[tokio::test]
async fn test_store_runnable_jobs_filtering() {
    let store = InMemoryJobStore::new();

    let immediate_id = JobId::new();
    let delayed_future_id = JobId::new();
    let delayed_past_id = JobId::new();

    // 1. Immediate job: should be runnable immediately
    store.insert(JobMetadata::new(immediate_id, JobSchedule::Immediate)).await.unwrap();

    // 2. Delayed job in the future (10 seconds from now): should not be runnable
    store.insert(JobMetadata::new(
        delayed_future_id, 
        JobSchedule::Delayed(Duration::from_secs(10))
    )).await.unwrap();

    // 3. Delayed job in the past (manually craft next_run_time to be past): should be runnable
    let mut past_job = JobMetadata::new(delayed_past_id, JobSchedule::Delayed(Duration::from_secs(5)));
    past_job.next_run_time = Some(Instant::now() - Duration::from_secs(1));
    store.insert(past_job).await.unwrap();

    let runnable = store.get_runnable_jobs().await.unwrap();
    assert_eq!(runnable.len(), 2);
    assert!(runnable.contains(&immediate_id));
    assert!(runnable.contains(&delayed_past_id));
    assert!(!runnable.contains(&delayed_future_id));
}

#[tokio::test]
async fn test_store_job_lifecycle_transitions() {
    let store = InMemoryJobStore::new();
    let id = JobId::new();
    
    // Test errors on non-existent job
    let err_start = store.start_run(id).await;
    assert!(matches!(err_start, Err(SchedulerError::JobNotFound(_))));

    let err_complete = store.complete_run(id, Ok(())).await;
    assert!(matches!(err_complete, Err(SchedulerError::JobNotFound(_))));

    // Insert job
    let schedule = JobSchedule::Immediate;
    store.insert(JobMetadata::new(id, schedule)).await.unwrap();

    // Start running
    let started = store.start_run(id).await.unwrap();
    assert_eq!(started.status, JobStatus::Running);
    assert_eq!(started.run_count, 1);
    assert!(started.last_run_start.is_some());
    assert!(started.next_run_time.is_none());

    // Complete run (Success)
    let completed = store.complete_run(id, Ok(())).await.unwrap();
    assert_eq!(completed.status, JobStatus::Completed);
    
    // If we run it again
    let started_again = store.start_run(id).await.unwrap();
    assert_eq!(started_again.run_count, 2);

    // Complete run (Failed)
    let failed = store.complete_run(id, Err("Something went wrong".to_string())).await.unwrap();
    assert_eq!(failed.status, JobStatus::Failed("Something went wrong".to_string()));
}

#[tokio::test]
async fn test_store_periodic_rescheduling() {
    let store = InMemoryJobStore::new();
    let id = JobId::new();
    let interval = Duration::from_millis(50);
    
    store.insert(JobMetadata::new(id, JobSchedule::Interval(interval))).await.unwrap();

    // Start and complete the job
    let started = store.start_run(id).await.unwrap();
    let start_time = started.last_run_start.unwrap();
    
    // Complete the run
    let completed = store.complete_run(id, Ok(())).await.unwrap();
    
    // Interval jobs should go back to Pending
    assert_eq!(completed.status, JobStatus::Pending);
    // Next execution time should be exactly start_time + interval duration
    let next_run = completed.next_run_time.unwrap();
    assert_eq!(next_run, start_time + interval);
}

#[tokio::test]
async fn test_store_concurrent_access() {
    let store = Arc::new(InMemoryJobStore::new());
    let mut handles = Vec::new();

    // Spawn 10 concurrent writers inserting distinct jobs
    for _i in 0..10 {
        let store_clone = store.clone();
        handles.push(tokio::spawn(async move {
            let id = JobId::new();
            store_clone.insert(JobMetadata::new(id, JobSchedule::Immediate)).await.unwrap();
            
            // Read back immediately
            let job = store_clone.get(id).await.unwrap().unwrap();
            assert_eq!(job.status, JobStatus::Pending);
            
            // Transition job
            let started = store_clone.start_run(id).await.unwrap();
            assert_eq!(started.status, JobStatus::Running);
            
            let completed = store_clone.complete_run(id, Ok(())).await.unwrap();
            assert_eq!(completed.status, JobStatus::Completed);
        }));
    }

    // Spawn 5 concurrent readers querying list_all
    for _ in 0..5 {
        let store_clone = store.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..10 {
                let _list = store_clone.list_all().await.unwrap();
                sleep(Duration::from_millis(5)).await;
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify final size is 10
    let list = store.list_all().await.unwrap();
    assert_eq!(list.len(), 10);
    for job in list {
        assert_eq!(job.status, JobStatus::Completed);
    }
}
