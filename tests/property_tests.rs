use proptest::prelude::*;
use rust_best_practices::{JobId, JobMetadata, JobSchedule, JobStatus};
use std::time::Duration;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]
    #[test]
    fn test_job_metadata_creation_properties(
        delay_ms in 1u64..10000u64,
        interval_ms in 1u64..10000u64,
    ) {
        let id = JobId::new();

        // 1. Immediate Schedule Property
        let meta_immediate = JobMetadata::new(id, JobSchedule::Immediate);
        prop_assert_eq!(meta_immediate.id, id);
        prop_assert_eq!(meta_immediate.schedule, JobSchedule::Immediate);
        prop_assert_eq!(meta_immediate.status, JobStatus::Pending);
        prop_assert_eq!(meta_immediate.run_count, 0);
        prop_assert!(meta_immediate.last_run_start.is_none());
        prop_assert!(meta_immediate.next_run_time.is_some());

        // 2. Delayed Schedule Property
        let delay = Duration::from_millis(delay_ms);
        let meta_delayed = JobMetadata::new(id, JobSchedule::Delayed(delay));
        prop_assert_eq!(meta_delayed.schedule, JobSchedule::Delayed(delay));
        prop_assert!(meta_delayed.next_run_time.is_some());

        // 3. Interval Schedule Property
        let interval = Duration::from_millis(interval_ms);
        let meta_interval = JobMetadata::new(id, JobSchedule::Interval(interval));
        prop_assert_eq!(meta_interval.schedule, JobSchedule::Interval(interval));
        prop_assert!(meta_interval.next_run_time.is_some());
    }
}
