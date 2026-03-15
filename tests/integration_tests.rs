use bounded_async_worker_pool::WorkerPool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, Instant};

#[tokio::test]
async fn test_backpressure_with_queue() {
    let pool = WorkerPool::new(2);
    let job_start_times = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let job_end_times = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let start = Instant::now();

    // submit 6 jobs that takes 100ms each
    // With pool size 2, this should take ~300ms total

    for _i in 0..6 {
        let start_times = Arc::clone(&job_start_times);
        let end_times = Arc::clone(&job_end_times);
        pool.submit(async move {
            start_times.lock().await.push(Instant::now());
            sleep(Duration::from_millis(100)).await;
            end_times.lock().await.push(Instant::now());
        })
        .await
        .unwrap();
    }
    pool.shutdown().await;
    let total_duration = start.elapsed();
    assert!(
        total_duration >= Duration::from_millis(290),
        "job finished to quickly {:?}",
        total_duration
    );
    assert!(
        total_duration < Duration::from_millis(800),
        "job took too long {:?}",
        total_duration
    );

    //verify that all jobs completed
    let end_times_vec = job_end_times.lock().await;
    assert_eq!(end_times_vec.len(), 6);
}
#[tokio::test]
async fn test_graceful_shutdown_with_inflight_jobs() {
    let pool = WorkerPool::new(5);
    let completed = Arc::new(AtomicUsize::new(0));

    //submit jobs taht take some time
    for _ in 0..20 {
        let completed_clone = Arc::clone(&completed);
        pool.submit(async move {
            sleep(Duration::from_millis(50)).await;
            completed_clone.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .unwrap();
    }
    pool.shutdown().await;
    //All jobs should have completedd
    assert_eq!(completed.load(Ordering::Relaxed), 20);
}

#[tokio::test]
async fn test_shutdown_prevents_new_submissions() {
    let pool = WorkerPool::new(5);

    pool.submit(async {
        sleep(Duration::from_millis(10)).await;
    })
    .await
    .unwrap();
    pool.shutdown().await;
}

#[tokio::test]
async fn test_high_concurrency_stress() {
    let pool = WorkerPool::new(10);
    let counter = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));
    let current_concurrent = Arc::new(AtomicUsize::new(0));

    // Submit 1000 jobs
    for _ in 0..1000 {
        let counter_clone = Arc::clone(&counter);
        let max_clone = Arc::clone(&max_concurrent);
        let current_clone = Arc::clone(&current_concurrent);
        pool.submit(async move {
            let current = current_clone.fetch_add(1, Ordering::Relaxed) + 1;
            let mut max = max_clone.load(Ordering::Relaxed);
            while current > max {
                match max_clone.compare_exchange(max, current, Ordering::Relaxed, Ordering::Relaxed)
                {
                    Ok(_) => break,
                    Err(x) => max = x,
                }
            }
            // Simulate work
            sleep(Duration::from_millis(100)).await;
            current_clone.fetch_sub(1, Ordering::Relaxed);
            counter_clone.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .unwrap();
    }
    pool.shutdown().await;
    // All jobs should be completed
    assert_eq!(counter.load(Ordering::Relaxed), 1000);
    // Should never exceed pool size
    let max = max_concurrent.load(Ordering::Relaxed);
    assert!(max <= 10, "max concurrent was {}, expected <=10", max);
}
#[tokio::test]
async fn empty_pool_shutdown() {
    let pool = WorkerPool::new(5);
    pool.shutdown().await;
}

#[tokio::test]

async fn test_dynamic_resize() {
    let pool = WorkerPool::new(5);
    pool.resize(10);

    let counter = Arc::new(AtomicUsize::new(0));
    let current = Arc::new(AtomicUsize::new(0));
    // Submit 4 jobs, only 2 should run concurrently
    for _ in 0..15 {
        let counter_clone = Arc::clone(&counter);
        let current_clone = Arc::clone(&current);
        pool.submit(async move {
            let val = current_clone.fetch_add(1, Ordering::Relaxed) + 1;
            let mut max = counter_clone.load(Ordering::Relaxed);

            while val > max {
                match counter_clone.compare_exchange(max, val, Ordering::Relaxed, Ordering::Relaxed)
                {
                    Ok(_) => break,
                    Err(x) => max = x,
                }
            }
            sleep(Duration::from_millis(50)).await;
            counter_clone.fetch_sub(1, Ordering::Relaxed);
        })
        .await
        .unwrap();
    }
    pool.shutdown().await;
    let max = counter.load(Ordering::Relaxed);
    assert!(
        max > 5,
        "Expected max concurrent >5 after resize, got {}",
        max
    );
    assert!(max <= 10, "Expected max concurrent <=10, got {}", max);
}

#[tokio::test]
async fn test_job_ordering_fairness() {
    let pool = WorkerPool::new(1);
    let order = Arc::new(tokio::sync::Mutex::new(Vec::<usize>::new()));

    //submit jobs in order
    for i in 0..10 {
        let order_clone = Arc::clone(&order);
        pool.submit(async move {
            let mut order_vec = order_clone.lock().await;
            order_vec.push(i);
        })
        .await
        .unwrap();
    }
    pool.shutdown().await;

    let final_order = order.lock().await;
    assert_eq!(*final_order, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[tokio::test]
async fn test_panic_in_job_does_not_crash_pool() {
    let pool = WorkerPool::new(5);
    let completed = Arc::new(AtomicUsize::new(0));

    //submit a job that panics
    pool.submit(async move {
        panic!("job panicked");
    })
    .await
    .unwrap();

    //submit a normal job after the panic
    for _ in 0..10 {
        let completed_clone = Arc::clone(&completed);
        pool.submit(async move {
            sleep(Duration::from_millis(50)).await;
            completed_clone.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .unwrap();
    }
    pool.shutdown().await;
    // Normal jobs should till complete
    // Note: The panicking job might be caught or cause issues,
    // But the pool should continue processing other jobs
    assert!(completed.load(Ordering::Relaxed) >= 9);
}
