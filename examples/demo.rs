use bounded_async_worker_pool::WorkerPool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, Instant};

#[tokio::main]
async fn main() {
    println!("=== Bounded Aysnc Worker Pool Demo ===\n");
    let pool = WorkerPool::new(5);
    println!("Worker pool created with 5 concurrent jobs");
    let counter = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    println!("Submitting 20 jobs (each takes ~ 200ms) ...");
    for i in 0..20 {
        let counter_clone = Arc::clone(&counter);
        pool.submit(async move {
            let job_start = Instant::now();
            let job_id = i;

            println!(" [Job {}] started", job_id);
            //simulate work
            sleep(Duration::from_millis(200)).await;
            let elapsed = job_start.elapsed();
            counter_clone.fetch_add(1, Ordering::Relaxed);
            println!(" [Job {}] completed in {:.2?}", job_id, elapsed);
        })
        .await
        .unwrap();
        // check metrics periodically
        if i % 5 == 4 {
            let metrics = pool.metrics();
            println!(
                "\n Metrics: Active={}, Queued={},Completed={}\n",
                metrics.active, metrics.queued, metrics.completed
            );
        }
    }

    println!("All jobs submitted . Waiting for Completion...");
    //Demonstrate dynamic resizing
    println!("\n Resizing pool to 10 Workers...");
    pool.resize(10);
    println!("Submitting 10 more jobs with increased capacity...");
    for i in 20..30 {
        let counter_clone = Arc::clone(&counter);
        pool.submit(async move {
            println!(" [Job {}] started (with larger pool)", i);
            sleep(Duration::from_millis(150)).await;
            counter_clone.fetch_add(1, Ordering::Relaxed);
            println!(" [Job {}] completed (with larger pool)", i);
        })
        .await
        .unwrap();
    }
    //Final metrics before shutdown
    sleep(Duration::from_millis(100)).await;
    let metrics = pool.metrics();
    println!(
        "Final Metrics: Active={}, Queued={}, Completed={}",
        metrics.active, metrics.queued, metrics.completed
    );
    //Graceful shutdown
    println!("\n Initiating graceful shutdown...");
    pool.shutdown().await;
    let total_duration = start.elapsed();
    let final_count = counter.load(Ordering::Relaxed);
    println!("\n === Results ===");
    println!("Total Jobs Completed: {}", final_count);
    println!("total time: {:?}", total_duration);
    println!(
        "Average Time per job {:?}",
        total_duration / final_count as u32
    );
    println!("\n Demo completed successfully");
}
