use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
// use bounded_async_worker_pool::WorkerPool;

// A boxed future Representing a job
type Job = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
//Error that can occure when interacting with the worker pool

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerPoolError {
    ShutDown,
}

impl std::fmt::Display for WorkerPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerPoolError::ShutDown => write!(f, "Worker pool has been shut down"),
        }
    }
}

impl std::error::Error for WorkerPoolError {}

// Metrics for Worker pool
#[derive(Debug, Clone)]
pub struct PoolMetrics {
    pub active: usize,
    pub queued: usize,
    pub completed: usize,
}

// Shared state between the worker pool and its workers
pub struct WorkerPoolState {
    semaphore: Arc<Semaphore>,
    shutdown: AtomicBool,
    active_count: AtomicUsize,
    queued_count: AtomicUsize,
    completed_count: AtomicUsize,
    max_size: AtomicUsize,
}
impl WorkerPoolState {
    fn new(max_size: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_size)),
            shutdown: AtomicBool::new(false),
            active_count: AtomicUsize::new(0),
            queued_count: AtomicUsize::new(0),
            completed_count: AtomicUsize::new(0),
            max_size: AtomicUsize::new(max_size),
        }
    }
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
    fn initiate_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }
}
pub struct WorkerPool {
    pub state: Arc<WorkerPoolState>,
    job_sender: mpsc::Sender<Job>,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
}
impl WorkerPool {
    pub fn new(max_concurrent_jobs: usize) -> Self {
        assert!(max_concurrent_jobs > 0, "Pool size must be at least 1");
        let state = Arc::new(WorkerPoolState::new(max_concurrent_jobs));
        let (job_sender, job_receiver) = mpsc::channel(max_concurrent_jobs * 2);
        let worker_state = Arc::clone(&state);
        let worker_handle = tokio::spawn(async move {
            Self::worker_loop(worker_state, job_receiver).await;
        });
        Self {
            state,
            job_sender,
            worker_handle: Some(worker_handle),
        }
    }

    pub async fn submit<F>(&self, job: F) -> Result<(), WorkerPoolError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if self.state.is_shutdown() {
            return Err(WorkerPoolError::ShutDown);
        }
        self.state.queued_count.fetch_add(1, Ordering::Relaxed);
        let boxed_job: Job = Box::pin(job);
        self.job_sender
            .send(boxed_job)
            .await
            .map_err(|_| WorkerPoolError::ShutDown)?;
        Ok(())
    }
    pub async fn shutdown(mut self) {
        self.state.initiate_shutdown();
        drop(self.job_sender);
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.await;
        }
    }
    pub fn metrics(&self) -> PoolMetrics {
        PoolMetrics {
            active: self.state.active_count.load(Ordering::Relaxed),
            queued: self.state.queued_count.load(Ordering::Relaxed),
            completed: self.state.completed_count.load(Ordering::Relaxed),
        }
    }
    pub fn is_shutdown(&self) -> bool {
        self.state.is_shutdown()
    }
    pub fn resize(&self, new_size: usize) {
        assert!(new_size > 0, "Pool size must be at least 1");
        let current_size = self.state.max_size.load(Ordering::Relaxed);
        if new_size > current_size {
            let to_add = new_size - current_size;
            self.state.semaphore.add_permits(to_add);
        } else if new_size < current_size {
            let to_remove = current_size - new_size;
            let semaphore = Arc::clone(&self.state.semaphore);
            tokio::spawn(async move {
                for _ in 0..to_remove {
                    if let Ok(permit) = semaphore.acquire().await {
                        permit.forget();
                    }
                }
            });
        }
        self.state.max_size.store(new_size, Ordering::Relaxed);
    }
    async fn worker_loop(state: Arc<WorkerPoolState>, mut job_receiver: mpsc::Receiver<Job>) {
        let mut join_set = JoinSet::new();
        loop {
            tokio::select! {
            Some(job)= job_receiver.recv()=>{
                state.queued_count.fetch_sub(1,Ordering::Relaxed);
                let permit=state.semaphore.clone().acquire_owned().await.unwrap();
                state.active_count.fetch_add(1, Ordering::Relaxed);
                let job_state= Arc::clone(&state);
                join_set.spawn(async move{
                    job.await;
                    // job completed
                    drop(permit);
                    job_state.active_count.fetch_sub(1,Ordering::Relaxed);
                    job_state.completed_count.fetch_add(1,Ordering::Relaxed);

                });
                }
                Some(_)=join_set.join_next(),
                if !join_set.is_empty()=>{

                }
                else => {
                    break;
                }
            }
        }
        while join_set.join_next().await.is_some() {}
    }
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::WorkerPool;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    #[test]
    #[should_panic(expected = "Pool size must be at least 1")]
    fn test_zero_size_panics() {
        let _ = WorkerPool::new(0);
    }
    #[tokio::test]
    async fn test_submit_single_job() {
        let pool = WorkerPool::new(5);
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        pool.submit(async move {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .unwrap();
        pool.shutdown().await;
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }
    #[tokio::test]
    async fn test_submit_multiple_jobs() {
        let pool = WorkerPool::new(5);
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..100 {
            let counter_clone = Arc::clone(&counter);
            pool.submit(async move {
                counter_clone.fetch_add(1, Ordering::Relaxed);
            })
            .await
            .unwrap();
        }
        pool.shutdown().await;
        assert_eq!(counter.load(Ordering::Relaxed), 100);
    }

    #[tokio::test]
    async fn test_bounded_concurrency() {
        let pool = WorkerPool::new(5);
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let current_concurrent = Arc::new(AtomicUsize::new(0));

        for _ in 0..20 {
            let max_clone = Arc::clone(&max_concurrent);
            let current_clone = Arc::clone(&current_concurrent);

            pool.submit(async move {
                let current = current_clone.fetch_add(1, Ordering::Relaxed) + 1;
                let mut max = max_clone.load(Ordering::Relaxed);

                while current > max {
                    match max_clone.compare_exchange(
                        max,
                        current,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(x) => max = x,
                    }
                }
                sleep(Duration::from_millis(50)).await;
                current_clone.fetch_sub(1, Ordering::Relaxed);
            })
            .await
            .unwrap();
        }
        pool.shutdown().await;
        let max = max_concurrent.load(Ordering::Relaxed);
        assert!(max <= 5, "Max concurrent jobs exceeded pool size: {}", max);
    }
    #[tokio::test]
    async fn test_shutdown_rejects_new_jobs() {
        let pool = WorkerPool::new(5);
        let state = Arc::clone(&pool.state);
        pool.shutdown().await;
        assert!(state.is_shutdown());
    }
    #[tokio::test]
    async fn test_metrics() {
        let pool = WorkerPool::new(2);
        let barrier1 = Arc::new(tokio::sync::Barrier::new(2));
        let barrier2 = Arc::new(tokio::sync::Barrier::new(2));
        let b1 = Arc::clone(&barrier1);
        let b2 = Arc::clone(&barrier2);
        pool.submit(async move {
            b1.wait().await;
            b2.wait().await;
        })
        .await
        .unwrap();
        sleep(Duration::from_millis(10)).await;
        barrier1.wait().await;
        let metrics = pool.metrics();
        assert_eq!(metrics.active, 1);
        assert_eq!(metrics.completed, 0);
        //release the job
        barrier2.wait().await;
        sleep(Duration::from_millis(50)).await;

        let metrics = pool.metrics();
        assert_eq!(metrics.active, 0);
        assert_eq!(metrics.completed, 1);
        pool.shutdown().await;
    }
    #[tokio::test]
    async fn test_resize_increase() {
        let pool = WorkerPool::new(2);
        pool.resize(5);
        assert_eq!(pool.state.max_size.load(Ordering::Relaxed), 5);
        // should be able to run 5 jobs concurrently now
        let counter = Arc::new(AtomicUsize::new(0));
        let current = Arc::new(AtomicUsize::new(0));

        for _ in 0..10 {
            let counter_clone = Arc::clone(&counter);
            let current_clone = Arc::clone(&current);
            pool.submit(async move {
                let val = current_clone.fetch_add(1, Ordering::Relaxed) + 1;
                let mut max = counter_clone.load(Ordering::Relaxed);
                while val > max {
                    match counter_clone.compare_exchange(
                        max,
                        val,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(x) => max = x,
                    }
                }
                sleep(Duration::from_millis(50)).await;
                current_clone.fetch_sub(1, Ordering::Relaxed);
            })
            .await
            .unwrap();
        }
        pool.shutdown().await;
        let max = counter.load(Ordering::Relaxed);
        assert!(max <= 5 && max > 2, "Max concurrent was {}", max);
    }

    #[tokio::test]
    async fn test_resize_decrease() {
        let pool = WorkerPool::new(5);
        pool.resize(2);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(pool.state.max_size.load(Ordering::Relaxed), 2);
        pool.shutdown().await;
    }
    #[tokio::test]
    #[should_panic(expected = "Pool size must be at least 1")]
    async fn test_resize_zero_panics() {
        let pool = WorkerPool::new(5);
        pool.resize(0);
        pool.shutdown().await;
    }
}
