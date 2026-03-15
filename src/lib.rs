//! ```
//! use std::time::Duration;
//! use bounded_async_worker_pool::WorkerPool;
//!
//! #[tokio::main]
//! async fn main(){
//!     let pool=WorkerPool::new(10);
//!     for i in 0..100{
//!         pool.submit(async  move{
//!             tokio::time::sleep(Duration::from_millis(10)).await;
//!         }).await.unwrap();
//!     }
//!     let metrics = pool.metrics();
//!     println!("Completed: {}", metrics.completed);
//!     pool.shutdown().await;
//! }
//! ```
mod worker_pool;
pub use worker_pool::{PoolMetrics, WorkerPool, WorkerPoolError};
