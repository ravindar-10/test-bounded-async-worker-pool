# Examples
This directory contains examples demonstrating various features of the bounded async worker pool.

## Running Examples

``` bash
# Run the main demo
cargo run --example demo

# Run with release optimisation
cargo run --example demo --release
```

## Available Examples

### demo.rs
 - Comprehensive demonstration of all features:
 - Creating a worker pool with automatic backpressure
 - Monitoring metrics (Active,queued, completed)
 - Dynamic resizing of the pool
 - Graceful shutdown

**Key Demonstration**
- Submit 20 jobs with pool size 5 (shows backpressure)
- Dynamically increases pool to 10 workers
- Submits 10 more jobs (Shows increased throughput)
- Tracks and displays metrics throughout
- Performs clean shutdown

## Common Patterns

## Pattern 1: Simple Job Processing
``` rust
use bounded_async_worker_pool::WorkerPool;

#[tokio::main]
async fn main(){
    let pool=WorkerPool::new(10);
    for i in 0..10{
        pool.submit(
            async move{
                //process item i
            }
        ).await.unwrap();
    }
    pool.shutdown().await;
}
```
## Pattern 2: Collecting Results
``` rust
use bounded_async_worker_pool::WorkerPool;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main(){
    let pool=WorkerPool::new(10);
    let results=Arc::new(Mutex::new(Vec::new()));
    for i in 0..100{
        let results=Arc::clone(&results);
        pool.submit(
            async move{
                let result=process_item(i).await;
                results.lock().await.push(result);
            }
        ).await.unwrap();
    }
    pool.shutdown().await;
    let final_results=results.lock().await;
    println!("Processed {} items",final_results.len());
}

async fn process_item(i:usize)->String{
    format!("Result {}",i)
}
```
### Pattern 3: With Error Handling

```rust
use bounded_async_worker_pool::WorkerPool;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main(){
    let pool=WorkerPool::new(10);
    let errors=Arc::new(Mutex::new(Vec::new()));

    for i in 0..100{
        let errors=Arc::clone(&errors);
        pool.submit(async move{
            if let Err(e) = process_item(i).await{
                errors.lock().await.push((i, e));
            }
        }).await.unwrap();
    }
    pool.shutdown().await;
    let error_list= errors.lock().await;
    if !error_list.is_empty(){
        eprintln!("Encountered {} errors",error_list.len());
    }
}

async fn process_item(i:usize)-> Result<(),String>{
    Ok(())
}
```

### Pattern 4: Dynamic Load Adjustment

```rust
use bounded_async_worker_pool::WorkerPool;

#[tokio::main]
async fn main(){
    let pool=WorkerPool::new(5);
    for i in 0..50{
        pool.submit(
            async move{
                //work
            }
        ).await.unwrap();
    }
    pool.resize(20);
    for i in 50..500{
        pool.submit(async move{
            //work
        }).await.unwrap();
    }
    pool.shutdown().await;
}
```

### Pattern 5: Monitoring and Metrics

```rust
use bounded_async_worker_pool::WorkerPool;
use std::time::Duration;

#[tokio::main]
async fn main(){
    let pool=WorkerPool::new(10);
    let _metrics= pool.clone();
    tokio::spawn(async move{
        loop{
            let metrics= pool.metrics();
            println!("Active: {}, Queued: {}, Completed: {}",metrics.active,metrics.queued,metrics.completed);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    for i in 0..1000{
        pool.submit(async move{
            //work
        }).await.unwrap();
    }
    pool.shutdown().await;
}
```

### Pattern 6: Rate Limiting External API Calls

```rust
use bounded_async_worker_pool::WorkerPool;

#[tokio::main]
async fn main(){
    let pool=WorkerPool::new(5);
    let urls=vec![];

    for url in urls{
        pool.submit(
          async move{
            match fetch_data(&url).await{
                Ok(data)=> println!("Fetched: {}",data),
                Err(e) => eprintln!("Error: {}",e)
            }
          }
        ).await.unwrap();

    }
    pool.shutdown().await;
}

async fn fetch_data(url:&str)-> Result<String,String>{
    Ok(String::from("data"))
}
```
