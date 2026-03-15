 # Design Document: Bounded Async Worker Pool

## Overview
This document describes the architecture, implementation details and design tradeoffs of the bounded asynchronous worker pool implementation in Rust

## Architecture
### High-Level Design

The Worker pool is built around 3 core components:
1. **Semaphore**: Controls concurrent execution (via tokio::sync::Semaphore)
2. **Bounded Channel**: Queues Incoming jobs ( via tokio::sync:mpsc::Channel)
3. **Worker Pool**: Coordinates job dispatch and execution

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │ submit(job)
       ▼
┌─────────────────────────────────────────────┐
│           WorkerPool                        │
│  ┌────────────┐      ┌─────────────┐      │
│  │  Bounded   │──────▶ Worker Loop  │      │
│  │  Channel   │      │             │      │
│  │ (Queue)    │      │  ┌────────┐ │      │
│  └────────────┘      │  │JoinSet │ │      │
│                      │  └────────┘ │      │
│  ┌────────────┐      └─────────────┘      │
│  │ Semaphore  │            │              │
│  │ (N permits)│◀───────────┘              │
│  └────────────┘                           │
└───────────────────────────────────────────┘
       │
       ▼
  ┌─────────┐
  │  Jobs   │
  │ Execute │
  └─────────┘
```
### Concurrency Model
The system uses a **permit-based** concurrency model:
1. Client calls `submit(job)` with a future to execute
2. Job is sent through a bounded channel (Capacity = 2 × max_concurrent_jobs)
3. Worker loop receives the job from the channel
4. Worker loop acquires a semaphore permit (blocks if at capacity)
5. Job is spawned as a tokio task with the permit
6. When job completes, permit is automatically released
7. Worker loop can now dispatch another job

This design naturally provides backpressure at two levels:
- **Channel level**: Prevents unbounded memory growth from queued jobs
- **Semaphore level**: Limits actual concurrent execution




### Key Data Structures
#### WorkerPoolState

Shared state between the pool and worker tasks:
```rust
struct WorkerPoolState {
    semaphore: Arc<Semaphore>,        // Limits concurrency
    shutdown: AtomicBool,              // Shutdown flag
    active_count: AtomicUsize,         // Currently executing jobs
    queued_count: AtomicUsize,         // Jobs in channel
    completed_count: AtomicUsize,      // Total completed
    max_size: AtomicUsize,             // Pool capacity
}
```
#### WorkerPool

Main API surface:

```rust
pub struct WorkerPool {
    state: Arc<WorkerPoolState>,
    job_sender: mpsc::Sender<Job>,
    worker_handle: Option<JoinHandle<()>>,
}
```
## Backpressure Implementation 
### Two-Tier Backpressure

#### 1. Bounded Channel Backpressure
The channel has a fixed capacity (2 × max_concurrent_jobs). When full:
- `submit()` awaits until space is available
- Prevents unbounded memory usage from job accumulation
- Provides buffering for bursty workloads

**Why 2× capacity?** 
- Allows some job buffering for smooth operation
- Not too large to cause memory concerns
- Balances latency vs. throughput

#### 2. Semaphore Backpressure

The semaphore has N permits (max_concurrent_jobs). When all permits are acquired:
- Worker loop blocked on `semaphore.acquire_owned().await`
- New jobs wait in the channel until permits are released
- Enforces strict concurrency limit

## Graceful Shutdown

### Shutdown Procedure

The shutdown process follows these steps:

1. **Mark shutdown**: Set `shutdown` flag to reject new submissions
2. **Close channel**: Drop the sender to signal worker loop
3. **Drain queue**: Worker loop processes all remaining queued jobs
4. **Await completion**: Worker loop waits for all in-flight jobs via `JoinSet`
5. **Exit**: Worker loop terminates, shutdown complete

### Implementation Details

```rust
pub async fn shutdown(mut self) {
    self.state.initiate_shutdown();
    drop(self.job_sender);
    if let Some(handle) = self.worker_handle.take() {
        let _ = handle.await;
    }
}
```

Worker loop shutdown logic:

```rust
loop {
    tokio::select! {
        Some(job) = job_receiver.recv() => {
            // Process job...
        }
        Some(_) = join_set.join_next() => {
            // Job completed...
        }
        else => {
            break;
        }
    }
}
// Wait for remaining jobs
while join_set.join_next().await.is_some() {}
```

## Trade-offs and Design Decisions

### 1. Boxed Futures vs. Generic Jobs

**Decision**: Use `Box<dyn Future<Output = ()> + Send>`

**Pros**:
- Simple, ergonomic API
- Works with any async function/closure
- Easy to store in collections

**Cons**:
- Heap allocation per job (small overhead)
- Loss of zero-cost abstraction

### 2. Semaphore vs. Manual Counting

**Decision**: Use `tokio::sync::Semaphore`

**Pros**:
- Built-in async/await support
- Well-tested, production-ready
- Efficient, no busy-waiting
- Clear semantics

**Cons**:
- External dependency (minor, Tokio is standard)

### 3. Channel Size = 2 × Pool Size

**Decision**: Bounded channel with capacity 2× max_concurrent_jobs

**Rationale**:
- Small buffer reduces memory footprint
- 2× allows some job buffering without excessive queuing
- Enforces backpressure before memory issues arise

### 4. Single Worker Loop vs. Multiple Workers

**Decision**: Single worker loop with `tokio::select!`

**Pros**:
- Simpler state management
- No contention on job queue
- Clear shutdown semantics
- Efficient with Tokio's scheduler

**Cons**:
- Single point of coordination (not an issue with Tokio)

**Why it works**: Tokio's work-stealing scheduler distributes spawned tasks across threads. The worker loop itself is lightweight coordination logic, not actual computation.

### 5. FIFO Ordering

**Decision**: Jobs are processed in submission order (FIFO)

**Pros**:
- Predictable, fair behavior
- Simple to understand and test
- Matches most user expectations

**Cons**:
- No priority support
- Can't optimize for short jobs

## Known Limitations
1. No Job Cancellation
2. No Error Propagation
3. No Prioritization
4. Fixed Job type
5. Panic Handling
6. Resize During High Load

## Performance Characteristics

### Time Complexity
- `submit()`: O(N) where N = number of in-flight jobs
- `metrics()`: O(1) (Atomic loads)
- `resize()`: O(K) where K = permit difference

## Testing Strategy

### Unit Tests

Located in `src/worker_pool.rs`:
- Pool creation and configuration
- Single/multiple job submission
- Bounded concurrency verification
- Shutdown rejection
- Metrics accuracy
- Resize operations

### Integration Tests

Located in `tests/integration_tests.rs`:
- Backpressure behavior under load
- Graceful shutdown with in-flight work
- High concurrency stress (1000 jobs)
- Empty pool shutdown
- Job ordering (FIFO)
- Panic resilience



## Conclusion

This bounded async worker pool provides a solid foundation for concurrent job execution in Rust. It correctly implements backpressure, graceful shutdown, and bounded resource usage while maintaining a simple, ergonomic API.

The design prioritizes:
1. **Correctness**: No job loss, proper shutdown, bounded memory
2. **Simplicity**: Clear API, minimal complexity
3. **Performance**: Efficient primitives, low overhead
4. **Safety**: Safe Rust only, no data races

The implementation is production-ready for systems requiring controlled concurrent execution with backpressure.