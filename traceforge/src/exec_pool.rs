use crate::exec_graph::ExecutionGraph;
use crate::must::Must;
use crate::runtime::execution::Execution;
use crate::runtime::thread::continuation::{ContinuationPool, CONTINUATION_POOL};
use crate::{Config, Stats};
use log::{debug, trace};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::env;
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{sleep, JoinHandle};
use std::time::{Duration, Instant};

#[derive(PartialEq, Debug)]
enum ExecutionPoolWorkerState {
    // When the pool is first created, each worker is in Created until they
    // enter the worker_loop() function.
    Created,

    // If a worker is not executing and has not been shut down, it's Waiting.
    Waiting,

    // The worker is executing a task.
    Busy,

    // The worker has been notified to shut down the next time the loop restarts.
    Shutdown,
}

/// LockableWorkerState wraps the about ExecutionPoolWorkerState enum in a mutex
/// so that it can be mutated either by the worker_loop() or externally when the
/// Shutdown state is asserted by the pool.
type LockableWorkerState = Arc<Mutex<ExecutionPoolWorkerState>>;

/// SharedWorkerDeque is the backlog of ExecutionGraphs that are queued for
/// distribution to the workers. Queueing an Option::None tells the worker to
/// use the EG that is local to the worker's local TraceForge instance. In theory,
/// that should only happen once when the pool is created to start the first
/// worker.
type SharedWorkerDeque = Arc<Mutex<VecDeque<Option<ExecutionGraph>>>>;

/// CondBlocker is the condition variable that is used to signal sleeping
/// workers to pop the next job from the queue and process it.
type CondBlocker = Arc<Condvar>;

/// ExecutionPoolWorker is the struct that holds all of the processing context
/// information which is provided as arguments to the worker_loop() function.
///
struct ExecutionPoolWorker {
    thread_handle: Option<JoinHandle<()>>,
    worker_state: LockableWorkerState,
    thread_idx: usize,
    shared_queue: SharedWorkerDeque,
    loop_block_cond: CondBlocker,
    pool_can_drain: Arc<Mutex<bool>>,
    pool_exec_stats: Arc<Mutex<Stats>>,
    must_conf: Config,
    /// In order to make max_iterations work right with parallel exploration
    /// we have to count executions as they start, not as they are finished,
    /// otherwise we end up overshooting while draining the queue of
    /// revisits.
    exec_counter: Arc<Mutex<u64>>,
}

impl ExecutionPoolWorker {
    // There is a circular dependency here with being able to create the thread
    // and the arguments for the thread as a member of the type before the members
    // themselves are created, so thread_handle is initially set to None and then
    // explicitly instantiated via start().
    //
    pub fn new(
        thread_idx: usize,
        shared_queue: SharedWorkerDeque,
        loop_block_cond: CondBlocker,
        pool_can_drain: Arc<Mutex<bool>>,
        pool_exec_stats: Arc<Mutex<Stats>>,
        must_conf: &Config,
        exec_counter: Arc<Mutex<u64>>,
    ) -> Self {
        debug!("Created Worker [{}]", &thread_idx);

        Self {
            thread_handle: None,
            worker_state: Arc::new(Mutex::new(ExecutionPoolWorkerState::Created)),
            thread_idx,
            shared_queue,
            loop_block_cond,
            pool_can_drain,
            pool_exec_stats,
            must_conf: must_conf.clone(),
            exec_counter,
        }
    }

    // Annoyance: RefCell<> doesn't implement Send so the compiler won't let it
    // be passed into a thread. Thus, we clone or create everything here and then
    // let it be moved into the worker_loop.
    //
    pub fn start<F>(&mut self, exec_func: &Arc<F>)
    where
        F: Fn() + Send + Sync + 'static,
    {
        let thread_idx = self.thread_idx;
        let worker_state = self.worker_state.clone();
        let shared_queue = self.shared_queue.clone();
        let loop_block_cond = self.loop_block_cond.clone();
        let exec_func = exec_func.clone();
        let pool_exec_can_drain = self.pool_can_drain.clone();
        let pool_exec_stats = self.pool_exec_stats.clone();
        let must_conf = self.must_conf.clone();
        let exec_counter = self.exec_counter.clone();

        let thread_handle = std::thread::Builder::new()
            .name(format!("exec-pool-{}", &self.thread_idx))
            .spawn(move || {
                worker_loop(
                    thread_idx,
                    worker_state,
                    shared_queue,
                    loop_block_cond,
                    pool_exec_can_drain,
                    pool_exec_stats,
                    exec_func,
                    must_conf,
                    exec_counter,
                )
            })
            .expect("Should spawn() ExecutionPool worker thread.");

        self.thread_handle = Some(thread_handle);

        trace!("Started worker thread {}", &self.thread_idx);
    }
}

// The worker_loop function is not a member function of the pool worker, though
// logically it should be; but because the lifetime of the thread may exceed the
// lifetime of the ExecutionPoolWorker itself, Rust won't allow that. I chatted
// with some clue-wielding folks on #rust and they convinced me that this was the
// cleanest approach.
//
/// Store `new` into a worker's state unless the drainer has already set
/// `Shutdown`. `Shutdown` is issued exactly once (by `shutdown_now`) and
/// never re-issued, so a worker that overwrote it with `Waiting` or `Busy`
/// would never leave `worker_loop` and the join loop would never finish.
fn mark_unless_shutdown(state: &LockableWorkerState, new: ExecutionPoolWorkerState) {
    let mut guard = state.lock().expect("Lock worker_state mutex");
    if *guard != ExecutionPoolWorkerState::Shutdown {
        *guard = new;
    }
}

#[allow(clippy::too_many_arguments)]
fn worker_loop<F>(
    thread_idx: usize,
    worker_state: LockableWorkerState,
    shared_queue: SharedWorkerDeque,
    loop_block_cond: CondBlocker,
    pool_exec_can_drain: Arc<Mutex<bool>>,
    pool_exec_stats: Arc<Mutex<Stats>>,
    exec_func: Arc<F>,
    must_conf: Config,
    exec_counter: Arc<Mutex<u64>>,
) where
    F: Fn() + Send + Sync + 'static,
{
    // Don't create n times what you can create once.
    let wait_timeout_ms = Duration::from_millis(250);
    let max_iterations = must_conf.max_iterations;

    // Create a new TraceForge instance for each worker.
    let mut exec_must = Must::new(must_conf, false);
    exec_must.set_parallel_queues((shared_queue.clone(), loop_block_cond.clone()));
    let must_wrap = Rc::new(RefCell::new(exec_must));

    let continuation_pool = ContinuationPool::new();

    // Until the Worker is signalled to Shutdown...
    loop {
        if *worker_state.lock().expect("Lock worker_state mutex")
            == ExecutionPoolWorkerState::Shutdown
        {
            break;
        }

        if shared_queue
            .lock()
            .expect("Lock shared_queue mutex")
            .is_empty()
        {
            // Never overwrite Shutdown: the drainer may have issued it
            // between the emptiness check above and this store.
            mark_unless_shutdown(&worker_state, ExecutionPoolWorkerState::Waiting);

            let _timed_out = loop_block_cond
                .wait_timeout(
                    shared_queue.lock().expect("Couldn't provide queue mutex"),
                    wait_timeout_ms,
                )
                .expect("wait_timeout() failed");
        }

        if cfg!(debug_assertions) {
            let queue_depth = shared_queue
                .lock()
                .expect("locking shared queue mutex")
                .len();
            trace!(
                "[{}] Queue depth is {}, state is {:?}",
                thread_idx,
                queue_depth,
                *worker_state.lock().unwrap()
            );
        }

        // After the (potential) wait_timeout() above finishes, there still may
        // or may not be work queued. Pop the head of the queue and, if there
        // is work, mark the worker Busy while STILL holding the queue lock.
        //
        // The drainer decides that the run is over by observing "queue
        // empty and no worker Busy" under this same lock, so the pop and
        // the Busy transition must be one atomic step from its point of
        // view. With a gap between them the drainer could conclude
        // "everybody idle" while this worker held the last graph, issue
        // Shutdown, and have it clobbered by the Busy store: the run would
        // finish all its work and then never join its workers.
        //
        let next_eg = {
            let mut queue = shared_queue.lock().expect("locking shared queue mutex");
            let item = queue.pop_front();
            if item.is_some() {
                // If Shutdown is already set we still process the item
                // (nothing is lost) and leave the loop at the next check.
                mark_unless_shutdown(&worker_state, ExecutionPoolWorkerState::Busy);
            }
            item
        };

        // If there's no work, loop around and try again.
        //
        if next_eg.is_none() {
            trace!("[{}] No work to do.", thread_idx);
            continue;
        }

        // The queued object may or not contain an actual graph. If so,
        // add it to this worker's TraceForge queue. If this queue node does NOT
        // contain an EG, this signals the start token and it should use the
        // EG that's already associated with the worker's TraceForge instance.
        //
        if let Some(eg) = next_eg.unwrap() {
            if cfg!(debug_assertions) {
                trace!("[{}] is working on a provided EG.", thread_idx);
            }
            must_wrap.borrow_mut().reset_execution_graph(eg);
        } else {
            trace!("[{}] is working on a new EG.", thread_idx);
        }

        // Loop until the graph is done. Once the first graph successfully
        // completes, mark can_drain as true.
        //
        CONTINUATION_POOL.set(&continuation_pool, || loop {
            if let Some(limit) = max_iterations {
                let exec_c = {
                    let mut exec_c = exec_counter.lock().expect("Couldn't unlock exec_counter");
                    *exec_c += 1;
                    *exec_c
                };
                if exec_c > limit {
                    break; // Reached max iterations.
                }
            }

            let this_func = exec_func.clone();
            let execution = Execution::new(must_wrap.clone());
            Must::begin_execution(&must_wrap);

            // Unless we're in debug mode, don't pay the cost for collecting
            // and outputting runtimes.
            //
            if cfg!(debug_assertions) {
                trace!("[{}] is executing.", thread_idx);
                let start_time = Instant::now();
                execution.run(move || this_func());
                let end_time = Instant::now();
                trace!(
                    "[{}] is done executing, ran from {:?} to {:?} for {:?}",
                    thread_idx,
                    start_time,
                    end_time,
                    end_time.duration_since(start_time)
                );
            } else {
                execution.run(move || this_func());
            }

            *pool_exec_can_drain
                .lock()
                .expect("expect_pool_can_drain mutex") = true;
            if Must::complete_execution(&must_wrap) {
                break;
            }
        }); // loop until graph processing complete.

        // The worker is done on this graph.
        trace!("[{}] is done working.", thread_idx);
    } // loop until shutdown

    // Loop has been exited; shutdown must have been set.
    debug!("[{}] worker is shutdown.", thread_idx);

    let must_stats = must_wrap.borrow().stats();
    pool_exec_stats
        .lock()
        .expect("Can't lock stats mutex")
        .add(&must_stats);
} // worker_loop()

/// ExecutionPool is the main object to be instantiated.
///
pub struct ExecutionPool {
    worker_vec: Vec<ExecutionPoolWorker>,
    work_deque: SharedWorkerDeque,
    loop_block_cond: CondBlocker,
    can_drain: Arc<Mutex<bool>>,
    exec_stats: Arc<Mutex<Stats>>,
    is_shutdown: bool,
}

impl ExecutionPool {
    /// No more than this many items will be enqueued on the queue which serves
    /// the workers. The main reason for doing this is to avoid having the queue
    /// grow without bounds. When the queue gets full, the existing TraceForge serial
    /// code (local queue of revisits) is used instead, so no revisits get lost
    /// and nothing blocks. This is a classic form of **backpressure** which is
    /// always needed whenever there is a possibility that work can arrive at
    /// the queue faster than it can be processed by the workers.
    ///
    /// When testing on a nontrivial customer model with 16 worker threads:
    /// - an unlimited queue yields a 2x improvement over serial
    /// - a limited queue yields a 4x improvement over serial
    ///
    /// This increases the parallel utilization factor from about 12.5% to 25%
    /// which is still not great.
    ///
    /// The value of this limit seems to be very insensitive; I got nearly identical
    /// results with a queue size of 2, 10, 100, or 1000. I believe that the real
    /// value of this limit is to prevent the parallel revisit queue from consuming
    /// all system memory.
    pub const MAX_QUEUE_SIZE: usize = 100;

    pub fn new(must_conf: &Config) -> Self {
        let work_deque = Arc::new(Mutex::new(VecDeque::new()));
        let loop_block_cond = Arc::new(Condvar::new());
        let exec_stats = Arc::new(Mutex::new(Stats::default()));
        let can_drain = Arc::new(Mutex::new(false));
        let exec_counter = Arc::new(Mutex::new(0));

        let worker_count: usize = if let Some(rpw) = must_conf.parallel_workers {
            rpw
        } else if let Ok(rpw) = env::var("MUST_PARALLEL_WORKERS") {
            rpw.parse().unwrap()
        } else {
            num_cpus::get()
        };

        debug!("Using Execution Pool with {} workers.", worker_count);

        let worker_vec: Vec<ExecutionPoolWorker> = (0..worker_count)
            .map(|idx| {
                ExecutionPoolWorker::new(
                    idx,
                    work_deque.clone(),
                    loop_block_cond.clone(),
                    can_drain.clone(),
                    exec_stats.clone(),
                    must_conf,
                    exec_counter.clone(),
                )
            })
            .collect();

        Self {
            worker_vec,
            work_deque,
            loop_block_cond,
            exec_stats,
            can_drain,
            is_shutdown: false,
        }
    }

    pub fn explore<F>(&mut self, exec_func: &Arc<F>) -> Stats
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.worker_vec.iter_mut().for_each(|w| w.start(exec_func));

        debug!("Enqueuing the start token...");
        self.enqueue(<Option<ExecutionGraph>>::None);

        debug!("Draining and Shutting Down...");
        self.drain_and_shutdown();

        debug!("Done. Returning Stats.");
        self.exec_stats
            .lock()
            .expect("can't lock pool mutex")
            .clone()
    }

    /// Adds an Option<ExecutionGraph> to the shared queue and then calls
    /// notify_one() on the shared condition variable to wake up one of the
    /// Workers to process a queued graph. If enqueue() is called with
    /// Option<None>, then the default ExecutionGraph that comes bundled with
    /// on the TraceForge object is used. (Generally this should only be invoked to
    /// start the processing.
    ///
    pub(crate) fn enqueue(&mut self, rv: Option<ExecutionGraph>) {
        let mut work_deque = self
            .work_deque
            .lock()
            .expect("Couldn't lock work deque mutex");

        if self.is_shutdown {
            panic!("Shouldn't enqueue() after shutdown() invoked.");
        }

        work_deque.push_back(rv);

        trace!("Pushed execution, queue size now {}", work_deque.len());

        self.loop_block_cond.notify_one();
    }

    /// This function blocks until all of the workers are not in the Busy state
    /// and until the shared_queue is empty; at which point shutdown_now() is
    /// called.
    ///
    pub fn drain_and_shutdown(&mut self) -> bool {
        loop {
            // Add the delay once here rather than at every branch/continue.
            sleep(Duration::from_millis(250));

            let can_drain = *self.can_drain.lock().expect("can_drain mutex lock");

            if !can_drain {
                debug!("can_drain not set yet ... ");
                continue;
            }

            // Evaluate "queue empty AND no worker Busy" as ONE observation
            // under the queue lock. Workers only become Busy while holding
            // that lock (see worker_loop), and new work is only ever pushed
            // by a Busy worker, so a snapshot taken here cannot be
            // invalidated between its two halves.
            let (depth, still_busy) = {
                let queue = self.work_deque.lock().expect("Couldn't lock deque mutex");
                let still_busy = self.worker_vec.iter().any(|w| {
                    *w.worker_state.lock().expect("worker vec mutex lock")
                        == ExecutionPoolWorkerState::Busy
                });
                (queue.len(), still_busy)
            };

            if depth > 0 {
                trace!("Draining ... deque depth still {depth}");
                continue;
            }

            if still_busy {
                debug!("Threads are still finishing ... ");
                continue;
            }

            // if depth is 0 and all the threads are waiting, we're done.
            debug!("Queue drained.");
            break;
        }

        self.shutdown_now()
    }

    /// This function immediately sets all of the workers to the Shutdown state
    /// to break them out of their worker_loop() after which this function can
    /// join() all the completed threads. This function returns whether or not
    /// all of the threads were joined (e.g. did any of them panic.)
    ///
    pub fn shutdown_now(&mut self) -> bool {
        self.is_shutdown = true;

        let mut threads_joined = 0;

        debug!("Shutting threads down...");
        self.worker_vec.iter_mut().for_each(|w| {
            *w.worker_state.lock().expect("worker vec mutex lock") =
                ExecutionPoolWorkerState::Shutdown
        });
        // Wake every worker parked in wait_timeout() so it sees Shutdown now
        // rather than after its 250 ms timeout.
        self.loop_block_cond.notify_all();

        // Not all the threads may be complete yet so join() the ones that are
        // ready and loop until all of the threads in the Vec have been set to
        // Option::None via .take().
        //
        loop {
            self.worker_vec.iter_mut().for_each(|w| {
                if let Some(busy_th) = &w.thread_handle {
                    if busy_th.is_finished() {
                        trace!("[{}] Joining ... ", &w.thread_idx);
                        let th = w.thread_handle.take().unwrap();
                        th.join().expect("Didn't join worker thread");
                        trace!("[{}] Joined. ", &w.thread_idx);
                        threads_joined += 1;
                    } else {
                        debug!("[{}] Not finished yet.", &w.thread_idx);
                    }
                }
            });

            // If any of the threads haven't completed, loop again; otherwise,
            // break out of the loop
            if let Some(busy_worker) = self.worker_vec.iter().find(|&w| w.thread_handle.is_some()) {
                trace!("[{}] Still isn't done. Looping().", &busy_worker.thread_idx);
                // Poll rather than spin: a worker may still be finishing
                // its last graph.
                sleep(Duration::from_millis(10));
            } else {
                trace!("All workers have completed and join()ed.");
                break;
            }
        } // loop

        threads_joined == self.worker_vec.len()
    } // shutdown_now()
}
