use crate::cons::Consistency;
use crate::event::Event;
use crate::exec_graph::{ExecutionGraph, RecvLike};
use crate::exec_pool::ExecutionPool;
use crate::revisit::{Revisit, RevisitEnum, RevisitPlacement};
use crate::future::PollerMsg;
use crate::loc::{Loc, WakeMsg};
use crate::runtime::failure::init_panic_hook;
use crate::runtime::task::TaskId;
use crate::telemetry::{Recorder, Telemetry};
use crate::vector_clock::VectorClock;
use crate::{event_label::*, ExecutionState, MonitorAcceptorFn, MonitorCreateFn};
use crate::{replay as REPLAY, Val};
use crate::{Config, ExplorationMode, SchedulePolicy, Stats};
use log::{debug, info, trace, warn};
use rand::distr::Distribution;
use rand::seq::IndexedRandom;
use rand::{RngExt, SeedableRng};
use rand_pcg::Pcg64Mcg;

use core::panic;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use crate::msg::Message;
use crate::thread::{main_thread_id, ThreadId};

#[cfg(feature = "symbolic")]
use crate::symbolic::SymbolicSolver;

use crate::monitor_types::{EndCondition, ExecutionEnd, Monitor, MonitorResult};
use std::any::TypeId;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::Write;

const EXECS: &str = "execs";
const BLOCKED: &str = "blocked";
const EXECS_EST: &str = "execs_est";
/// Assert violations whose graphs admit no consistent timeline
/// (relaxation artifacts of the legacy interval walker), suppressed by
/// the certification gate instead of being reported as counterexamples.
const SUPPRESSED_SPURIOUS: &str = "suppressed_spurious";

macro_rules! cast {
    ($target: expr, $pat: path) => {{
        if let $pat(a) = $target {
            a
        } else {
            std::io::stderr().flush().unwrap();
            panic!("mismatch variant when cast to {}", stringify!($pat));
        }
    }};
}

type RQueue = BTreeMap<usize, Vec<RevisitEnum>>;
type StateStack = Vec<MustState>;

#[derive(Clone, Serialize, Deserialize)]
pub struct MustState {
    graph: ExecutionGraph,
    rqueue: RQueue,
    /// Pending trigger for the completion-time feasibility gate: armed
    /// whenever a send is added in timed mode (any send can poison the
    /// graph's timeline under FIFO arrival coupling), cleared whenever
    /// a later FULL-GRAPH oracle build reports a feasible base
    /// (feasibility is monotone in the constraint set, so that build
    /// vouches for every earlier send). Lives in MustState, NOT Must:
    /// it describes THIS branch's graph and must be saved/restored
    /// with it (a clear leaking across a state switch would skip the
    /// gate on an unrelated, possibly infeasible graph). Fresh states
    /// start ARMED: one conservative re-check per branch completion.
    /// EVERY constructor arms it: `new`, the manual `Default` (used by
    /// `push_state`'s `mem::take` when a backward revisit takes over),
    /// and deserialization of pre-field replay states (serde default).
    #[serde(default = "arm_completion_check")]
    timed_completion_check: bool,
}

fn arm_completion_check() -> bool {
    true
}

impl Default for MustState {
    fn default() -> Self {
        Self::new()
    }
}

impl MustState {
    fn new() -> Self {
        Self {
            graph: ExecutionGraph::new(),
            rqueue: RQueue::new(),
            timed_completion_check: true,
        }
    }
}

thread_local! {
    /// This thread local variable stores the Must that is being used by the current
    /// thread's exploration. At present this is only used by the panic handler.
    ///
    /// The rest of the code gets Must by either calling ExecutionState::with(|s| s.must)
    /// or by just passing an Rc<RefCell<Must>> up and down the call stack.
    /// However, those don't work with the panic handler.
    ///
    /// In the future, we probably should change the code more so that Must is just
    /// a thread local static RefCell<Option<Must>>, and it's never passed up and
    /// down the stack anywhere, and is not stored inside ExecutionState either.
    ///
    /// Notes:
    /// 1. All must exploration happens on a single OS thread, even though Must presents
    /// the illusion of multiple threads.
    /// 2. However, during unit testing, Rust runs all tests on different threads at
    /// the same time concurrently, which means that this cannot be static, and
    /// we need to strictly avoid any kind of storage which is global such as
    /// passing Must into the panic handler.
    static CURRENT_MUST: RefCell<Option<Rc<RefCell<Must>>>> = const { RefCell::new(None) };
}

/// Information about the monitor
pub(crate) struct MonitorInfo {
    /// The thread id of the monitor
    pub thread_id: ThreadId,
    /// Packages up the sender and receiver in a message whose type is right for the monitor.
    pub create_fn: MonitorCreateFn,
    /// Returns true if the monitor accepts this message.
    pub acceptor_fn: MonitorAcceptorFn,
    /// The monitor's struct.
    /// This uses an Arc<Mutex<_>> to hold the monitor because the monitor's data will be
    /// used both inside the monitor thread (to receive messages) and at the end of the
    /// execution (from the Must thread). Only one of these accesses can be happening at once
    /// so we could have just used unsafe to share the data, but using Arc<Mutex<_>> shows
    /// the compiler that we are not doing anything that's ultimately unsafe
    pub monitor_struct: Arc<Mutex<dyn Monitor>>,
}

type ExecutionGraphEnqueuePair = (Arc<Mutex<VecDeque<Option<ExecutionGraph>>>>, Arc<Condvar>);

// No getters so that the borrow checker does not get confused
pub(crate) struct Must {
    states: StateStack,
    current: MustState,
    replay_info: REPLAY::ReplayInformation,
    checker: Consistency,
    pub config: Config,
    /// Assert violations recorded during the CURRENT execution of a
    /// timed run, judged at completion instead of at fire time: at the
    /// assert, threads that have not run yet (a backward revisit's
    /// cut-away sender, or simply a later-scheduled thread) can still
    /// add sends whose FIFO-coupled arrivals poison the timeline, so a
    /// full-graph feasibility verdict taken mid-execution is schedule
    /// dependent. Entries are (position, Some(task name) for
    /// keep_going_after_error mode / None for abort mode); drained at
    /// record_ending_telemetry.
    pending_asserts: Vec<(Event, Option<String>)>,
    monitors: BTreeMap<ThreadId, MonitorInfo>,
    rng: Pcg64Mcg,
    stop: bool,
    warn_limit: usize,
    pqueue: Option<ExecutionGraphEnqueuePair>,
    pub telemetry: Telemetry,
    published_values: BTreeMap<(ThreadId, TypeId), Val>,
    pub started_at: Instant,

    // Named nondeterministic choice support
    // Per-choice-name thread indexing: each choice name has independent thread indices
    // Frozen mapping used to ensure consistent thread indices across all executions.
    // Keys are origination_vecs (spawn lineage paths) which are stable across executions,
    // unlike ThreadIds which can change when scheduling decisions differ.
    pub(crate) frozen_thread_index_map: Option<HashMap<String, HashMap<Vec<u32>, usize>>>,
    // Current execution's mapping (built during first execution, then copied from frozen)
    // Map: choice_name -> (origination_vec -> thread_idx)
    pub(crate) thread_index_map: HashMap<String, HashMap<Vec<u32>, usize>>,
    // Next available index for each choice name
    pub(crate) next_thread_index: HashMap<String, usize>,
    // Per-execution counters: (choice_name, thread_idx) -> occurrence count
    pub(crate) choice_occurrence_counters: HashMap<(String, usize), usize>,
    #[cfg(feature = "symbolic")]
    // Solver for the symbolic constraints in the current execution.
    symbolic_solver: SymbolicSolver,
    // Cache for global named choices: once resolved, the same value is returned for all threads
    pub(crate) global_named_choices: HashMap<String, bool>,
    // Maximum number of events across all complete (non-blocked) execution graphs
    max_graph_events: usize,

    /// Dedup set for prune-log records: Cleared at the start of every
    /// new execution. Only used when `config.prune_log_file` is set.
    pub(crate) prune_dedup: HashSet<(&'static str, Event, Event)>,
}

impl Must {
    pub(crate) fn new(conf: Config, replay_mode: bool) -> Self {
        let seed = conf.seed;
        if conf.schedule_policy == SchedulePolicy::Arbitrary
            || conf.mode == ExplorationMode::Estimation
        {
            info!("Random schedule seed: {:?}", seed);
        }
        let telemetry = Telemetry::new(conf.keep_per_execution_coverage);
        let _ = telemetry.register_counter(&EXECS.to_owned());
        let _ = telemetry.register_counter(&BLOCKED.to_owned());
        let _ = telemetry.register_counter(&SUPPRESSED_SPURIOUS.to_owned());
        let _ = telemetry.register_histogram(&EXECS_EST.to_owned());

        Self {
            states: Vec::new(),
            current: MustState::new(),
            replay_info: REPLAY::ReplayInformation::new(conf.clone(), replay_mode),
            checker: Consistency {},
            config: conf,
            monitors: BTreeMap::new(),
            rng: Pcg64Mcg::seed_from_u64(seed),
            stop: false,
            warn_limit: 1,
            pending_asserts: Vec::new(),
            pqueue: None,
            telemetry,
            published_values: BTreeMap::new(),
            started_at: Instant::now(),
            frozen_thread_index_map: None,
            thread_index_map: HashMap::new(),
            next_thread_index: HashMap::new(),
            choice_occurrence_counters: HashMap::new(),
            #[cfg(feature = "symbolic")]
            symbolic_solver: SymbolicSolver::new(),
            global_named_choices: HashMap::new(),
            max_graph_events: 0,
            prune_dedup: HashSet::new(),
        }
    }

    /// Resets the Must instance for a new sample exploration.
    /// This avoids reallocating the entire Must struct between samples,
    /// reducing heap fragmentation and memory overhead.
    pub(crate) fn reset_for_sample(&mut self, seed: u64) {
        self.states.clear();
        self.current = MustState::new();
        self.monitors.clear();
        self.published_values.clear();
        self.stop = false;
        self.warn_limit = 1;
        self.config.seed = seed;
        self.rng = Pcg64Mcg::seed_from_u64(seed);
        self.replay_info = REPLAY::ReplayInformation::new(self.config.clone(), false);
        self.telemetry = Telemetry::default();
        let _ = self.telemetry.register_counter(&EXECS.to_owned());
        let _ = self.telemetry.register_counter(&BLOCKED.to_owned());
        let _ = self.telemetry.register_counter(&SUPPRESSED_SPURIOUS.to_owned());
        let _ = self.telemetry.register_histogram(&EXECS_EST.to_owned());
        self.frozen_thread_index_map = None;
        self.thread_index_map.clear();
        self.next_thread_index.clear();
        self.choice_occurrence_counters.clear();
        #[cfg(feature = "symbolic")]
        self.symbolic_solver.reset();
        self.global_named_choices.clear();
    }

    pub(crate) fn gen_bool(&mut self) -> bool {
        self.rng.random_range(0..=1) == 0
    }

    pub(crate) fn current() -> Option<Rc<RefCell<Must>>> {
        CURRENT_MUST.with(|current_must| current_must.borrow().clone())
    }

    pub(crate) fn set_current(must: Option<Rc<RefCell<Self>>>) {
        CURRENT_MUST.with(|current_must| {
            *current_must.borrow_mut() = must;
        });
    }

    pub(crate) fn begin_execution(must: &Rc<RefCell<Must>>) {
        let mut must = must.borrow_mut();
        #[cfg(feature = "symbolic")]
        must.symbolic_solver.reset();
        must.current.graph.initialize_for_execution();
        must.telemetry.coverage.new_eid();

        // Reset per-execution state for named choices
        // Initialize frozen mapping if not yet created (first execution)
        if must.frozen_thread_index_map.is_none() {
            must.frozen_thread_index_map = Some(HashMap::new());
            debug!("Initialized empty frozen thread index mapping for incremental freezing");
        }

        // Restore from frozen mapping (which grows incrementally as threads are discovered)
        let frozen_map = must.frozen_thread_index_map.as_ref().unwrap().clone();
        must.thread_index_map = frozen_map.clone();
        // Restore next_thread_index for each choice name
        must.next_thread_index = frozen_map
            .iter()
            .map(|(name, map)| (name.clone(), map.len()))
            .collect();

        if !frozen_map.is_empty() {
            let total_mappings: usize = frozen_map.values().map(|m| m.len()).sum();
            debug!("Restored frozen thread index mapping for {} choice names with {} total thread mappings",
                frozen_map.len(), total_mappings);
        }

        must.choice_occurrence_counters.clear();
        must.global_named_choices.clear();

        // TODO: when must is borrowed, the panic handler cannot capture
        // a counterexample. run_metrics_before() invokes must model code
        // that might panic, and it would be nice to refactor the code so that
        // a lock on Must is not held when calling run_metrics_before.
        must.run_metrics_before();
    }

    pub(crate) fn publish<T: Message + 'static>(&mut self, thread_id: ThreadId, val: T) {
        self.published_values
            .insert((thread_id, TypeId::of::<T>()), Val::new(val));
    }

    pub(crate) fn invoke_on_stop(monitor: &mut dyn Monitor) -> MonitorResult {
        let published_values =
            ExecutionState::with(|s| s.must.borrow_mut().published_values.clone());
        let execution_end = ExecutionEnd {
            condition: EndCondition::MonitorTerminated,
            published_values,
            _unused_lifetime: std::marker::PhantomData,
        };
        monitor.on_stop(&execution_end)
    }

    pub(crate) fn run_metrics_before(&mut self) {
        // Reset per-execution prune-dedup state so each execution gets
        // its own distinct set of rejected rfs in the log. Only does this
        // when the user opted into prune logging, otherwise
        // the set is empty and we'd be paying for nothing.
        if self.config.prune_log_file.is_some() {
            self.prune_dedup.clear();
        }
        let eid = self.telemetry.coverage.current_eid();
        for cb in &mut self
            .config
            .callbacks
            .lock()
            .expect("Could not lock callbacks")
            .iter_mut()
        {
            cb.before(eid);
        }
    }

    pub(crate) fn run_metrics_at_end(&mut self) {
        for cb in &mut self
            .config
            .callbacks
            .lock()
            .expect("Could not lock callbacks")
            .iter_mut()
        {
            cb.at_end_of_exploration();
        }
    }

    pub(crate) fn to_thread_id(&self, task_id: TaskId) -> ThreadId {
        self.current.graph.to_thread_id(task_id)
    }

    pub(crate) fn to_task_id(&self, tid: ThreadId) -> Option<TaskId> {
        self.current.graph.to_task_id(tid)
    }

    pub(crate) fn set_parallel_queues(&mut self, pq: ExecutionGraphEnqueuePair) {
        self.pqueue = Some(pq);
    }

    pub(crate) fn reset_execution_graph(&mut self, eg: ExecutionGraph) {
        self.current.rqueue.clear();
        self.states.clear();
        self.current.graph = eg;
        // Foreign graph (popped from the shared pool queue): arm the
        // completion-time feasibility gate conservatively, exactly as
        // `load_state_stack` does for the partitioned path and as
        // `push_state` does (via `MustState::new`) for the sequential
        // path. Without this the gate keeps whatever value the previous
        // graph left behind, which may be `false` after a feasible
        // full-graph oracle check, so a foreign branch that completes
        // without adding a send would be counted without its final
        // timeline re-check.
        self.current.timed_completion_check = true;
        #[cfg(feature = "symbolic")]
        self.symbolic_solver.reset();
    }

    /// Cheaply reset a Must instance for reuse by a new parallel task.
    /// Clears accumulated state (counters, states, monitors, telemetry)
    /// so stats start fresh for this task.
    pub(crate) fn reset_for_reuse(&mut self) {
        self.states.clear();
        self.current = MustState::new();
        self.monitors.clear();
        self.stop = false;
        self.published_values.clear();
        self.started_at = Instant::now();
        self.choice_occurrence_counters.clear();
        self.global_named_choices.clear();
        self.max_graph_events = 0;
        // Reset telemetry so stats() starts from zero for this task.
        self.telemetry = Telemetry::new(self.config.keep_per_execution_coverage);
        let _ = self.telemetry.register_counter(&EXECS.to_owned());
        let _ = self.telemetry.register_counter(&BLOCKED.to_owned());
        let _ = self.telemetry.register_counter(&SUPPRESSED_SPURIOUS.to_owned());
        let _ = self.telemetry.register_histogram(&EXECS_EST.to_owned());
        // Note: frozen_thread_index_map, thread_index_map, next_thread_index,
        // config, rng are intentionally NOT reset — they are either set
        // explicitly by the caller (frozen map) or persist across tasks.
    }

    /// Drain only the saved states (not current). Returns them as work items.
    /// Current state remains in place, untouched.
    pub(crate) fn drain_saved_states(&mut self) -> Vec<(ExecutionGraph, RQueue)> {
        self.states
            .drain(..)
            .map(|state| (state.graph, state.rqueue))
            .collect()
    }

    /// Check if the current state's revisit queue is empty.
    pub(crate) fn current_rqueue_empty(&self) -> bool {
        self.current.rqueue.is_empty()
    }

    /// Load a state stack with partitioned queues (for parallel workers).
    /// Each state gets its graph and a partitioned subset of its revisits.
    pub(crate) fn load_state_stack(&mut self, mut stack: Vec<(ExecutionGraph, RQueue)>) {
        self.states.clear();

        if stack.is_empty() {
            return;
        }

        // Pop the last entry, it becomes current
        let (last_graph, last_rqueue) = stack.pop().unwrap();
        self.current.graph = last_graph;
        self.current.rqueue = last_rqueue;
        // Foreign graph: arm the completion-time feasibility gate
        // conservatively (one re-check per branch completion).
        self.current.timed_completion_check = true;

        // Remaining entries become saved states (moved, not cloned)
        for (graph, rqueue) in stack {
            self.states.push(MustState {
                graph,
                rqueue,
                timed_completion_check: true,
            });
        }
    }

    /// Add the replay information to a fresh instance of Must
    pub(crate) fn load_replay_information(&mut self, replay_info: REPLAY::ReplayInformation) {
        self.replay_info = replay_info;
        self.current = self.replay_info.extract_error_state();
        self.config = self.replay_info.config();
    }

    /// Extract the replay information from a failing execution
    pub(crate) fn store_replay_information(&mut self, pos: Option<Event>) {
        println!("Random schedule seed: {:?}.", self.config().seed);

        if !self.replay_info.error_found() {
            let sorted_error_graph = self.current.graph.top_sort(pos);

            let replay_info = REPLAY::ReplayInformation::create(
                sorted_error_graph,
                self.current.clone(),
                self.config.clone(),
            );

            let error_trace_file = self.config.error_trace_file.as_ref();
            match error_trace_file {
                None => {
                    warn!("No counterexample trace will because Must is not configured with a filename. Use `Config::with_error_trace()`");
                }
                Some(f) => {
                    let mut file = File::create(f).unwrap();
                    match serde_json::to_string_pretty(&replay_info) {
                        Ok(replay_str) => {
                            writeln!(&mut file, "{}", replay_str).unwrap();
                        }
                        Err(err) => {
                            println!("Can't serialize graph to json: {}", err);
                        }
                    };
                    self.replay_info = replay_info;
                }
            }
        }
    }

    /// If the replayed event, i.e., `label` matches the `current_event`, it means
    /// that the `current_event` from the linearization has been replayed.
    /// So, now it's time to replay the next event from the linearization.
    fn try_consume(&mut self, label: &LabelEnum) {
        if self.replay_info.replay_mode() {
            if let Some(current_event) = self.replay_info.current_event() {
                if label.pos() == current_event.pos() {
                    // Playing the current event.
                    info!("|| Consuming {}", label);
                    self.replay_info.reset_current_event();
                } else {
                    std::io::stderr().flush().unwrap();
                    panic!(
                        "Replay failure: Executing {} instead of the counterexample's {}",
                        label.pos(),
                        current_event.pos()
                    );
                }
            }
        }
    }

    /// This function tries to consume the current event (if possible)
    /// and updates the graph with any field that was lost during (de)serialization.
    fn process_event(&mut self, label: LabelEnum) {
        self.current.graph.unreplayed_events.remove(&label.pos());
        self.try_consume(&label);
        self.recover_lost_data(label);
    }

    pub(crate) fn handle_register_mon(&mut self, monitor_info: MonitorInfo) {
        self.monitors.insert(monitor_info.thread_id, monitor_info);
    }

    /// `case e ∈ sleep(d)`: add the event to the graph and continue.
    /// The only effect is to advance the thread's local clock.
    pub(crate) fn handle_sleep(&mut self, slab: Sleep) {
        if self.is_replay(slab.pos()) {
            info!("| Replay Mode for sleep {}", slab);
            let lab = LabelEnum::Sleep(slab);
            self.current.graph.validate_replay_event(&lab);
            self.process_event(lab);
            return;
        }
        info!("| Handle Mode for {}", slab);
        self.add_to_graph(LabelEnum::Sleep(slab));
    }

    /// Returns the value read, if any, along with the rlab's receiving channel index, if any.
    /// Note: It can be that there is a "value" but no index (Val::default, during replay).
    pub(crate) fn handle_recv(
        &mut self,
        rlab: RecvMsg,
        blocking: bool,
    ) -> (Option<Val>, Option<usize>) {
        if self.is_replay(rlab.pos()) {
            info!("| Replay Mode for receive {}", rlab);
            // Try to see if the `current_event` matches `rlab`
            let pos = rlab.pos();
            let lab = LabelEnum::RecvMsg(rlab);
            self.current.graph.validate_replay_event(&lab);
            self.process_event(lab);

            // If the send that R reads from has a different reader R', assert that
            // R' is in a cancelled async receive, then fix up the reader.
            let g = &mut self.current.graph;
            let rlab = g.recv_label(pos).unwrap();
            if let Some(send_pos) = rlab.rf() {
                let slab = g.send_label(send_pos).unwrap();
                if let Some(reader) = slab.reader() {
                    if reader != pos {
                        // Verify R' is in a cancelled async receive (same check as cons.rs),
                        // OR that the mismatch is due to monitor message tracking.
                        // not a PollerMsg/WakeMsg, and a later event on R's thread reads
                        // from a PollerMsg::Cancel.
                        assert!(
                            slab.is_monitored_from(&pos.thread)
                            || slab.is_monitored_from(&reader.thread)
                            || (slab.val.as_any_ref().downcast_ref::<PollerMsg>().is_none()
                            && slab.val.as_any_ref().downcast_ref::<WakeMsg>().is_none()
                            && g.get_thr(&reader.thread).labels[(reader.index as usize + 1)..]
                                .iter()
                                .any(|lab| {
                                    if let LabelEnum::RecvMsg(recv) = lab {
                                        recv.rf().is_some_and(|rf| {
                                            if let LabelEnum::SendMsg(send) = g.label(rf) {
                                                send.val.as_any_ref().downcast_ref::<PollerMsg>()
                                                    .is_some_and(|msg| matches!(msg, PollerMsg::Cancel))
                                            } else {
                                                false
                                            }
                                        })
                                    } else {
                                        false
                                    }
                                })),
                            "Replay: send {} has reader {} but replaying receive {} and reader is not in a cancelled async receive",
                            send_pos, reader, pos
                        );
                        let slab = g.send_label_mut(send_pos).unwrap();
                        if slab.is_monitored_from(&pos.thread) {
                            // Monitor is replaying its receive; add as monitor reader
                            slab.add_monitor_reader(pos);
                        } else if slab.is_monitored_from(&reader.thread) {
                            // Existing reader is a monitor; move it to monitor readers
                            slab.add_monitor_reader(reader);
                            slab.set_reader(Some(pos));
                        } else {
                            slab.push_cancelled_recv_reader(reader);
                            slab.set_reader(Some(pos));
                        }
                    }
                }
            }

            let g = &self.current.graph;
            // Fetch it again, it might have been updated
            let rlab = g.recv_label(pos).unwrap();
            return (g.val_copy(pos), g.get_receiving_index(rlab));
        }
        info!("| Handle Mode for {}", rlab);

        let pos = self.add_to_graph(LabelEnum::RecvMsg(rlab));
        let val = self.visit_rfs(pos, blocking);
        self.current.graph.register_recv(&pos);
        let g = &self.current.graph;
        (
            val,
            g.recv_label(pos).and_then(|r| g.get_receiving_index(r)),
        )
    }

    pub(crate) fn handle_inbox(
        &mut self,
        ilab: Inbox,
    ) -> (Vec<Option<Val>>, Vec<Option<usize>>, bool) {
        if self.is_replay(ilab.pos()) {
            info!("| Replay Mode for receive {}", ilab);
            let mut ilab = ilab;

            if let Some(saved) = self.current.graph.inbox_label(ilab.pos()) {
                ilab.set_rf(saved.rfs());
            }

            let pos = ilab.pos();
            let lab = LabelEnum::Inbox(ilab);
            self.current.graph.validate_replay_event(&lab);
            self.process_event(lab);

            let g = &self.current.graph;
            let ilab = g.inbox_label(pos).unwrap();
            let vals = self.inbox_vals_copy(pos);
            return (vals, g.get_receiving_indexes(ilab), false);
        }

        info!("| Handle Mode for {}", ilab);

        let pos = self.add_to_graph(LabelEnum::Inbox(ilab));
        let vals = self.visit_inbox_rfs(pos);
        self.current.graph.register_inbox(&pos);
        let g = &self.current.graph;
        let blocked = matches!(g.label(pos), LabelEnum::Block(_));
        let indexes = match g.inbox_label(pos) {
            Some(il) => g.get_receiving_indexes(il),
            None => Vec::new(),
        };
        (vals, indexes, blocked)
    }

    // Returns the events that *might* be stuck waiting for the send,
    // in case this is a replay.
    pub(crate) fn handle_send(&mut self, slab: SendMsg) -> Vec<Event> {
        let spos = slab.pos();
        let mut stuck: Vec<Event> = Vec::new();
        if self.is_replay(spos) {
            info!("| Replay Mode for {} with reader {:?}", slab, slab.reader());
            let lab = LabelEnum::SendMsg(slab);
            self.current.graph.validate_replay_event(&lab);
            self.process_event(lab);

            // Wake up the tasks that (want to) read from this send
            let LabelEnum::SendMsg(slab) = self.current.graph.label(spos) else {
                unreachable!()
            };
            // The reader might be stuck waiting us, inform caller
            // to handle appropriately (has access to ExecutionState).
            if let Some(r) = slab.reader() {
                stuck.push(r)
            }
            // Similar for monitor readers
            slab.monitor_readers().iter().for_each(|&r| stuck.push(r));
            return stuck;
        }
        info!("| Handle Mode for {}", slab);

        trace!("[must.rs] Handling send at position {}", slab.pos());

        let pos = self.add_to_graph(LabelEnum::SendMsg(slab));
        trace!("[must.rs] Adding the system send {}", pos);

        // Consider dropping the send message
        // TODO: Estimation mode

        // TODO: Currently, we consider dropping the message at the time the send appears.
        // If there's no one to receive, we might be doing unnecessary work.
        // For models apart from Mailbox/TotalOrder, we could instead lazily
        // consider message drops implicitly at the time a receive is added:
        // receiving from a later send is equivalent to dropping the send.
        // For models apart from mailbox (?), checking consistency remains
        // polynomial but might require some caching to do it efficiently
        // (which sends have implicitly been dropped).
        let slab = self.current.graph.send_label(pos).unwrap();
        if slab.is_lossy() && self.dropped_messages() < self.config.lossy_budget {
            push_worklist(
                &mut self.current.rqueue,
                slab.stamp(),
                RevisitEnum::new_forward(pos, Event::new_init()),
            )
        }

        self.calc_revisits(pos);
        self.current.graph.register_send(&spos);

        // stuck is only used during replay
        assert!(stuck.is_empty());
        stuck
    }

    /// Returns the next thread id to use in thread creation.
    pub(crate) fn next_thread_id(&self, pos: &Event) -> ThreadId {
        let parent_tclab: TCreate = self.current.graph.get_thread_tclab(pos.thread);
        let mut origination_vec = parent_tclab.origination_vec();
        origination_vec.push(pos.index);
        self.current.graph.tid_for_spawn(pos, &origination_vec)
    }

    /// Returns the origination_vec for the given thread.
    pub(crate) fn thread_origination_vec(&self, tid: ThreadId) -> Vec<u32> {
        self.current.graph.get_thread_tclab(tid).origination_vec()
    }

    /// Returns the filtered_origination_vec for the given thread.
    pub(crate) fn thread_filtered_origination_vec_from_tid(&self, tid: ThreadId) -> Vec<u32> {
        self.current.graph.get_thread_tclab(tid).filtered_origination_vec()
    }

    /// Counts the number of TCreate events in the given thread up to and including
    /// the specified event index, excluding those whose names contain the filter pattern.
    fn count_filtered_tcreate_events(&self, thread: ThreadId, up_to_index: u32, filter_pattern: &str) -> u32 {
        let mut count = 0;
        let thread_size = self.current.graph.thread_size(thread) as u32;

        // Iterate only up to the minimum of up_to_index and the actual thread size - 1
        // (since we're currently adding a new event at up_to_index, it may not exist yet)
        let max_idx = up_to_index.min(thread_size.saturating_sub(1));

        for idx in 0..=max_idx {
            let event = Event::new(thread, idx);
            if let LabelEnum::TCreate(tclab) = self.current.graph.label(event) {
                // Check if this thread creation should be counted
                let should_count = if let Some(ref name) = tclab.name() {
                    !name.contains(filter_pattern)
                } else {
                    // Unnamed threads are counted
                    true
                };

                if should_count {
                    count += 1;
                }
            }
        }

        count
    }

    pub(crate) fn handle_tcreate(
        &mut self,
        tid: ThreadId,
        cid: TaskId,
        sym_cid: Option<ThreadId>,
        pos: Event,
        name: Option<String>,
        is_daemon: bool,
    ) {
        let parent_tclab: TCreate = self.current.graph.get_thread_tclab(pos.thread);
        let mut origination_vec = parent_tclab.origination_vec();
        origination_vec.push(pos.index);

        // Compute filtered_origination_vec
        let mut filtered_origination_vec = parent_tclab.filtered_origination_vec();
        let filtered_count = self.count_filtered_tcreate_events(
            pos.thread,
            pos.index,
            crate::FILTERED_THREAD_NAME_PATTERN
        );
        filtered_origination_vec.push(filtered_count);

        let tclab = TCreate::new(pos, tid, name, is_daemon, sym_cid, origination_vec, filtered_origination_vec);

        if self.is_replay(pos) {
            info!("| Replay Mode for {}", tclab);
            // Try to see if the `current_event` matches `tclab`
            self.current.graph.set_task_for_replay(tid, cid);
            let lab = LabelEnum::TCreate(tclab);
            self.current.graph.validate_replay_event(&lab);
            self.process_event(lab);
            return;
        }
        info!("| Handle Mode for {}", tclab);

        let spawn_pos = self.add_to_graph(LabelEnum::TCreate(tclab.clone()));
        assert_eq!(spawn_pos, pos);

        self.current.graph.add_new_thread(tclab, cid);
        let blab = Begin::new(Event::new(tid, 0), Some(spawn_pos), sym_cid);

        self.add_to_graph(LabelEnum::Begin(blab));
    }

    pub(crate) fn handle_tjoin(&mut self, tjlab: TJoin) -> Option<Val> {
        if self.is_replay(tjlab.pos()) {
            info!("| Replay Mode for {}", tjlab);
            // Try to see if the `current_event` matches `tjlab`
            let lab = LabelEnum::TJoin(tjlab.clone());
            self.current.graph.validate_replay_event(&lab);
            self.process_event(lab);
            return Some(
                cast!(
                    self.current.graph.thread_last(tjlab.cid()).unwrap(),
                    LabelEnum::End
                )
                .result()
                .clone(),
            );
        }
        info!("| Handle Mode for {}", tjlab);

        if self.current.graph.is_thread_complete(tjlab.cid()) {
            let cid = tjlab.cid();
            self.add_to_graph(LabelEnum::TJoin(tjlab));
            Some(
                cast!(self.current.graph.thread_last(cid).unwrap(), LabelEnum::End)
                    .result()
                    .clone(),
            )
        } else {
            self.add_to_graph(LabelEnum::Block(Block::new(
                tjlab.pos(),
                BlockType::Join(tjlab.cid()),
            )));
            None
        }
    }

    pub(crate) fn handle_tend(&mut self, elab: End) {
        if self.is_replay(elab.pos()) {
            info!("| Replay Mode for {}", elab);
            let lab = LabelEnum::End(elab);
            self.current.graph.validate_replay_event(&lab);
            self.process_event(lab);
            return;
        }
        info!("| Handle Mode for {}", elab);
        self.add_to_graph(LabelEnum::End(elab));
    }

    pub(crate) fn handle_unique(&mut self, nclab: Unique) -> Loc {
        let chan = nclab.get_loc();
        if self.is_replay(nclab.pos()) {
            info!("| Replay Mode for {}", nclab);
            let lab = LabelEnum::Unique(nclab);
            self.current.graph.validate_replay_event(&lab);
            self.process_event(lab);
            return chan;
        }
        info!("| Handle Mode for {}", nclab);
        self.add_to_graph(LabelEnum::Unique(nclab));
        chan
    }

    pub(crate) fn handle_ctoss(&mut self, ctlab: CToss) -> bool {
        if self.is_replay(ctlab.pos()) {
            info!("| Replay Mode for {}", ctlab);
            // Try to see if the `current_event` matches `ctlab`
            let lab = LabelEnum::CToss(ctlab.clone());
            self.current.graph.validate_replay_event(&lab);
            self.process_event(lab);
            if let LabelEnum::CToss(tclab) = self.current.graph.label(ctlab.pos()) {
                return tclab.result();
            }
            std::io::stderr().flush().unwrap();
            panic!();
        }
        info!("| Handle Mode for {}", ctlab);
        let maximal = ctlab.maximal();

        let pos = self.add_to_graph(LabelEnum::CToss(ctlab));
        let stamp = self.current.graph.label(pos).stamp();

        if self.config.mode == ExplorationMode::Estimation {
            return self.pick_ctoss(pos);
        }

        push_worklist(
            &mut self.current.rqueue,
            stamp,
            RevisitEnum::new_forward(pos, Event::new_init()),
        );
        maximal
    }

    /// Handle a CToss with a predetermined value. Similar to handle_ctoss but does not add revisits.
    pub(crate) fn handle_ctoss_predetermined(&mut self, mut ctlab: CToss, value: bool) -> bool {
        if self.is_replay(ctlab.pos()) {
            info!(
                "| Replay Mode for {} with predetermined value {}",
                ctlab, value
            );
            // In replay mode, validate the event exists and process it
            ctlab.set_result(value);
            ctlab.set_predetermined();
            let lab = LabelEnum::CToss(ctlab);
            self.current.graph.validate_replay_event(&lab);
            self.process_event(lab);
            // Return the predetermined value (ignoring what was in the graph)
            return value;
        }

        info!(
            "| Handle Mode for {} with predetermined value {}",
            ctlab, value
        );

        ctlab.set_result(value);
        ctlab.set_predetermined();
        self.add_to_graph(LabelEnum::CToss(ctlab));
        // Note: We don't add a revisit here because the value is predetermined
        value
    }

    pub(crate) fn handle_choice(&mut self, chlab: Choice) -> usize {
        let result = chlab.result();
        let end = *chlab.range().end();

        if self.is_replay(chlab.pos()) {
            info!("| Replay Mode for {}", chlab);
            // Try to see if the `current_event` matches `chlab`
            let lab = LabelEnum::Choice(chlab.clone());
            self.current.graph.validate_replay_event(&lab);
            self.process_event(lab);
            if let LabelEnum::Choice(tclab) = self.current.graph.label(chlab.pos()) {
                return tclab.result();
            }
            std::io::stderr().flush().unwrap();
            panic!();
        }
        info!("| Handle Mode for {}", chlab);

        let pos = self.add_to_graph(LabelEnum::Choice(chlab));
        let stamp = self.current.graph.label(pos).stamp();

        if self.config.mode == ExplorationMode::Estimation {
            return self.pick_choice(pos);
        }
        if result < end {
            // a revisit is needed only if the range has further elements
            push_worklist(
                &mut self.current.rqueue,
                stamp,
                RevisitEnum::new_forward(pos, Event::new_init()),
            );
        }
        result
    }

    pub(crate) fn handle_block(&mut self, blab: Block) {
        if self.is_replay(blab.pos()) {
            info!("| Replay Mode for {}", blab);
            let lab = LabelEnum::Block(blab);
            self.current.graph.validate_replay_event(&lab);
            self.process_event(lab);
            return;
        }
        self.add_to_graph(LabelEnum::Block(blab));
    }

    pub(crate) fn handle_sample<
        T: Clone + std::fmt::Debug + Serialize + for<'a> Deserialize<'a>,
        D: Distribution<T>,
    >(
        &mut self,
        pos: Event,
        distr: D,
        max_samples: usize,
    ) -> T {
        if self.is_replay(pos) {
            info!("| Replay mode for sample");
            let l = self.current.graph.label(pos);
            match l {
                LabelEnum::Sample(s) => {
                    let v = s.current().clone();
                    self.try_consume(&LabelEnum::Sample(s.clone())); // consume the next element in the trace being replayed
                    return serde_json::from_value(v).unwrap();
                }
                _ => panic!(),
            }
        }

        assert!(max_samples > 0);

        let mut it = self.rng.clone().sample_iter(distr);
        let first = it.next().unwrap();
        let rest = if max_samples == 1 {
            vec![]
        } else {
            it.take(max_samples - 2)
                .map(|val| serde_json::to_value(val).unwrap())
                .collect::<Vec<serde_json::Value>>()
        };
        let l = LabelEnum::Sample(Sample::new(
            pos,
            serde_json::to_value(first.clone()).unwrap(),
            rest,
        ));

        info!("| Handle Mode for {}", l);

        let pos = self.add_to_graph(l);

        if max_samples > 1 {
            let stamp = self.current.graph.label(pos).stamp();
            push_worklist(
                &mut self.current.rqueue,
                stamp,
                RevisitEnum::new_forward(pos, Event::new_init()),
            );
        }
        first
    }

    #[cfg(feature = "symbolic")]
    pub(crate) fn handle_symbolic_var(&mut self, lab: SymbolicVar) {
        if self.is_replay(lab.pos()) {
            let actual = LabelEnum::SymbolicVar(lab);
            self.current.graph.validate_replay_event(&actual);
            self.process_event(actual);
            return;
        }

        self.add_to_graph(LabelEnum::SymbolicVar(lab));
    }

    #[cfg(feature = "symbolic")]
    pub(crate) fn handle_constraint_eval(&mut self, mut lab: ConstraintEval) -> bool {
        if self.is_replay(lab.pos()) {
            let pos = lab.pos();

            let stored = match self.current.graph.label(pos).clone() {
                LabelEnum::ConstraintEval(c) => c,
                other => panic!("expected constraint at {}, got {}", pos, other),
            };

            let lab = LabelEnum::ConstraintEval(lab.clone());
            self.current.graph.validate_replay_event(&lab);
            self.process_event(LabelEnum::ConstraintEval(stored.clone()));
            self.add_constraint_to_path_solver(&stored);
            return stored.branch_taken();
        }

        let true_sat = self.symbolic_solver.sat_with(lab.expr());
        let false_sat = self.symbolic_solver.sat_with_not(lab.expr());

        if !true_sat && !false_sat {
            panic!(
                "both a constraint and its negation are unsatisfiable for {:?}",
                lab.expr()
            );
        }

        let chosen = true_sat;
        lab.set_branch_taken(chosen);

        let pos = self.add_to_graph(LabelEnum::ConstraintEval(lab.clone()));
        self.add_constraint_to_path_solver(&lab);

        if true_sat && false_sat {
            push_worklist(
                &mut self.current.rqueue,
                self.current.graph.label(pos).stamp(),
                RevisitEnum::new_forward(pos, Event::new_init()),
            );
        }

        chosen
    }

    #[cfg(feature = "symbolic")]
    fn add_constraint_to_path_solver(&mut self, c: &ConstraintEval) {
        if c.branch_taken() {
            self.symbolic_solver.assert(c.expr());
        } else {
            self.symbolic_solver.assert_not(c.expr());
        }
    }

    #[cfg(feature = "symbolic")]
    fn symbolic_solver_for_graph(&self, g: &ExecutionGraph) -> SymbolicSolver {
        let mut solver = SymbolicSolver::new();

        let mut labels = g
            .threads
            .iter()
            .flat_map(|t| t.labels.iter())
            .collect::<Vec<_>>();

        labels.sort_by_key(|lab| lab.stamp());

        for lab in labels {
            if let LabelEnum::ConstraintEval(c) = lab {
                if c.branch_taken() {
                    solver.assert(c.expr());
                } else {
                    solver.assert_not(c.expr());
                }
            }
        }

        solver
    }

    #[cfg(feature = "symbolic")]
    fn symbolic_backward_revisit_is_sat(&self, rev: &Revisit) -> bool {
        if !self.config.symbolic {
            return true;
        }

        let view = self.current.graph.revisit_view(rev);
        let mut g = self.current.graph.copy_to_view(&view);
        g.change_rf(rev.pos, Some(rev.rev));

        self.symbolic_solver_for_graph(&g).is_sat()
    }

    #[cfg(feature = "symbolic")]
    fn is_maximal_constraint(&self, c: &ConstraintEval, rev: &Revisit) -> bool {
        let view = self.current.graph.revisit_view(rev);
        let mut g = self.current.graph.copy_to_view(&view);
        g.change_rf(rev.pos, Some(rev.rev));

        let solver = self.symbolic_solver_for_graph(&g);

        let true_sat = solver.sat_with(c.expr());
        if true_sat {
            return c.branch_taken();
        }

        let false_sat = solver.sat_with_not(c.expr());
        if false_sat {
            return !c.branch_taken();
        }

        false
    }

    // this checks if the current graph is consistent
    // trivially true unless the semantics is Mailbox
    pub(crate) fn is_consistent(&self) -> bool {
        self.checker.is_consistent(&self.current.graph)
    }

    pub(crate) fn dropped_messages(&self) -> usize {
        self.current.graph.dropped_sends()
    }

    pub(crate) fn next_task(
        &mut self,
        runnable: &[(TaskId, usize)],
        _current: Option<TaskId>,
    ) -> Option<TaskId> {
        if self.is_stopped() {
            return None;
        }

        // If in replay mode, use the linearization to obtain the next thread
        // that must be executed
        if self.replay_info.replay_mode() {
            return self.replay_info.next_task().map(|tid| {
                self.to_task_id(tid)
                    .expect("task id not found in the execution graph!")
            });
        }

        let next = match self.config.schedule_policy {
            SchedulePolicy::LTR => runnable
                .iter()
                .find(|(t, i)| self.is_thread_runnable(t, i))
                .map(|(t, _)| t.to_owned()),
            SchedulePolicy::Arbitrary => runnable
                .sample(&mut self.rng, runnable.len())
                .find(|(t, i)| self.is_thread_runnable(t, i))
                .map(|(t, _)| t.to_owned()),
        };
        if next.is_some() {
            next
        } else {
            self.unblock_ready(runnable)
        }
    }

    fn is_thread_runnable(&self, t: &TaskId, i: &usize) -> bool {
        let thread_id = self.to_thread_id(*t);
        let g = &self.current.graph;

        // runnable when:
        match g.thread_last(thread_id).unwrap() {
            // Either the last event is Block and
            LabelEnum::Block(blab) => match blab.btype() {
                // it's an internal blocking and the instruction points
                // at least *2* instructions before it (see event_label::Block)
                BlockType::Join(_) | BlockType::Value(..) => (*i as u32) < blab.pos().index - 1,
                // it's a user blocking and the instruction points before it
                BlockType::Assume | BlockType::Assert => (*i as u32) < blab.pos().index,
            },
            // or the last event is not Block
            _ => true,
        }
    }

    fn unblock_ready(&mut self, runnable: &[(TaskId, usize)]) -> Option<TaskId> {
        let blocked = runnable
            .iter()
            .filter(|(t, _)| {
                let t = self.to_thread_id(*t);
                self.is_waiting_on_written(t) || self.is_waiting_on_finished(t)
            })
            .collect::<Vec<_>>();

        blocked
            .iter()
            .for_each(|task| self.current.graph.remove_last(self.to_thread_id(task.0)));

        blocked.first().map(|(t, _)| t.to_owned())
    }

    fn is_waiting_on_written(&self, t: ThreadId) -> bool {
        let g = &self.current.graph;
        if let LabelEnum::Block(blab) = g.thread_last(t).unwrap() {
            if blab.refuses_matching() {
                // GC refusal block: never wakes by design; the read
                // worlds are the sibling branches.
                return false;
            }
            if let BlockType::Value(loc, wait, min, comm, from_inbox) = blab.btype() {
                // Count sends that could actually be OFFERED to the woken
                // receive by the real rf assignment. Divergence between
                // this predicate and the offer path was the root cause of
                // the unblock/re-block livelock, so the candidate set
                // mirrors the exact path the wake-up hands over to:
                // recv-shaped blocks mirror `coherent_rfs_in_view`
                // (monitor reads allowed, porf-minimality for monitors),
                // inbox-shaped blocks mirror `coherent_inbox_rfs_in_view`
                // (NO monitor branch, sb-minimals hardcoded): structural
                // match, unread-ness, sb-minimality, then timed
                // feasibility. Unblocks once `min` such sends exist.
                let structurally: Vec<&SendMsg> = g
                    .matching_stores(loc)
                    .filter(|send| {
                        // Monitor reading from the send: availability is
                        // judged by monitor_readers alone (checker.rfs
                        // never consults reader() on this branch). The
                        // inbox offer path has no monitor branch, so an
                        // inbox-shaped block must not count these (a
                        // consumed monitored send is never offerable to
                        // an inbox and would wake the block forever).
                        (!from_inbox && send.can_be_monitor_read(&blab.pos()))
                            // Plain read: location matches, send is not
                            // cancelled, and not already read elsewhere
                            // (as in filter_available_sends_in_view with
                            // no view).
                            || (send.can_be_read_from(loc)
                                && !send.is_cancelled_wrt(blab.as_event_label())
                                && send.reader().map_or(true, |r| {
                                    // Offer-path parity: a consumed send is
                                    // re-available when its reader's async
                                    // receive was later cancelled.
                                    r == blab.pos()
                                        || Consistency::reader_cancelled_async(
                                            g, send, r, None,
                                        )
                                }))
                    })
                    .collect();
                // GC semantics: time-eligibility BEFORE the order choice,
                // mirroring the offer path in coherent_rfs_in_view. A send
                // this block can never read in any timeline is dropped, so
                // a time-dead front no longer suppresses live candidates
                // behind it. Mixed-mode contract: untimed blocks (wait =
                // None) carry no timing constraints. Timing applies during
                // replay too: a recorded blocked execution must stay
                // blocked (offer-side replay exemption covers rf choices,
                // not wake-ups). TotalOrder (Mailbox) keeps the old order
                // (minimality first): skips are incoherent there.
                // Eligibility pre-filter for every model except Mailbox
                // (TotalOrder keeps the old order below); eviction only
                // where an order exists to invert.
                let gc_order = *comm != crate::loc::CommunicationModel::TotalOrder;
                let gc_evict = gc_order
                    && *comm != crate::loc::CommunicationModel::NoOrder;
                let mut dcs = match &self.config.timed {
                    Some(cfg) if wait.is_some() => Some(crate::timed_dcs::TimedDcs::build(
                        g,
                        cfg,
                        None,
                        Some(blab.pos()),
                    )),
                    _ => None,
                };
                let structurally: Vec<&SendMsg> = if gc_evict && dcs.is_some() {
                    // GC eviction parity with the offer path.
                    structurally
                        .into_iter()
                        .filter(|b| {
                            !Consistency::send_overtaken_in_view(
                                g,
                                b,
                                blab.pos(),
                                None,
                            )
                        })
                        .collect()
                } else {
                    structurally
                };
                let structurally: Vec<&SendMsg> = match dcs.as_mut() {
                    // Probes return false on an infeasible base (a real
                    // verdict under FIFO coupling), so wake-up and offer
                    // stay in agreement by both pruning everything.
                    // Inbox-shaped blocks follow the completion-count
                    // rule, so their probe carries the exclusion dodges
                    // (audit 2026-08-09): a window-only wake would be
                    // looser than the offer filter and livelock.
                    Some(d) if gc_order => structurally
                        .into_iter()
                        .filter(|send| {
                            if *from_inbox {
                                d.probe_unblock_inbox_subset(
                                    blab.pos(),
                                    &[send.pos()],
                                    loc,
                                    *comm,
                                    inbox_block_at_capacity(wait, *min, 1),
                                )
                            } else {
                                d.probe_block_unblock(blab.pos(), send.pos())
                            }
                        })
                        .collect(),
                    _ => structurally,
                };
                // porf-override parity: recv-shaped blocks follow
                // checker.rfs (is_monitor); the inbox enumeration
                // hardcodes false (coherent_inbox_rfs_in_view), so
                // inbox-shaped blocks must too, even at min == 1.
                let porf_override = !from_inbox && self.is_monitor(&blab.pos());
                let candidates: Vec<&SendMsg> =
                    if *comm != crate::loc::CommunicationModel::NoOrder {
                        let minimals = Consistency::retain_sb_minimals(
                            structurally.iter().copied(),
                            porf_override,
                        );
                        // Dead-front unsealing parity with the offer
                        // path (2026-08-28), recv-shaped GC wakes only
                        // (the inbox member pool keeps its documented
                        // antichain semantics): a deeper eligible send
                        // wakes the block when some timeline dodges
                        // all its earlier fronts jointly with the read.
                        if gc_order && !*from_inbox {
                            let mut out = minimals;
                            if let Some(d) = dcs.as_mut() {
                                for &s in &structurally {
                                    if out.iter().any(|m| m.pos() == s.pos()) {
                                        continue;
                                    }
                                    let sview = if porf_override { s.porf() } else { s.sb() };
                                    let fronts: Vec<Event> = structurally
                                        .iter()
                                        .filter(|b| {
                                            b.pos() != s.pos() && sview.contains(b.pos())
                                        })
                                        .map(|b| b.pos())
                                        .collect();
                                    if d.probe_unblock_skipping(blab.pos(), s.pos(), &fronts) {
                                        out.push(s);
                                    }
                                }
                            }
                            out
                        } else {
                            minimals
                        }
                    } else {
                        structurally
                    };
                // NoOrder/TotalOrder: eligibility runs AFTER the order
                // choice (the pre-GC order), matching their offer paths.
                let candidates: Vec<&SendMsg> = match dcs.as_mut() {
                    Some(d) if !gc_order => candidates
                        .into_iter()
                        .filter(|send| {
                            if *from_inbox {
                                d.probe_unblock_inbox_subset(
                                    blab.pos(),
                                    &[send.pos()],
                                    loc,
                                    *comm,
                                    inbox_block_at_capacity(wait, *min, 1),
                                )
                            } else {
                                d.probe_block_unblock(blab.pos(), send.pos())
                            }
                        })
                        .collect(),
                    _ => candidates,
                };
                if candidates.len() < *min {
                    return false;
                }
                // Every candidate is individually feasible now; batches
                // (min >= 2) additionally need some min-subset readable
                // TOGETHER (two individually feasible sends with disjoint
                // lifetimes can never form one batch; waking on their
                // count livelocks).
                if *min >= 2 {
                    if let Some(d) = dcs.as_mut() {
                        return any_jointly_feasible_subset(
                            d,
                            blab.pos(),
                            &candidates.iter().map(|s| s.pos()).collect::<Vec<_>>(),
                            *min,
                            if *from_inbox { Some((loc, *comm)) } else { None },
                            // Batches here are exactly `min` long.
                            inbox_block_at_capacity(wait, *min, *min),
                        );
                    }
                }
                candidates.len() >= *min
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Certification gate for error reporting: with a timed config, a
    /// violation is reported only when the counterexample graph admits
    /// one consistent timeline (soundness of FIREs). The legacy
    /// interval walker can steer exploration into relaxation artifacts
    /// (interval endpoints assuming different times for one shared
    /// ancestor); those are suppressed here, counted under
    /// `suppressed_spurious`, and the execution continues as an
    /// ordinary blocked one. Under the exact engine every explored
    /// graph stays feasible by construction.
    /// Certification judges the FULL committed graph, not just the
    /// violation's porf prefix (audit 2026-08-09): the prefix omits
    /// concurrent matching senders, so an inbox batch no operational
    /// run produces could still be "certified" (the witness simply
    /// left out the excluded sender's thread). Judging the whole graph
    /// is sound: feasibility is monotone in the constraint set, so a
    /// prefix of a feasible branch is feasible, and every realizable
    /// violation fires again in the branch that realizes it, which
    /// this gate then passes. The printed witness stays causal (porf
    /// prefix), and exists whenever the full graph is feasible.
    /// Record an assert violation for completion-time judging (timed
    /// runs only; see the `pending_asserts` field doc). The caller has
    /// already installed the Block(Assert) label.
    pub(crate) fn defer_assert_report(&mut self, pos: Event, keep_going_name: Option<String>) {
        self.pending_asserts.push((pos, keep_going_name));
    }

    /// True when assert violations of this run must be deferred to
    /// completion instead of judged at fire time.
    pub(crate) fn defers_assert_reports(&self) -> bool {
        self.config.timed.is_some() && !self.replay_info.replay_mode()
    }

    /// Judge the assert violations recorded during the finished
    /// execution against the now-complete graph. A feasible consistent
    /// graph certifies them: keep-going entries are persisted, an
    /// abort-mode entry prints the graph + causal witness, stores the
    /// replay information, and panics (the original fire-time
    /// behavior, moved after the graph is whole). An infeasible or
    /// inconsistent graph suppresses them all.
    fn judge_pending_asserts(&mut self) {
        if self.pending_asserts.is_empty() {
            return;
        }
        let pending = std::mem::take(&mut self.pending_asserts);
        let certified = self.is_consistent()
            && self.config.timed.as_ref().is_none_or(|cfg| {
                crate::timed_dcs::TimedDcs::graph_feasible(&self.current.graph, cfg, None)
            });
        if !certified {
            for _ in &pending {
                self.telemetry.counter(SUPPRESSED_SPURIOUS.to_owned());
            }
            info!(
                "timed certification: suppressing {} counterexample(s) with no consistent timeline",
                pending.len()
            );
            return;
        }
        let mut abort_pos: Option<Event> = None;
        for (pos, name) in pending {
            match name {
                Some(name) => {
                    let message = crate::runtime::failure::persist_task_failure(name, Some(pos));
                    info!("Persisted failure {message}");
                }
                None => abort_pos = Some(pos),
            }
        }
        if let Some(pos) = abort_pos {
            info!("Error Detected!");
            println!("{}", self.print_graph(None));
            if let Some(witness) = self.timed_witness_report(Some(pos)) {
                println!("{witness}");
            }
            self.store_replay_information(Some(pos));
            use std::io::Write;
            std::io::stderr().flush().unwrap();
            panic!("assertion failed (certified timed counterexample; graph and witness above)");
        }
    }

    pub(crate) fn timed_error_report_allowed(&self, pos: Option<Event>) -> bool {
        // Replay exists to reproduce a recorded failure verbatim; the
        // gate must not re-judge it (a legacy-recorded artifact should
        // fail to reproduce loudly, not vanish into a silent success).
        if self.replay_info.replay_mode() {
            return true;
        }
        let Some(cfg) = &self.config.timed else {
            return true;
        };
        let g = &self.current.graph;
        let _ = pos; // witness printing stays causal; the gate is global
        let ok = crate::timed_dcs::TimedDcs::graph_feasible(g, cfg, None);
        if !ok {
            self.telemetry.counter(SUPPRESSED_SPURIOUS.to_owned());
            info!("timed certification: suppressing counterexample with no consistent timeline");
        }
        ok
    }

    /// Witness timeline of the current (feasible) graph, formatted for
    /// printing next to a certified counterexample.
    pub(crate) fn timed_witness_report(&self, pos: Option<Event>) -> Option<String> {
        let cfg = self.config.timed.as_ref()?;
        let g = &self.current.graph;
        let view = pos.map(|e| g.porf(e));
        let w = crate::timed_dcs::TimedDcs::graph_witness(g, cfg, view.as_ref())?;
        let mut out = String::from("Certified witness timeline (event @ time):\n");
        for (e, t) in w {
            out.push_str(&format!("  {e} @ {t}\n"));
        }
        Some(out)
    }

    fn is_waiting_on_finished(&self, t: ThreadId) -> bool {
        if let LabelEnum::Block(blab) = self.current.graph.thread_last(t).unwrap() {
            match blab.btype() {
                BlockType::Join(jlab) => self.current.graph.finished_threads.contains(jlab),
                _ => false,
            }
        } else {
            false
        }
    }

    fn block_exec(&mut self, bt: BlockType) {
        self.current.graph.thread_ids().iter().for_each(|&t| {
            self.add_to_graph(LabelEnum::Block(Block::new(
                self.current.graph.thread_last(t).unwrap().pos().next(),
                bt.clone(),
            )));
        });
    }

    fn stop(&mut self) {
        self.stop = true;
    }

    fn unstop(&mut self) {
        self.stop = false;
    }

    fn is_stopped(&self) -> bool {
        self.stop
    }

    /// Check if the execution is blocked. Return None if it's not blocked, or Some(Block)
    /// to tell why it is blocked.
    fn check_blocked(&mut self) -> Option<BlockType> {
        self.current.graph.check_blocked()
    }

    /// `complete_execution` is invoked when a particular single execution has finished.
    /// `complete_execution` returns false if there is another execution to do, or
    /// true if there is nothing more to explore.
    ///
    /// It takes a Rc<RefCell<Must>>, rather than &mut self, because it needs
    /// the ability to call into Must model code (the monitor on_stop) while
    /// not holding a reference to entire Must object.
    pub(crate) fn complete_execution(must: &Rc<RefCell<Must>>) -> bool {
        let maybe_block = must.borrow_mut().check_blocked();
        let exceeded_max_executions = must.borrow_mut().record_ending_telemetry(&maybe_block);

        let condition = match maybe_block {
            None => EndCondition::AllThreadsCompleted,
            Some(block) => match block {
                BlockType::Assume | BlockType::Assert => EndCondition::FailedAssumption,
                BlockType::Value(..) | BlockType::Join(_) => EndCondition::Deadlock,
            },
        };

        Must::call_on_stop_on_monitors(must, &condition);
        must.borrow_mut().published_values.clear();
        must.borrow_mut().call_telemetry_after(&condition);

        if exceeded_max_executions {
            return true; // no more executions.
        }

        must.borrow_mut().unstop();
        !must.borrow_mut().try_revisit()
    }

    fn record_ending_telemetry(&mut self, maybe_block: &Option<BlockType>) -> bool {
        // Deferred assert certification: the graph is complete now, so
        // the feasibility verdict is schedule independent (see the
        // pending_asserts field doc). May panic on a certified
        // violation, exactly like the old fire-time report.
        self.judge_pending_asserts();
        // Debug: print events that were not replayed during this execution.
        let unreplayed = &self.current.graph.unreplayed_events;
        if !unreplayed.is_empty() {
            let mut sorted: Vec<_> = unreplayed.iter().collect();
            sorted.sort();
            debug!("[DEBUG] Unreplayed events ({}):", sorted.len());
            for ev in &sorted {
                let label = self.current.graph.label(**ev);
                debug!("  {} -> {}", ev, label);
            }
        } else {
            debug!("[DEBUG] All events were replayed.");
        }
        let elapsed = Instant::now() - self.started_at;
        if maybe_block.is_some() {
            // Same feasibility gate as completed executions, needed
            // when a GC refusal block's constraints (or a post-commit
            // matching send) make the branch timeline-impossible: it
            // then counts as nothing at all.
            let has_refusal = self.current.graph.threads.iter().any(|t| {
                t.labels
                    .last()
                    .is_some_and(|l| matches!(l, LabelEnum::Block(b) if b.refuses_matching()))
            });
            let timed_impossible = (has_refusal || self.current.timed_completion_check)
                && self.config.timed.as_ref().is_some_and(|tcfg| {
                    !self.replay_info.replay_mode()
                        && !crate::timed_dcs::TimedDcs::graph_feasible(
                            &self.current.graph,
                            tcfg,
                            None,
                        )
                });
            if !timed_impossible && self.is_consistent() {
                self.telemetry.counter(BLOCKED.to_owned()); // increment BLOCKED
                let event_count: usize = self.current.graph.threads.iter().map(|t| t.labels.len()).sum();
                if event_count > self.max_graph_events {
                    self.max_graph_events = event_count;
                }
                if self.config.verbose >= 2 {
                    println!("One more blocked execution");
                    println!("{}", self.print_graph(None));
                    println!("Finished printing graph");
                } else if self.config.dot_out_blocked
                    && (self.config.dot_file.is_some() || self.config.trace_file.is_some())
                {
                    // Opt-in: write the dot/trace file for the blocked
                    // execution without emitting the graph to stdout.
                    // `print_graph` is the same routine the verbose
                    // path calls; we just discard its returned string.
                    let _ = self.print_graph(None);
                }
            }
        } else if self.is_consistent() {
            // Completion-time feasibility gate (audit 2026-08-09): a
            // backward revisit's cut view can hide a matching sender
            // from the inbox exclusion probes; re-execution then
            // rebuilds that sender into a graph whose constraint
            // system no timeline satisfies, and no later probe
            // re-checks it (infeasible-base probes keep-all by the
            // never-tighter convention). Count such a completion as
            // blocked instead: every counted execution then carries a
            // witness timeline.
            let timed_impossible = self.current.timed_completion_check
                && self.config.timed.as_ref().is_some_and(|tcfg| {
                    !self.replay_info.replay_mode()
                        && !crate::timed_dcs::TimedDcs::graph_feasible(
                            &self.current.graph,
                            tcfg,
                            None,
                        )
                });
            if timed_impossible {
                // No timeline satisfies the graph (routine under FIFO
                // arrival coupling: e.g. same-channel transit overrides
                // that would require overtaking): not a behavior, so it
                // counts as nothing at all, same as the blocked arm.
                if self.config.verbose >= 2 {
                    println!("One timeline-impossible completion (not counted)");
                    println!("{}", self.print_graph(None));
                }
            } else {
                self.telemetry.counter(EXECS.to_owned()); // increment EXECS
                let event_count: usize =
                    self.current.graph.threads.iter().map(|t| t.labels.len()).sum();
                if event_count > self.max_graph_events {
                    self.max_graph_events = event_count;
                }
                self.print_turmoil_trace();
                if self.config.verbose >= 1 {
                    println!("One more complete execution");
                    println!("{}", self.print_graph(None));
                }
            }
        }

        let num_execs = self.telemetry.read_counter(EXECS.to_owned()).unwrap_or(0);
        let num_blocked = self.telemetry.read_counter(BLOCKED.to_owned()).unwrap_or(0);
        let num_total = num_execs + num_blocked;
        let speed: String = if elapsed.as_secs() < 5 {
            "".to_string()
        } else {
            format!(" ({:.2}/sec)", num_total as f64 / elapsed.as_secs() as f64)
        };
        let progress_desc = format!(
            "Executions attempted so far: {} total {} finished normally {} blocked{}.",
            num_total, num_execs, num_blocked, speed
        );

        if self.config.progress_report > 0 {
            if num_total.is_multiple_of(self.config.progress_report as u64) {
                // Although it might be nice to use \r (carriage return) here to
                // repeatedly rewrite the same line with new progress reports, this
                // will eat up the last log line, and if the program is printing anything
                // else at all (very likely) then the goal of rewriting the same
                // line is defeated anyway.
                println!("{}", progress_desc);
                let _ = std::io::stdout().flush();
                eprintln!("{}", progress_desc);
                let _ = std::io::stderr().flush();
            }
        } else {
            // Implement P-style progress report, which reports
            // after 1, 2, 3, .... 10, 20, 30, ... 100, 200, 300, etc.
            if Self::should_report(num_total) {
                println!("{}", progress_desc);
            }
        }

        if let Some(n) = self.config.max_iterations {
            if n <= num_total {
                println!("Stopping exploration because max_iterations was reached.");
                return true; // done
            }
        }

        false // not done
    }

    pub(crate) fn should_report(n: u64) -> bool {
        if n == 0 {
            return false;
        }
        // Cap at every 1M once we reach that scale
        if n >= 1_000_000 {
            return n.is_multiple_of(1_000_000);
        }
        // Below that, use P-style: report at 1,2,..,9, 10,20,..,90, 100,200,..,900, etc.
        let mut p = n;
        while p.is_multiple_of(10) {
            p /= 10;
        }
        p < 10
    }

    /// All of the monitors on_stop functions and return an error if there is one.
    fn call_on_stop_on_monitors(must: &Rc<RefCell<Must>>, condition: &EndCondition) {
        // Allow panics in Monitor::on_stop to be caught.
        let _guard = init_panic_hook();

        if condition == &EndCondition::FailedAssumption {
            // Don't execute the monitor's on_stop since an assumption failed.
            return;
        }

        // Extract all of the monitors from the must.monitor's BTree.
        let mut monitors: Vec<MonitorInfo> = vec![];
        let mut mustp = must.borrow_mut();
        while let Some((_, monitor_info)) = mustp.monitors.pop_first() {
            if !mustp
                .current
                .graph
                .finished_threads
                .contains(&monitor_info.thread_id)
            {
                monitors.push(monitor_info);
            }
        }
        drop(mustp);

        let published_values = must.borrow().published_values.clone();
        let execution_end = ExecutionEnd {
            condition: condition.clone(),
            published_values,
            _unused_lifetime: PhantomData,
        };

        // Run the on_stop function for any monitors that did not already get terminated.
        // Note that we are not holding the lock on Must because we extracted the
        // monitors earlier.
        for monitor_info in monitors {
            let mut monitor = monitor_info.monitor_struct.lock().unwrap();
            let res = (*monitor).on_stop(&execution_end);
            if let Err(msg) = res {
                // Store the replay information first.
                must.borrow_mut().store_replay_information(None);
                println!("{}", must.borrow_mut().print_graph(None));
                std::io::stderr().flush().unwrap();
                panic!(
                    "\u{1b}[1;31mA monitor returned the message: {}\u{1b}[0m",
                    msg
                );
            }
        }
    }

    fn call_telemetry_after(&mut self, condition: &EndCondition) {
        // run all registered on-stop handlers with end condition and coverage information
        // This is not ideal that we are locking Must while calling them; we can't
        // generate a counterexample if they panic. OTOH, the callbacks should not.
        // A monitor provides a general solution for generating a counterexample at the end of
        // an execution.
        for cb in &mut self
            .config
            .callbacks
            .lock()
            .expect("Could not lock callbacks")
            .iter_mut()
        {
            cb.after(
                self.telemetry.coverage.current_eid(),
                condition,
                self.telemetry.coverage.export_current().into(),
            );
        }

        // Clean up per-execution coverage data after observers have been notified
        self.telemetry.coverage.cleanup_current_execution();
    }

    fn visit_rfs(&mut self, pos: Event, blocking: bool) -> Option<Val> {
        let mut rfs = self.checker.rfs(
            &self.current.graph,
            self.current.graph.recv_label(pos).unwrap(),
            self.is_monitor(&pos),
            self.timed_for_views(),
        );

        self.filter_symmetric_rfs(&mut rfs, pos);
        self.filter_timed_consistent_rfs(&mut rfs, pos);

        // At this point, we have handled all the cases for nonblocking receive
        // so we know blocking == true
        if !blocking {
            if !rfs.is_empty() {
                if self.config.mode == ExplorationMode::Estimation {
                    self.telemetry
                        .histogram(EXECS_EST.to_owned(), (rfs.len() + 1) as f64);

                    let idx = self.rng.random_range(0..=rfs.len());

                    info!("| Choosing {} out of {}", idx, rfs.len());

                    if idx < rfs.len() {
                        self.current.graph.change_rf(pos, Some(rfs[idx]));
                    } else {
                        self.current.graph.change_rf(pos, None);
                    }
                    return self.current.graph.val_copy(pos);
                } else {
                    rfs.iter().for_each(|&rf| {
                        push_worklist(
                            &mut self.current.rqueue,
                            self.current.graph.label(pos).stamp(),
                            RevisitEnum::new_forward(pos, rf),
                        );
                    });
                }
            }
            self.current.graph.change_rf(pos, None);
            return self.current.graph.val_copy(pos);
        }

        if !rfs.is_empty() {
            if self.config.mode == ExplorationMode::Estimation {
                self.telemetry
                    .histogram(EXECS_EST.to_owned(), rfs.len() as f64);

                let idx = self.rng.random_range(0..=(rfs.len() - 1));

                info!("| Choosing {} out of {}", idx, rfs.len());

                self.current.graph.change_rf(pos, Some(rfs[idx]));
            } else {
                debug!("Forward revisits at {}: {:?}", pos, rfs);
                // GC refusal sibling (audit 2026-08-09): a waiting
                // timed receive may ALSO never read: every matching
                // message can die before or while it waits. Push the
                // refusal branch when some timeline realizes it, and
                // push it FIRST so the LIFO pop applies it LAST at
                // this stamp (it converts the receive into a block;
                // every read alternative must already have run).
                // Worlds where the receive reads a later send are the
                // read siblings' backward revisits, so the refusal
                // block never wakes.
                let refusal_feasible = self.config.timed.is_some()
                    && !self.replay_info.replay_mode()
                    && !self.is_monitor(&pos)
                    && {
                        let rlab = self.current.graph.recv_label(pos).unwrap();
                        matches!(rlab.wait(), Some(crate::WaitTime::Infinite))
                            && Consistency::timed_gc_offer_arm(rlab.wait(), rlab.comm())
                            && {
                                let tcfg = self.config.timed.as_ref().unwrap();
                                let mut d = crate::timed_dcs::TimedDcs::build(
                                    &self.current.graph,
                                    tcfg,
                                    None,
                                    Some(pos),
                                );
                                d.base_feasible()
                                    && d.probe_gc_block(
                                        pos,
                                        rlab.as_event_label(),
                                        rlab.recv_loc(),
                                    )
                            }
                    };
                if refusal_feasible {
                    push_worklist(
                        &mut self.current.rqueue,
                        self.current.graph.label(pos).stamp(),
                        RevisitEnum::new_forward_block(pos),
                    );
                }
                self.current.graph.change_rf(pos, Some(rfs[0]));
                rfs.iter().skip(1).for_each(|&rf| {
                    push_worklist(
                        &mut self.current.rqueue,
                        self.current.graph.label(pos).stamp(),
                        RevisitEnum::new_forward(pos, rf),
                    );
                });
            }
            self.current.graph.val_copy(pos)
        } else {
            // Overwrites RecvMsg. Capture the original recv's wait
            // before the overwrite so the resulting Block can still tell
            // visualizers which kind of timed recv this was.
            let (loc, wait, comm) = {
                let rlab = self.current.graph.recv_label(pos).unwrap();
                (rlab.recv_loc().clone(), rlab.wait(), rlab.comm())
            };
            self.add_to_graph(LabelEnum::Block(Block::new(
                pos,
                BlockType::Value(loc, wait, 1, comm, false),
            )));
            None
        }
    }

    fn visit_inbox_rfs(&mut self, pos: Event) -> Vec<Option<Val>> {
        let ilab = self.current.graph.inbox_label(pos).unwrap().clone();
        // One oracle per inbox visit, shared between member eligibility
        // (inside inbox_rfs) and the joint subset probes below. Built
        // comm-independently: a TotalOrder timed inbox skips the cons
        // eligibility arm but still needs the oracle for subsets.
        let timed_cfg = self.config.timed.clone();
        let build_oracle = timed_cfg.is_some()
            && ilab.wait().is_some()
            && !self.replay_info.replay_mode();
        let mut oracle = if build_oracle {
            let d = crate::timed_dcs::TimedDcs::build(
                &self.current.graph,
                timed_cfg.as_ref().unwrap(),
                None,
                Some(pos),
            );
            if d.base_feasible() {
                // Full-graph feasible base: vouches for any pending
                // poisoned exclusion pair (see field doc).
                self.current.timed_completion_check = false;
            }
            Some(d)
        } else {
            None
        };
        let rfs = self.checker.inbox_rfs(
            &self.current.graph,
            &ilab,
            self.timed_for_views(),
            oracle.as_mut(),
        );

        let min = ilab.min();
        let max = ilab.max();
        let wait = ilab.wait();
        // A finite wait can time out (returning the empty set); an infinite
        // or untimed inbox cannot, and instead blocks until >= min messages
        // are available.
        let finite = matches!(wait, Some(crate::timed_cons::WaitTime::Finite(_)));

        // Timed inboxes now require `min >= 1` (enforced at the public API in
        // lib.rs); only the *untimed* non-blocking inbox still uses `min == 0`.
        // So a `min == 0` reaching this checker boundary always implies an
        // untimed (`wait == None`) inbox.
        debug_assert!(
            wait.is_none() || min >= 1,
            "timed inbox (wait = {wait:?}) must have min >= 1"
        );

        // Enumerate only the non-empty success subsets. An empty result is
        // represented separately, in one of two forms that share the return
        // value `{}` but differ in time:
        //   Some(vec![]) = immediate empty (untimed min==0 success, t = pred)
        //   None         = timeout  empty (finite wait, t = pred + W_r)
        // A timed inbox requires `min >= 1`, so its only empty is the
        // timeout (None); the immediate empty belongs to the untimed
        // non-blocking inbox alone. The `.max(1)` keeps the non-empty subset
        // floor at 1 for that untimed `min == 0` case (no-op when min >= 1).
        let mut combinations =
            compute_inbox_possible_subsets_from_rfs(&rfs, min.max(1), max, None);

        // Drop non-empty subsets that no consistent timeline admits.
        // Pure no-op when `config.timed` is `None` or when this inbox
        // was constructed without a wait time. The read is the last
        // event of its thread here, so the conjunctive probe is exact
        // (terminal-read equivalence, see timed_dcs::probe_inbox_rfs);
        // no graph mutation is needed. Replay reproduces recorded
        // subsets verbatim (parity with filter_timed_consistent_rfs).
        // Reuses the oracle built above (one build per inbox visit).
        {
            if let Some(dcs) = oracle.as_mut() {
                // An infeasible base is a REAL verdict under FIFO
                // arrival coupling: every subset is then exactly
                // infeasible and the probes prune them all.
                use crate::timed_dcs::prof;
                prof::add(&prof::RETAIN_CALLS, 1);
                prof::add(&prof::RETAIN_SUBSETS, combinations.len() as u64);
                let _t = prof::Timer::start(&prof::RETAIN_NS);
                combinations.retain(|subset| dcs.probe_inbox_rfs(pos, subset));
                prof::add(&prof::RETAIN_KEPT, combinations.len() as u64);
            }
        }

        if self.config.mode == ExplorationMode::Estimation {
            // Branch factor = the feasible outcome set of this inbox:
            //   min == 0 (untimed non-blocking): subsets + the immediate empty
            //   finite wait (min >= 1):          subsets + the timeout empty
            //   infinite / untimed blocking:     subsets only; empty => block
            // One outcome is sampled and committed; NO forward revisits
            // are pushed (previously every non-canonical subset was
            // pushed even in Estimation mode, so a "sample" re-ran the
            // whole inbox subtree while multiplying no factor for it:
            // the ~70x underestimate on the leader-election model).
            // Backward inbox revisits remain unsampled (see the TODO in
            // calc_revisits) and GC refusal classes carry no factor
            // (blocked worlds; the recv convention), so the estimator
            // stays a documented underestimate in those directions.
            let extra_empty = min == 0 || finite;
            if combinations.is_empty() && !extra_empty {
                self.add_to_graph(LabelEnum::Block(Block::new(
                    pos,
                    BlockType::Value(ilab.recv_loc().clone(), wait, min, ilab.comm(), true),
                )));
                return Vec::new();
            }
            let n = combinations.len() + usize::from(extra_empty);
            self.telemetry.histogram(EXECS_EST.to_owned(), n as f64);
            let idx = self.rng.random_range(0..n);
            info!("| Choosing {} out of {}", idx, n);
            let canonical = if idx < combinations.len() {
                Some(combinations.swap_remove(idx))
            } else if min == 0 {
                Some(Vec::new()) // immediate empty
            } else {
                None // timeout empty
            };
            self.current.graph.change_inbox_rfs(pos, canonical);
            return self.inbox_vals_copy(pos);
        }

        // Choose the canonical outcome; the rest become forward
        // inbox revisits.
        let mut revisits: Vec<Option<Vec<Event>>> = Vec::new();

        let canonical: Option<Vec<Event>> = if min == 0 {
            // Non-blocking UNTIMED inbox: the immediate empty `{}` is the
            // canonical (maximal) base. Timed inboxes now require `min >= 1`
            // (enforced in lib.rs), so `finite` is always false on this branch
            // and the timeout-empty push below is unreachable in-tree; it is
            // kept as a defensive no-op should a finite-wait `min == 0` inbox
            // ever be constructed internally.
            if finite {
                revisits.push(None);
            }
            for subset in combinations.drain(..) {
                revisits.push(Some(subset));
            }
            Some(Vec::new())
        } else if finite {
            // min >= 1, finite wait: the timeout `None` is the single canonical
            // (maximal) base and EVERY feasible non-empty subset is a forward
            // revisit - mirroring a non-blocking timed recv, whose timeout is
            // the maximal base and whose reads are all revisits. This pairs with
            // `inbox_reads_tiebreaker` treating only the timeout as maximal, so
            // the base execution holds the inbox in its maximal state and each
            // outcome has exactly one launch point (no duplicate executions).
            for subset in combinations.drain(..) {
                revisits.push(Some(subset));
            }
            None
        } else {
            // min >= 1, infinite / untimed: no timeout fallback, so the base is
            // the first `min` coherent sends when available, else the first
            // surviving subset, else the inbox blocks until >= min arrive.
            let default: Vec<Event> = rfs.iter().take(min).cloned().collect();
            let base = if combinations.iter().any(|s| *s == default) {
                Some(default)
            } else if let Some(first) = combinations.first().cloned() {
                Some(first)
            } else {
                // No feasible `min`-subset and no timeout fallback: block.
                self.add_to_graph(LabelEnum::Block(Block::new(
                    pos,
                    BlockType::Value(ilab.recv_loc().clone(), wait, min, ilab.comm(), true),
                )));
                return Vec::new();
            };
            // The remaining non-empty subsets become forward revisits.
            for subset in combinations.drain(..) {
                if Some(&subset) != base.as_ref() {
                    revisits.push(Some(subset));
                }
            }
            base
        };

        // GC refusal sibling for blocking (infinite-wait) inboxes,
        // mirroring the recv one in visit_rfs: the inbox may ALSO never
        // collect min messages, legal exactly in timelines where every
        // matching message dies before the wait begins. Exact for
        // min == 1 ("never reaches 1" IS "all dead"); for min >= 2 the
        // all-dead encoding is sound but conservative: disjoint-lifetime
        // refusal worlds are not separately enumerated (those with no
        // jointly-feasible subset already end in the empty-combinations
        // Block above). Reaching this point in the infinite arm implies
        // combinations were nonempty. Reuses the oracle built at the
        // top of this visit (its presence implies a timed config, a
        // timed wait, and non-replay); Estimation returned earlier.
        // Pushed FIRST so the LIFO pop applies it LAST at this stamp
        // (it converts the label kind; every read alternative must
        // already have run). No monitor gate: the inbox offer path has
        // no monitor branch. The branch adds blocked classes and never
        // loses or duplicates any: refusing Blocks are invisible to
        // calc_revisits and never wake, and worlds where the inbox
        // reads later sends are the read siblings' backward revisits
        // (full timed-inbox completeness remains gated on the inbox
        // backward-closure fix, tracked separately).
        let refusal_feasible = matches!(wait, Some(crate::timed_cons::WaitTime::Infinite))
            && Consistency::timed_gc_offer_arm(ilab.wait(), ilab.comm())
            && oracle.as_mut().is_some_and(|d| {
                d.base_feasible()
                    && d.probe_gc_block(pos, ilab.as_event_label(), ilab.recv_loc())
            });
        if refusal_feasible {
            push_worklist(
                &mut self.current.rqueue,
                self.current.graph.label(pos).stamp(),
                RevisitEnum::new_forward_block(pos),
            );
        }

        for placement in revisits.drain(..) {
            push_worklist(
                &mut self.current.rqueue,
                self.current.graph.label(pos).stamp(),
                RevisitEnum::new_forward_inbox(pos, placement),
            );
        }

        self.current.graph.change_inbox_rfs(pos, canonical);

        self.inbox_vals_copy(pos)
    }

    fn inbox_vals_copy(&self, pos: Event) -> Vec<Option<Val>> {
        match self.current.graph.vals_copy(pos) {
            Some(vs) => vs.into_iter().map(Some).collect(),
            None => Vec::new(),
        }
    }

    fn is_maximal_extension(&self, rev: &Revisit) -> bool {
        let g = &self.current.graph;
        let recv_stamp = g.label(rev.pos).stamp();

        let mut prefix = VectorClock::new();
        match &rev.rev {
            RevisitPlacement::Default(s) => prefix.update(g.send_label(*s).unwrap().porf()),
            RevisitPlacement::Inbox(sends) => {
                for &s in sends.iter().flatten() {
                    prefix.update(g.send_label(s).unwrap().porf());
                }
            }
            RevisitPlacement::BlockInstead => unreachable!("forward-only placement"),
        }

        // Any receive/inbox outside this protected prefix must remain maximal.
        for thread in g.threads.iter() {
            let i = thread
                .labels
                .partition_point(|lab| lab.stamp() <= recv_stamp || prefix.contains(lab.pos()));
            if thread.labels[i..]
                .iter()
                .any(|lab| !self.is_maximal(lab, rev))
            {
                return false;
            }
        }
        true
    }

    // computing the set of backward revisits for the send at position "pos"
    fn calc_revisits(&mut self, pos: Event) {
        let slab = self.current.graph.send_label(pos).unwrap();
        let stamp = slab.stamp();
        let g = &self.current.graph;

        info!(
            "[revisit/backward] computing revisits for send {} (thread {})",
            pos,
            slab.pos().thread
        );

        // Respect symmetry for plain receives, but keep symmetric sends if any inbox could read them.
        if self.config.symmetry {
            let flab = self.current.graph.thread_first(slab.pos().thread).unwrap();
            if flab.sym_id().is_some() && self.is_prefix_symmetric(flab.sym_id(), pos) {
                let has_inbox = g
                    .rev_matching_recvs(slab)
                    .any(|rl| matches!(rl, RecvLike::Inbox(_)));
                if !has_inbox {
                    return;
                }
            }
        }

        let send_porf = slab.porf();

        let mut revs: Vec<RevisitEnum> = Vec::new();

        for rl in g.rev_matching_recvs(slab) {
            // Revisits are only for receives/inboxes not already in send's porf prefix.
            if send_porf.contains(rl.pos()) {
                continue;
            }

            match rl {
                RecvLike::RecvMsg(r) => {
                    let rev = Revisit::new(r.pos(), pos);
                    if !self.is_maximal_recv(r, &rev) {
                        break;
                    }
                    if self
                        .checker
                        .is_revisit_consistent(
                            g,
                            r,
                            slab,
                            self.is_monitor(&r.pos()),
                            self.timed_for_views(),
                        )
                        && self.is_maximal_extension(&rev)
                    {
                        revs.push(RevisitEnum::BackwardRevisit(Revisit::new(r.pos(), pos)));
                    }
                }
                RecvLike::Inbox(i) => {
                    let seed_rev = Revisit::new_inbox(i.pos(), Some(vec![pos]));
                    // Backward revisits are generated only from maximal inbox events.
                    if !self.is_maximal_inbox(i, &seed_rev) {
                        break;
                    }

                    // collect candidate sends (including this new send), dedup
                    let mut cands: Vec<Event> = g
                        .matching_stores(i.recv_loc())
                        .map(|s| s.pos())
                        .filter(|&e| !g.send_label(e).unwrap().is_dropped())
                        .collect();
                    if !cands.contains(&pos) {
                        cands.push(pos);
                    }
                    cands.sort();
                    cands.dedup();

                    // Enumerate only subsets that include the freshly added send.
                    for mut subset in
                        compute_inbox_possible_subsets_from_rfs(&cands, i.min(), i.max(), Some(pos))
                    {
                        Consistency::normalize_event_set(&mut subset);
                        // Only generate the subset when the freshly added send
                        // is the owner (newest send in the subset).
                        if Consistency::inbox_owner(&self.current.graph, &subset) != Some(pos) {
                            continue;
                        }
                        info!(
                            "  [revisit/backward] inbox {} subset {}",
                            i.pos(),
                            self.fmt_event_set(&subset)
                        );
                        let rev_inbox = Revisit::new_inbox(i.pos(), Some(subset.clone()));
                        // Paper-style inbox revisit condition:
                        // keep only subsets that are consistent and preserve maximality.
                        if self.checker.is_revisit_consistent_inbox(g, i, &subset)
                            && self.is_maximal_inbox(i, &rev_inbox)
                            && self.is_maximal_extension(&rev_inbox)
                        {
                            revs.push(RevisitEnum::BackwardRevisit(rev_inbox));
                        }
                    }
                }
            }
        }

        // Drop backward revisits whose resulting graph would be timed
        // inconsistent. No-op when `timed` is `None`. Handles both
        // RecvMsg backward revisits (single rf swap) and Inbox backward
        // revisits (whole-subset swap).
        if let Some(cfg) = self.config.timed.clone() {
            // Only allocate the rejection-tracking Vec when prune logging
            // is opted into; otherwise this stays zero-cost.
            let track = self.config.prune_log_file.is_some();
            let mut rejected_revs: Vec<(Event, crate::timed_cons::TimeInterval)> =
                if track { Vec::new() } else { Vec::with_capacity(0) };
            // A backward revisit applies to the CUT graph
            // copy_to_view(revisit_view(rev)), so the exact probe must
            // be view-restricted; judging against the full graph would
            // import constraints from events the revisit deletes and
            // over-prune (completeness). Decided in an immutable
            // pre-pass (exact probes never mutate the graph). The
            // revisited read is the last event of its thread in the
            // cut view, so the conjunctive inbox probe is exact there
            // (terminal-read equivalence, see timed_dcs).
            let use_exact = !self.replay_info.replay_mode();
            let mut keep_exact: Vec<Option<bool>> = vec![None; revs.len()];
            if use_exact {
                let g = &self.current.graph;
                for (i, rev_enum) in revs.iter().enumerate() {
                    let RevisitEnum::BackwardRevisit(rev) = rev_enum else {
                        continue;
                    };
                    // NOTE: untimed inboxes used to short-circuit to
                    // Some(true) here ("no timing constraints to
                    // judge"). Under FIFO arrival coupling the CUT
                    // WORLD itself can be base-infeasible through
                    // poisoned sends the view drags in, so untimed
                    // inboxes are judged like everything else (their
                    // own label probe is transparent; the build's
                    // base feasibility is the real verdict).
                    let view = g.revisit_view(rev);
                    let mut dcs = crate::timed_dcs::TimedDcs::build(
                        g,
                        &cfg,
                        Some(&view),
                        Some(rev.pos),
                    );
                    if !dcs.base_feasible() {
                        // Real verdict under FIFO arrival coupling: the
                        // revisited cut world admits no timeline, so
                        // the revisit is exactly infeasible.
                        keep_exact[i] = Some(false);
                        if track {
                            rejected_revs.push((
                                rev.pos,
                                crate::timed_cons::TimeInterval::empty(),
                            ));
                        }
                        continue;
                    }
                    let ok = match &rev.rev {
                        RevisitPlacement::Default(_) => dcs.probe_recv_rf(rev.pos, pos),
                        RevisitPlacement::BlockInstead => {
                            unreachable!("forward-only placement")
                        }
                        RevisitPlacement::Inbox(sends) => match sends {
                            None => dcs.probe_inbox_timeout(rev.pos),
                            Some(v) => {
                                let mut sorted = v.clone();
                                sorted.sort();
                                dcs.probe_inbox_rfs(rev.pos, &sorted)
                            }
                        },
                    };
                    keep_exact[i] = Some(ok);
                    if !ok && track {
                        rejected_revs
                            .push((rev.pos, crate::timed_cons::TimeInterval::empty()));
                    }
                }
            }
            let mut rev_idx = 0usize;
            revs.retain(|rev_enum| {
                let i = rev_idx;
                rev_idx += 1;
                let RevisitEnum::BackwardRevisit(_) = rev_enum else {
                    return true;
                };
                // No exact verdict = replay (reproduce verbatim) or the
                // infeasible-cut-base tripwire: keep. Rejections were
                // recorded in the pre-pass.
                keep_exact[i].unwrap_or(true)
            });
            if track && !rejected_revs.is_empty() {
                self.log_backward_revisit_prunings(pos, &rejected_revs);
            }
        }

        // Estimation mode currently samples backward revisits for plain receives only
        if self.config.mode == ExplorationMode::Estimation {
            let recv_revs: Vec<Event> = revs
                .iter()
                .filter_map(|item| {
                    let RevisitEnum::BackwardRevisit(r) = item else {
                        return None;
                    };
                    match &r.rev {
                        RevisitPlacement::Default(send) if *send == pos => Some(r.pos),
                        // TODO: support inbox in estimation mode. Worlds
                        // reachable only via inbox BACKWARD revisits are never
                        // sampled and carry no factor: the estimator remains a
                        // documented underestimate on inbox programs whose
                        // sends arrive after the inbox visit (the forward
                        // fan-out IS sampled since the visit_inbox_rfs
                        // estimation arm landed).
                        _ => None,
                    }
                })
                .collect();

            self.pick_revisit(recv_revs, pos);
            return;
        }

        for rev in revs {
            info!(
                "  [revisit/backward] enqueue {}",
                self.fmt_revisit_item(&rev)
            );
            push_worklist(&mut self.current.rqueue, stamp, rev);
        }
    }

    // Return whether lab reads from a stamp-later send that would
    // be deleted from the revisit.
    fn revisited_by_deleted(&self, rlab: &RecvMsg, rev: &Revisit) -> bool {
        let g = &self.current.graph;
        // Union of PORF prefixes of the chosen sends for this revisit.
        let mut target_prefix = VectorClock::new();
        match &rev.rev {
            RevisitPlacement::Default(send) => {
                target_prefix.update(g.send_label(*send).unwrap().porf());
            }
            RevisitPlacement::BlockInstead => unreachable!("forward-only placement"),
            RevisitPlacement::Inbox(sends) => {
                for &s in sends.iter().flatten() {
                    target_prefix.update(g.send_label(s).unwrap().porf());
                }
            }
        }
        rlab.rf().is_some_and(|rf| {
            let stamp = g.label(rf).stamp();
            // Reads from stamp-later
            stamp > rlab.stamp() &&
                // Deleted from revisit:
                // stamp-after rev.pos
                stamp > g.label(rev.pos).stamp() &&
                // and not porf-before rev.rev
                !target_prefix.contains(rf)
        })
    }

    fn inbox_revisited_by_deleted(&self, lab: &Inbox, rev: &Revisit) -> bool {
        let g = &self.current.graph;
        // Union of PORF prefixes of the chosen sends for this revisit
        let mut target_prefix = VectorClock::new();
        match &rev.rev {
            RevisitPlacement::Inbox(sends) => {
                for &s in sends.iter().flatten() {
                    target_prefix.update(g.send_label(s).unwrap().porf());
                }
            }
            RevisitPlacement::Default(send) => {
                target_prefix.update(g.send_label(*send).unwrap().porf());
            }
            RevisitPlacement::BlockInstead => unreachable!("forward-only placement"),
        }

        match lab.rfs() {
            None => false, // nothing to delete
            Some(rfs) => rfs.iter().any(|&rf| {
                if !g.contains(rf) {
                    return false;
                }
                let rf_stamp = g.label(rf).stamp();
                // A chosen inbox read is "deleted" when it is later than both inbox and revisited
                // event and is not preserved by the revisit prefix.
                rf_stamp > lab.stamp()
                    && rf_stamp > g.label(rev.pos).stamp()
                    && !target_prefix.contains(rf)
            }),
        }
    }

    fn reads_tiebreaker(&self, rlab: &RecvMsg, rev: &Revisit) -> bool {
        self.checker.reads_tiebreaker(
            &self.current.graph,
            rlab,
            rev,
            self.is_monitor(&rlab.pos()),
            self.timed_for_views(),
        )
    }

    fn inbox_reads_tiebreaker(&self, ilab: &Inbox, rev: &Revisit) -> bool {
        self.checker.inbox_reads_tiebreaker(
            &self.current.graph,
            ilab,
            rev,
            self.timed_for_views(),
        )
    }

    fn is_monitor(&self, recv: &Event) -> bool {
        self.monitors.contains_key(&recv.thread)
    }

    /// Timed config handed to the consistency view construction (the GC
    /// eligibility rule). None during replay: recorded runs reproduce
    /// verbatim, parity with filter_timed_consistent_rfs.
    fn timed_for_views(&self) -> Option<&crate::timed_cons::TimedConfig> {
        if self.replay_info.replay_mode() {
            None
        } else {
            self.config.timed.as_ref()
        }
    }

    fn is_maximal_recv(&self, rlab: &RecvMsg, rev: &Revisit) -> bool {
        // Revisitable flag is a (faster) alternative to checking
        // if the sends deleted by a revisit are read by a stamp-earlier receive.
        !self.revisited_by_deleted(rlab, rev)
            && rlab.is_revisitable()
            && self.reads_tiebreaker(rlab, rev)
    }

    fn is_maximal_inbox(&self, ilab: &Inbox, rev: &Revisit) -> bool {
        // Inbox maximality follows the same structure as receive maximality:
        // no deleted later reads, still revisitable, and canonical reads tiebreaker holds.
        !self.inbox_revisited_by_deleted(ilab, rev)
            && ilab.is_revisitable()
            && self.inbox_reads_tiebreaker(ilab, rev)
    }

    fn is_maximal(&self, lab: &LabelEnum, rev: &Revisit) -> bool {
        match lab {
            LabelEnum::RecvMsg(rlab) => self.is_maximal_recv(rlab, rev),
            LabelEnum::Inbox(ilab) => self.is_maximal_inbox(ilab, rev),
            // Predetermined CToss events are always maximal: they are not branching
            // points, so no forward revisit exists to discover blocked backward revisits.
            LabelEnum::CToss(ctlab) => {
                ctlab.is_predetermined() || ctlab.result() == ctlab.maximal()
            }
            // Instead of checking if a send is read by a stamp-earlier receive,
            // we handle this via the revisitable flag on the corresponding receive.
            LabelEnum::SendMsg(slab) => !slab.is_dropped(),
            LabelEnum::Choice(chlab) => chlab.result() == *chlab.range().end(),
            #[cfg(feature = "symbolic")]
            LabelEnum::ConstraintEval(c) => self.is_maximal_constraint(c, rev),
            #[cfg(feature = "symbolic")]
            LabelEnum::SymbolicVar(_) => true,
            _ => true,
        }
    }

    /// Drop candidate sends whose `setRF(G, pos, s)` would be timed
    /// infeasible. When `config.timed` is `None` this is a no-op.
    ///
    /// One difference-constraint oracle is built per batch with `pos`
    /// floating, and each candidate is probed without mutating the
    /// graph; a candidate is dropped iff NO consistent timeline
    /// realizes the read (exact: maximal sound pruning). Every timed
    /// decision, inbox included, goes through the same exact oracle;
    /// the legacy interval walker lives only in the pre-timed-exact
    /// branch history.
    fn filter_timed_consistent_rfs(&mut self, rfs: &mut Vec<Event>, pos: Event) {
        let cfg = match self.config.timed.clone() {
            None => return,
            Some(c) => c,
        };
        let _t = crate::timed_dcs::prof::Timer::start(&crate::timed_dcs::prof::FILTER_NS);
        // Replay follows a recorded schedule; filters must never
        // out-prune it.
        if self.replay_info.replay_mode() {
            return;
        }
        // GC offers were already eligibility-filtered inside the view
        // construction (cons::coherent_rfs_in_view, SAME predicate), so
        // every survivor re-passes these probes by construction: skip
        // the redundant second oracle build. TotalOrder is exempt there
        // (seal semantics) and takes the full filter below. Two
        // documented deltas: the base-infeasible info! tripwire below no
        // longer prints for GC recvs (cons's keep-all arm is silent),
        // and forward prune-log rows for GC recvs were already empty
        // before this skip (cons rejects candidates before this filter
        // ever saw them).
        {
            let rlab = self.current.graph.recv_label(pos).unwrap();
            if Consistency::timed_gc_offer_arm(rlab.wait(), rlab.comm()) {
                // Drift tripwire (debug/test builds): if the cons arm
                // and this skip ever diverge, fail loudly.
                #[cfg(debug_assertions)]
                {
                    let g = &self.current.graph;
                    let mut dcs =
                        crate::timed_dcs::TimedDcs::build(g, &cfg, None, Some(pos));
                    if dcs.base_feasible() {
                        for &s in rfs.iter() {
                            debug_assert!(
                                dcs.probe_recv_rf(pos, s),
                                "GC-filtered offer {s} failed re-probe at {pos}"
                            );
                        }
                    }
                }
                return;
            }
        }
        // Only allocate the rejection-tracking Vec when prune logging is
        // opted into; otherwise this stays zero-cost.
        let track = self.config.prune_log_file.is_some();
        let mut rejected: Vec<(Event, crate::timed_cons::TimeInterval)> =
            if track { Vec::new() } else { Vec::with_capacity(0) };
        {
            let g = &self.current.graph;
            let mut dcs = crate::timed_dcs::TimedDcs::build(g, &cfg, None, Some(pos));
            if !dcs.base_feasible() {
                // Real verdict under FIFO arrival coupling: the
                // committed graph admits no timeline, so no candidate
                // does either. Prune them all; the completion gate
                // keeps the branch out of the counts.
                if track {
                    rejected.extend(rfs.iter().map(|&s| {
                        (s, crate::timed_cons::TimeInterval::empty())
                    }));
                }
                rfs.clear();
            } else {
            let kept: Vec<Event> = rfs
                .iter()
                .copied()
                .filter(|&s| {
                    let ok = dcs.probe_recv_rf(pos, s);
                    if !ok && track {
                        // Prune-log parity: the empty-interval sentinel
                        // (lo 1, hi 0) marks exact rejections; consumers
                        // key on `reason`.
                        rejected.push((s, crate::timed_cons::TimeInterval::empty()));
                    }
                    ok
                })
                .collect();
            *rfs = kept;
            }
        }
        if track && !rejected.is_empty() {
            self.log_timed_prunings("forward", pos, &rejected);
        }
    }

    /// Backward-revisit equivalent of `log_timed_prunings`. Each entry
    /// is a (recv, iv) pair where the rejected revisit would have re-paired
    /// `recv` with the newly-added send `new_send`.
    fn log_backward_revisit_prunings(
        &mut self,
        new_send: Event,
        rejected: &[(Event, crate::timed_cons::TimeInterval)],
    ) {
        // Reuse the same writer; format the record as a backward kind by
        // swapping recv/send roles.
        let mapped: Vec<_> = rejected.iter().map(|(r, iv)| (*r, *iv)).collect();
        // For the "backward" form the rejected pair is (r, new_send), not
        // (recv, candidate_send). Encode that distinction with a `kind`.
        self.log_timed_prunings_kind("backward", new_send, &mapped);
    }

    /// Append one JSONL record per pruned candidate to `prune_log_file`.
    fn log_timed_prunings(
        &mut self,
        kind: &'static str,
        recv: Event,
        rejected: &[(Event, crate::timed_cons::TimeInterval)],
    ) {
        self.log_timed_prunings_kind(kind, recv, rejected);
    }

    fn log_timed_prunings_kind(
        &mut self,
        kind: &'static str,
        anchor: Event,
        rejected: &[(Event, crate::timed_cons::TimeInterval)],
    ) {
        let path = match &self.config.prune_log_file {
            Some(p) => p.clone(),
            None => return,
        };
        // The in-progress execution is the next eid that EXECS will reach
        // when it completes, i.e. (current EXECS counter) + 1.
        let exec_done = self.telemetry.read_counter(EXECS.to_owned()).unwrap_or(0);
        let in_progress_eid = exec_done + 1;
        let mut buf = String::new();
        for (other, iv) in rejected {
            // For "forward": anchor is the receive, other is the rejected send.
            // For "backward": anchor is the new send, other is the receive
            // whose backward-revisit would have re-paired with the new send.
            let (recv, send) = if kind == "forward" {
                (anchor, *other)
            } else {
                (*other, anchor)
            };
            // Skip records we've already emitted in *this* execution. The
            // model checker re-runs filter_timed_consistent_rfs many
            // times along the worklist, so without this dedup the log can
            // grow to millions of duplicate lines for the same rejection.
            if !self.prune_dedup.insert((kind, recv, send)) {
                continue;
            }
            let line = format!(
                "{{\"eid\":{},\"kind\":\"{}\",\"recv\":\"{}\",\"send\":\"{}\",\"reason\":\"timed\",\"iv_lo\":{},\"iv_hi\":{}}}\n",
                in_progress_eid, kind, recv, send, iv.lo, iv.hi
            );
            buf.push_str(&line);
        }
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            use std::io::Write;
            let _ = f.write_all(buf.as_bytes());
        }
    }

    fn filter_symmetric_rfs(&self, rfs: &mut Vec<Event>, pos: Event) {
        assert!(self.current.graph.is_recv(pos) || self.current.graph.is_inbox(pos));

        let mut sym_rfs = HashSet::new();
        for rf in rfs.iter() {
            let blab = self.current.graph.thread_first(rf.thread).unwrap();
            if blab.sym_id().is_some()
                && rfs.iter().any(|rf2| {
                    rf2 != rf
                        && rf2.thread == blab.sym_id().unwrap()
                        && self.is_prefix_symmetric(blab.sym_id(), *rf)
                        && self.current.graph.label(*rf2).stamp()
                            < self.current.graph.label(*rf).stamp()
                })
            {
                sym_rfs.insert(*rf);
            }
        }
        rfs.retain(|rf| !sym_rfs.contains(rf));
    }

    fn is_prefix_symmetric(&self, sym_id: Option<ThreadId>, pos: Event) -> bool {
        if sym_id.is_none() {
            return false;
        }
        let tid = pos.thread;
        let sym_id = sym_id.unwrap();
        let sym_size = self.current.graph.thread_size(sym_id);
        let index = pos.index;
        if sym_size <= (index as usize) {
            return false;
        }
        (1..index).all(|i| {
            let lab = self.current.graph.label(Event::new(tid, i));
            let sym_lab = self.current.graph.label(Event::new(sym_id, i));
            match (lab, sym_lab) {
                // Two receives cannot be reading from the same send, so this
                // is false (unless they both timeout).
                // Checking for same-value, however, is not sound (see `symmetry_reduction.rs` test).
                (LabelEnum::RecvMsg(a), LabelEnum::RecvMsg(b)) => a.rf() == b.rf(),
                _ => true,
            }
        })
    }

    fn add_to_graph(&mut self, lab: LabelEnum) -> Event {
        let tid = lab.thread();
        let tindex = self.current.graph.thread_size(tid);
        if tindex > self.config.thread_threshold as usize && self.warn_limit > 0 {
            self.warn(&format!(
                "Large thread size {} events)! Is the test bounded?",
                tindex
            ));
            // debug
            eprintln!("Printing the large graph:");
            println!("{}", self.print_graph(None));
            // when a graph becomes too big, we can stop the search and return.
            // TODO: In principle, we should allow the exploration to proceed on the other threads.
            // TODO: We should implement this by adding a Block(TooBigThread) at the end of the
            // large thread but allowing other threads to proceed.
            // TODO: Needs scoping and work
            self.stop();
        }
        let pos = self.current.graph.add_label(lab);
        self.checker.calc_views(&mut self.current.graph, pos);
        if !self.current.timed_completion_check
            && self.config.timed.is_some()
            && !self.replay_info.replay_mode()
            && matches!(self.current.graph.label(pos), LabelEnum::SendMsg(_))
        {
            // Any send-add can poison the graph's timeline under FIFO
            // arrival coupling (its window can contradict the coupled
            // arrivals of already-committed same-channel sends), on top
            // of the original excluded-inbox-member hazard. Arm the
            // completion-time feasibility re-check; any later feasible
            // full-graph oracle build clears it (see the field doc).
            self.current.timed_completion_check = true;
        }
        pos
    }

    /// Recover data that was Default'd either
    /// a) during (de)serialization (counterexample replay, look for `#[serde(skip)]`, or
    /// b) explicitly (revisit replay, look for `set_pending()`)
    fn recover_lost_data(&mut self, label: LabelEnum) {
        let g = &mut self.current.graph;
        let pos = label.pos();
        match g.label_mut(pos) {
            LabelEnum::RecvMsg(rlab) => {
                if self.replay_info.replay_mode() {
                    if let LabelEnum::RecvMsg(new_rlab) = label {
                        rlab.recover_lost(new_rlab);
                    } else {
                        unreachable!();
                    }
                }
            }
            LabelEnum::Inbox(ilab) => {
                if let LabelEnum::Inbox(new_ilab) = label {
                    ilab.recover_lost(new_ilab);
                } else {
                    unreachable!();
                }
            }
            LabelEnum::SendMsg(slab) => {
                if let LabelEnum::SendMsg(new_slab) = label {
                    if self.replay_info.replay_mode() {
                        slab.recover_lost(new_slab);
                    } else {
                        slab.recover_val(new_slab);
                    }
                } else {
                    unreachable!();
                }
            }
            LabelEnum::End(elab) => {
                if let LabelEnum::End(new_elab) = label {
                    if self.replay_info.replay_mode() {
                        elab.recover_lost(new_elab);
                    } else {
                        elab.recover_result(new_elab);
                    }
                } else {
                    unreachable!();
                }
            }
            _ => {}
        }
        // Do *not* recover cache during trace replay:
        // It is, so far, not used, *and* we cannot guarantee
        // that they are sorted by stamp.
        // If we ever need the cache, without the order guarantee,
        // add a `register_send`, if `replay_mod()`.
    }

    pub(crate) fn try_revisit(&mut self) -> bool {
        loop {
            debug!("Finished execution with current rqueue {:?}", self.current.rqueue.clone());
            if self.current.rqueue.is_empty() {
                if self.try_pop_state() {
                    continue;
                }
                return false;
            }
            let rev = {
                pop_worklist(
                    &mut self.current.rqueue,
                    self.config.schedule_policy == SchedulePolicy::Arbitrary,
                    &mut self.rng,
                )
            };
            if self.config.verbose >= 3 {
                println!("Revisit {} <= {}", rev.pos(), rev.rev());
            }
            // Execute first feasible revisit; if skipped, continue polling worklist.
            if match &rev {
                RevisitEnum::ForwardRevisit(r) => self.forward_revisit(r),
                RevisitEnum::BackwardRevisit(r) => self.backward_revisit(r),
            } {
                return true;
            }
        }
    }

    fn forward_revisit(&mut self, rev: &Revisit) -> bool {
        let placement = self.fmt_revisit_placement(&rev.rev);
        info!("[revisit/forward] start {} <= {}", rev.pos, placement);
        let pos = rev.pos;
        let stamp = self.current.graph.label(pos).stamp();

        if matches!(self.current.graph.label(pos), LabelEnum::Inbox(_)) {
            if let RevisitPlacement::Inbox(sends) = &rev.rev {
                // For inbox forward revisits, validate the chosen subset in the
                // prefix first; if invalid, skip before mutating the current graph.
                let sends_slice: &[Event] = sends.as_deref().unwrap_or(&[]);
                let view = self.current.graph.view_from_stamp(stamp);
                let prefix = self.current.graph.copy_to_view(&view);
                let Some(inbox) = prefix.inbox_label(pos) else {
                    return false;
                };
                if !self
                    .checker
                    .is_revisit_consistent_inbox(&prefix, inbox, sends_slice)
                {
                    info!(
                        "  [revisit] skip inbox {} due to inconsistent subset {}",
                        pos,
                        self.fmt_event_set(sends_slice)
                    );
                    return false;
                }
            }
        }

        let lab = self.current.graph.label_mut(pos);

        match lab {
            LabelEnum::CToss(ctlab) => ctlab.set_result(!ctlab.result()),
            LabelEnum::Choice(chlab) => {
                let result = chlab.result();
                let end = *chlab.range().end();
                chlab.set_result(result + 1);

                if result + 1 < end {
                    // we have not reached the end yet, so set another revisit
                    push_worklist(
                        &mut self.current.rqueue,
                        stamp,
                        RevisitEnum::new_forward(pos, Event::new_init()),
                    );
                }
            }
            LabelEnum::Sample(sample) => {
                let more = sample.next();
                if more {
                    // we have not reached the end yet, so set another revisit
                    push_worklist(
                        &mut self.current.rqueue,
                        stamp,
                        RevisitEnum::new_forward(pos, Event::new_init()),
                    );
                }
            }
            LabelEnum::RecvMsg(rlab) => match &rev.rev {
                RevisitPlacement::BlockInstead => {
                    // Convert the receive into a GC refusal block,
                    // keeping its label base (stamp bookkeeping);
                    // detach the committed rf first so the send's
                    // reader mark is cleared, then recompute views.
                    let (loc, wait, comm) =
                        (rlab.recv_loc().clone(), rlab.wait(), rlab.comm());
                    self.current.graph.change_rf(pos, None);
                    let base = self
                        .current
                        .graph
                        .recv_label(pos)
                        .unwrap()
                        .as_event_label()
                        .clone();
                    *self.current.graph.label_mut(pos) = LabelEnum::Block(
                        Block::new_refusing(
                            base,
                            BlockType::Value(loc, wait, 1, comm, false),
                        ),
                    );
                    self.checker.calc_views(&mut self.current.graph, pos);
                }
                _ => self.change_rf(rev),
            },
            // Inbox revisits also go through change_rf, but replace a full send set.
            LabelEnum::Inbox(ilab) => match &rev.rev {
                RevisitPlacement::BlockInstead => {
                    // Convert the inbox into a GC refusal block, keeping
                    // its label base (stamp bookkeeping). Detach the
                    // committed subset FIRST: change_inbox_rfs asserts
                    // is_inbox and clears every member's reader edge;
                    // then swap the label and recompute views. The
                    // shared cut_to_stamp below closes the conversion.
                    let (loc, wait, min, comm) =
                        (ilab.recv_loc().clone(), ilab.wait(), ilab.min(), ilab.comm());
                    self.current.graph.change_inbox_rfs(pos, None);
                    let base = self
                        .current
                        .graph
                        .inbox_label(pos)
                        .unwrap()
                        .as_event_label()
                        .clone();
                    *self.current.graph.label_mut(pos) = LabelEnum::Block(
                        Block::new_refusing(
                            base,
                            BlockType::Value(loc, wait, min, comm, true),
                        ),
                    );
                    self.checker.calc_views(&mut self.current.graph, pos);
                }
                _ => self.change_rf(rev),
            },
            LabelEnum::SendMsg(slab) => {
                slab.set_dropped();
                self.current.graph.incr_dropped_sends();
            }
            #[cfg(feature = "symbolic")]
            LabelEnum::ConstraintEval(c) => {
                c.set_branch_taken(!c.branch_taken());
            }
            LabelEnum::Sleep(_) => unreachable!("sleep events are not revisitable"),
            _ => panic!(),
        };
        self.current.graph.cut_to_stamp(stamp);
        debug!("After cut");
        debug!("{}", self.print_graph(None));
        true
    }

    // Mark events in the porf-prefix as non revisitable
    fn mark_prefix_non_revisitable(&mut self, revisit_placement: RevisitPlacement) {
        let mut prefix = VectorClock::new();
        match revisit_placement {
            RevisitPlacement::Default(send) => {
                prefix.update(self.current.graph.send_label(send).unwrap().porf());
            }
            // A refusal adds no send: nothing new becomes non-revisitable.
            RevisitPlacement::BlockInstead => {}
            RevisitPlacement::Inbox(sends) => {
                // Inbox revisit prefix is the union of porf-prefixes of all chosen sends.
                for s in sends.into_iter().flatten() {
                    prefix.update(self.current.graph.send_label(s).unwrap().porf());
                }
            }
        }

        for thread in self.current.graph.threads.iter_mut() {
            let j = thread
                .labels
                .partition_point(|lab| prefix.contains(lab.pos()));
            for lab in &mut thread.labels[..j] {
                match lab {
                    LabelEnum::RecvMsg(rlab) => rlab.set_revisitable(false),
                    LabelEnum::Inbox(ilab) => ilab.set_revisitable(false),
                    _ => {}
                };
            }
        }
    }

    fn backward_revisit(&mut self, rev: &Revisit) -> bool {
        info!(
            "================ begin backward_revisit for {:?} ===================",
            rev
        );
        let v = self.current.graph.revisit_view(rev);
        let mut ng = self.current.graph.copy_to_view(&v);
        // If any send's reader was set to the revisited receive via
        // cancelled_recv_readers fallback, update it before change_rf.
        ng.pop_fallback_readers(rev.pos);
        // Save current state so alternative pending revisits remain explorable.
        self.push_state();
        self.current.graph = ng;

        self.mark_prefix_non_revisitable(rev.rev.clone());

        // println!("After marking prefix");

        self.change_rf(rev);

        // println!("After change rf");

        if self.config.verbose >= 3 {
            println!("After backward revisit graph");
            println!("{}", self.current.graph);
        }

        if let Some(pqueue_pair) = &self.pqueue {
            let mut queue = pqueue_pair
                .0
                .lock()
                .expect("Couldn't lock shared work queue");

            if queue.len() < ExecutionPool::MAX_QUEUE_SIZE {
                // Push this revisit onto the parallel revisit queue
                // and return false. This signals to the caller that this
                // worker can continue working on other local executions
                // that are available.
                queue.push_back(Some(self.current.graph.clone()));
                pqueue_pair.1.notify_one();
                return false;
            }
        }

        true
    }

    fn pick_ctoss(&mut self, pos: Event) -> bool {
        self.telemetry.histogram(EXECS_EST.to_owned(), 2.0);

        let toss = rand::rng().random_range(0..=1) == 0;
        cast!(self.current.graph.label_mut(pos), LabelEnum::CToss).set_result(toss);
        toss
    }

    fn pick_choice(&mut self, pos: Event) -> usize {
        let choice = cast!(self.current.graph.label_mut(pos), LabelEnum::Choice);
        let range = choice.range();
        let start = *range.start();
        let end = *range.end();
        let rand_value = rand::rng().random_range(start..=end);
        choice.set_result(rand_value);

        self.telemetry
            .histogram(EXECS_EST.to_owned(), (end - start + 1) as f64);
        rand_value
    }

    /// Change an rf according to the revisit
    fn change_rf(&mut self, rev: &Revisit) {
        match &rev.rev {
            RevisitPlacement::Default(vv) => {
                // Standard recv revisit: single rf edge.
                self.current.graph.change_rf(rev.pos, Some(*vv));
            }
            // BlockInstead is dispatched before change_rf (it replaces
            // the label, it does not change an rf).
            RevisitPlacement::BlockInstead => unreachable!(),
            RevisitPlacement::Inbox(vv) => {
                // Inbox revisit: whole set of chosen sends. `None` is the
                // timeout empty, `Some(vec![])` the immediate empty.
                match vv {
                    None => self.current.graph.change_inbox_rfs(rev.pos, None),
                    Some(v) => {
                        let mut vv_sorted = v.clone();
                        // Keep a canonical order for deterministic comparisons/printing.
                        vv_sorted.sort();
                        self.current
                            .graph
                            .change_inbox_rfs(rev.pos, Some(vv_sorted));
                    }
                }
            }
        }
    }

    fn pick_revisit(&mut self, revs: Vec<Event>, pos: Event) {
        self.telemetry
            .histogram(EXECS_EST.to_owned(), (revs.len() + 1) as f64);

        let idx = rand::rng().random_range(0..=revs.len());
        if idx < revs.len() {
            push_worklist(
                &mut self.current.rqueue,
                self.current.graph.label(pos).stamp(),
                RevisitEnum::new_backward(revs[idx], pos),
            );
            // Note: this code adds a Block with BlockType::Assume to the current execution.
            // This makes it seem like the Must model had "assume(false)" when in fact it does not.
            // This behavior only happens during Must `estimate` mode, where a random number is used
            // to pick some other revisit to execute instead of the current execution to simulate
            // the case that one of the other random revisits was chosen instead.

            // Using `BlockType::Assume` is an implementation detail which can leak out to the customer
            // in a couple ways--if they print out the execution graph they can see it, and if they
            // use a monitor, the monitor's EndCondition will be EndCondition::AssumeFailed.
            self.block_exec(BlockType::Assume); // Block this and revisit something else.
            self.stop();
        }
    }

    fn try_pop_state(&mut self) -> bool {
        if self.states.is_empty() {
            return false;
        }
        let state = self.states.pop().unwrap();
        self.current = state;
        true
    }

    fn push_state(&mut self) {
        self.states.push(std::mem::take(&mut self.current));
    }

    fn is_replay(&self, pos: Event) -> bool {
        self.current.graph.contains(pos)
    }

    fn warn(&mut self, msg: &str) {
        eprintln!("{}", msg);
        self.warn_limit -= 1;
        if self.config.warnings_as_errors {
            eprintln!("Exiting process because warnings_as_errors is set");
            std::process::exit(exitcode::DATAERR);
        }
    }

    pub(crate) fn stats(&self) -> Stats {
        Stats {
            execs: self.telemetry.read_counter(EXECS.into()).unwrap_or(0) as usize,
            block: self.telemetry.read_counter(BLOCKED.into()).unwrap_or(0) as usize,
            coverage: self.telemetry.coverage.export_aggregate().into(),
            max_graph_events: self.max_graph_events,
        }
    }

    pub(crate) fn execs_est(&self) -> f64 {
        self.telemetry
            .read_histogram(EXECS_EST.into())
            .unwrap_or(0.0)
    }

    pub(crate) fn config(&self) -> &Config {
        &self.config
    }

    pub(crate) fn monitors(&mut self) -> &mut BTreeMap<ThreadId, MonitorInfo> {
        &mut self.monitors
    }

    /// Prints the trace in Turmoil format
    pub(crate) fn print_turmoil_trace(&self) {
        if self.config.turmoil_trace_file.is_some() {
            let trace = self.current.graph.top_sort(None);

            let serialized_trace = trace.filter();
            let serialized_trace_str = serde_json::to_string(&serialized_trace).unwrap();

            let mut out_file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.config.turmoil_trace_file.as_ref().unwrap())
                .unwrap();

            std::io::Write::write(
                &mut out_file,
                format!("{}\n", serialized_trace_str).as_bytes(),
            )
            .unwrap();
        }
    }

    pub(crate) fn print_graph(&self, pos: Option<Event>) -> String {
        let out = if self.config.pretty_graph_printing {
            format!("{}", self.current.graph.pretty_display())
        } else {
            format!("{}", self.current.graph)
        };
        if self.config.dot_file.is_some() {
            self.print_graph_dot(pos)
                .expect("could not dot-print to supplied file");
        }
        if self.config.trace_file.is_some() {
            self.print_graph_trace(pos)
                .expect("could not print trace to supplied file");
        }

        out
    }

    fn print_graph_dot(&self, error: Option<Event>) -> std::io::Result<()> {
        let v = if let Some(event) = error {
            self.current.graph.porf(event)
        } else {
            self.current
                .graph
                .view_from_stamp(self.current.graph.stamp())
        };
        let num_execs = self.telemetry.read_counter(EXECS.to_owned()).unwrap_or(0);
        let create_file = error.is_some() || num_execs == 1;
        let mut out_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(create_file)
            .write(true)
            //.append(!create_file)
            .append(false)
            .open(self.config.dot_file.as_ref().unwrap())
            .unwrap();

        std::io::Write::write(
            &mut out_file,
            "strict digraph {\n\
            node [shape=plaintext]\n\
            labeljust=l\n\
            splines=false\n"
                .to_string()
                .as_bytes(),
        )?;

        let g = &self.current.graph;
        // Exact engine: one oracle for the whole dot dump; per-event
        // windows come from true shortest-path bounds instead of the
        // relaxed chain walk (None hi renders as infinity).
        let mut exact_dcs = self
            .config
            .timed
            .as_ref()
            .map(|cfg| crate::timed_dcs::TimedDcs::build(g, cfg, None, None));
        for (tid, ind) in v.entries() {
            std::io::Write::write(
                &mut out_file,
                format!("subgraph cluster_{} {{\n", tid).as_bytes(),
            )?;
            std::io::Write::write(
                &mut out_file,
                format!("\tlabel=\"thread {}\"\n", tid).as_bytes(),
            )?;
            for j in 1..=ind {
                let pos = Event::new(tid, j);
                // Skip the synthetic End terminator (carries no useful info).
                if matches!(g.label(pos), LabelEnum::End(_)) {
                    continue;
                }
                let is_error = error.is_some() && error.unwrap() == pos;
                // When timed mode is enabled, append the exact
                // [τ_lo, τ_hi] window of this event (under the
                // certified inbox case assignment).
                let time_str = if let Some(dcs) = exact_dcs.as_mut() {
                    match dcs.exact_bounds(pos) {
                        None => {
                            "<br/><font point-size=\"10\" color=\"#cc3333\">τ ∈ ∅</font>".to_string()
                        }
                        Some((lo, hi)) => {
                            let hi_s =
                                hi.map(|h| h.to_string()).unwrap_or_else(|| "∞".to_string());
                            format!(
                                "<br/><font point-size=\"10\" color=\"#3366aa\">τ ∈ [{}, {}]</font>",
                                lo, hi_s
                            )
                        }
                    }
                } else {
                    String::new()
                };
                std::io::Write::write(
                    &mut out_file,
                    format!(
                        "\t\"{}\" [label=<{}{}>{}]\n",
                        pos,
                        g.label(pos),
                        time_str,
                        if is_error {
                            ",style=filled,fillcollor=yellow"
                        } else {
                            ""
                        }
                    )
                    .as_bytes(),
                )?;
            }
            std::io::Write::write(&mut out_file, "}\n".to_string().as_bytes())?;
        }

        for (tid, ind) in v.entries() {
            for j in 1..ind + 1 {
                let pos = Event::new(tid, j);
                if j < ind {
                    let next = pos.next();
                    // The label loop drops the synthetic End terminator;
                    // skip the edge into it too so it doesn't render as
                    // a dangling unlabeled node.
                    if !matches!(g.label(next), LabelEnum::End(_)) {
                        std::io::Write::write(
                            &mut out_file,
                            format!("\"{}\" -> \"{}\"\n", pos, next).as_bytes(),
                        )?;
                    }
                }
                if g.is_recv(pos) {
                    let rlab = g.recv_label(pos).unwrap();
                    if rlab.rf().is_some() {
                        std::io::Write::write(
                            &mut out_file,
                            format!("\"{}\" -> \"{}\"[color=green]\n", rlab.rf().unwrap(), pos)
                                .as_bytes(),
                        )?;
                    }
                }
            }
        }

        std::io::Write::write(&mut out_file, "}\n".to_string().as_bytes())?;
        Ok(())
    }

    fn print_graph_trace(&self, error: Option<Event>) -> std::io::Result<()> {
        let g = &self.current.graph;

        let maxs = if let Some(e) = error {
            vec![e]
        } else {
            g.thread_ids()
                .iter()
                .filter(|&&tid| {
                    let last = g.thread_last(tid).unwrap().pos();
                    !g.is_send(last) || g.is_rf_maximal_send(last)
                })
                .map(|&tid| g.thread_last(tid).unwrap().pos())
                .collect()
        };

        let num_execs = self.telemetry.read_counter(EXECS.to_owned()).unwrap_or(0);
        let create_file = error.is_some() || num_execs == 1;
        let mut out_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(create_file)
            .write(true)
            .append(!create_file)
            .open(self.config.trace_file.as_ref().unwrap())
            .unwrap();

        let mut v = VectorClock::new();
        for e in maxs {
            self.print_graph_trace_util(&mut out_file, &mut v, e)?
        }
        std::io::Write::write(&mut out_file, "\n".to_string().as_bytes())?;
        Ok(())
    }

    fn print_graph_trace_util(
        &self,
        file: &mut std::fs::File,
        view: &mut VectorClock,
        e: Event,
    ) -> std::io::Result<()> {
        let g = &self.current.graph;

        if view.contains(e) {
            return Ok(());
        }

        let start_idx = view.get(e.thread).unwrap_or(0);

        view.update_or_set(e);
        for i in start_idx..=e.index {
            let ei = Event::new(e.thread, i);
            if g.is_recv(ei) && g.recv_label(ei).unwrap().rf().is_some() {
                self.print_graph_trace_util(file, view, g.recv_label(ei).unwrap().rf().unwrap())?;
            }
            if g.is_inbox(ei) {
                if let Some(rfs) = g.inbox_label(ei).unwrap().rfs() {
                    for rf in rfs {
                        self.print_graph_trace_util(file, view, rf)?;
                    }
                }
            }
            if let LabelEnum::TJoin(jlab) = g.label(ei) {
                self.print_graph_trace_util(file, view, g.thread_last(jlab.cid()).unwrap().pos())?;
            }
            if let LabelEnum::Begin(blab) = g.label(ei) {
                if blab.parent().is_some() {
                    self.print_graph_trace_util(file, view, blab.parent().unwrap())?;
                }
            }
            std::io::Write::write(file, format!("{}\n", g.label(ei),).as_bytes())?;
        }
        Ok(())
    }

    /// Enforce that monitors are only spawned from the main thread
    /// at the very start of the execution.
    pub(crate) fn validate_monitor_spawn(&self, curr: &Event) {
        // Check for simplicity (optional)
        if curr.thread != main_thread_id() {
            panic!("Monitors can only be spawned from the main thread");
        }
        let g = &self.current.graph;
        for i in 1..curr.index {
            let lab = g.create_label(Event::new(curr.thread, i));
            if lab.is_none() || !self.monitors.contains_key(&lab.unwrap().cid()) {
                panic!("Monitors must be spawned before any other instruction");
            }
        }
    }
    pub(crate) fn unstuck_joiners(state: &mut ExecutionState, finished: ThreadId) {
        let must = state.must.borrow();
        for task in state.tasks.iter_mut() {
            if !task.is_stuck() {
                continue;
            }
            // A task with TaskState::Blocked that is waiting for a Join
            // must have the Join label in the graph, *but* the instruction
            // pointer is one instruction behind.
            // Detect this, ensure it's waiting for the finished tid, and unblock it
            let tid = must.to_thread_id(task.id());
            let curr = Event::new(tid, task.instructions as u32);
            if let LabelEnum::TJoin(jlab) = must.current.graph.label(curr.next()) {
                if jlab.cid() == finished {
                    task.unstuck();
                }
            }
        }
    }

    fn fmt_revisit_item(&self, rev: &RevisitEnum) -> String {
        match rev {
            RevisitEnum::ForwardRevisit(r) => {
                format!(
                    "forward {} <= {}",
                    r.pos,
                    self.fmt_revisit_placement(&r.rev)
                )
            }
            RevisitEnum::BackwardRevisit(r) => {
                format!(
                    "backward {} <= {}",
                    r.pos,
                    self.fmt_revisit_placement(&r.rev)
                )
            }
        }
    }

    fn fmt_events(&self, events: &[Event]) -> String {
        events
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn fmt_event_set(&self, events: &[Event]) -> String {
        format!("{{{}}}", self.fmt_events(events))
    }

    fn fmt_revisit_placement(&self, placement: &RevisitPlacement) -> String {
        match placement {
            RevisitPlacement::Default(ev) => ev.to_string(),
            RevisitPlacement::BlockInstead => "{refuse-all}".to_string(),
            RevisitPlacement::Inbox(None) => "{timeout}".to_string(),
            RevisitPlacement::Inbox(Some(v)) => self.fmt_event_set(v),
        }
    }
}

/// True iff some `k`-subset of `sends` is jointly feasible as one
/// batch read waking the block at `block_pos`. Exact: the block is the
/// last event of its thread, where the conjunctive joint probe equals
/// the disjunctive inbox rule (terminal-read equivalence, see
/// timed_dcs::probe_unblock_subset). Standard lexicographic
/// combination walk, first hit wins.
/// Capacity of an inbox-shaped block's wake-up batch, mirroring what the
/// OFFER filter would compute for the same batch.
///
/// The block label drops `max`, so it is reconstructed from `wait`: a TIMED
/// inbox always carries `max == Some(min)` (see `inbox_internal_timed`), so a
/// batch of exactly `min` is at capacity. An UNTIMED blocking inbox may have
/// `max > min` and its `max` is unrecoverable here, so it stays `false`, the
/// conservative side, which is the behaviour that shipped before.
fn inbox_block_at_capacity(wait: &Option<crate::timed_cons::WaitTime>, min: usize, len: usize) -> bool {
    wait.is_some() && len == min
}

fn any_jointly_feasible_subset(
    dcs: &mut crate::timed_dcs::TimedDcs<'_>,
    block_pos: Event,
    sends: &[Event],
    k: usize,
    inbox: Option<(&crate::loc::RecvLoc, crate::loc::CommunicationModel)>,
    inbox_at_capacity: bool,
) -> bool {
    let n = sends.len();
    if k == 0 {
        return true;
    }
    if k > n {
        return false;
    }
    let mut idx: Vec<usize> = (0..k).collect();
    let mut subset: Vec<Event> = Vec::with_capacity(k);
    loop {
        subset.clear();
        subset.extend(idx.iter().map(|&i| sends[i]));
        let feasible = match inbox {
            Some((loc, comm)) => {
                dcs.probe_unblock_inbox_subset(block_pos, &subset, loc, comm, inbox_at_capacity)
            }
            None => dcs.probe_unblock_subset(block_pos, &subset),
        };
        if feasible {
            return true;
        }
        // Advance to the next k-combination in lexicographic order.
        let mut i = k;
        loop {
            if i == 0 {
                return false;
            }
            i -= 1;
            if idx[i] != i + n - k {
                idx[i] += 1;
                for j in i + 1..k {
                    idx[j] = idx[j - 1] + 1;
                }
                break;
            }
        }
    }
}

fn push_worklist(worklist: &mut RQueue, stamp: usize, r: RevisitEnum) {
    if worklist.get(&stamp).is_none() {
        worklist.insert(stamp, Vec::new());
    }
    let alts = worklist.get_mut(&stamp).unwrap();
    alts.push(r);
}

fn pop_worklist(worklist: &mut RQueue, is_arbitrary: bool, rng: &mut Pcg64Mcg) -> RevisitEnum {
    let (stamp, rev, is_empty) = {
        let (stamp, revs) = worklist
            .iter_mut()
            .next_back()
            .expect("worklist is not empty");
        if !is_arbitrary {
            let rev = revs.pop().unwrap();
            (*stamp, rev, revs.is_empty())
        } else {
            // Choose randomly from alternatives at the highest stamp.
            // BlockInstead converts the label kind at its position, so
            // it may only pop once it is the last alternative there.
            let eligible: Vec<usize> = revs
                .iter()
                .enumerate()
                .filter(|(_, r)| {
                    !matches!(r.rev(), crate::revisit::RevisitPlacement::BlockInstead)
                })
                .map(|(i, _)| i)
                .collect();
            let idx = if eligible.is_empty() {
                rng.random_range(0..revs.len())
            } else {
                eligible[rng.random_range(0..eligible.len())]
            };
            let rev = revs.swap_remove(idx);
            (*stamp, rev, revs.is_empty())
        }
    };
    if is_empty {
        worklist.remove(&stamp);
    }
    rev
}

fn compute_inbox_possible_subsets_from_rfs(
    events: &[Event],
    min: usize,
    max: Option<usize>,
    must_include: Option<Event>,
) -> Vec<Vec<Event>> {
    fn build(
        idx: usize,
        events: &[Event],
        min: usize,
        max_len: usize,
        must_include: Option<Event>,
        has_must: bool,
        current: &mut Vec<Event>,
        out: &mut Vec<Vec<Event>>,
    ) {
        if current.len() > max_len {
            return;
        }
        let remaining = events.len() - idx;
        if current.len() + remaining < min {
            return;
        }

        if idx == events.len() {
            let len = current.len();
            if len >= min && len <= max_len && must_include.is_none_or(|_| has_must) {
                out.push(current.clone());
            }
            return;
        }

        // Exclude current event
        build(
            idx + 1,
            events,
            min,
            max_len,
            must_include,
            has_must,
            current,
            out,
        );

        // Include current event
        current.push(events[idx]);
        build(
            idx + 1,
            events,
            min,
            max_len,
            must_include,
            has_must || must_include == Some(events[idx]),
            current,
            out,
        );
        current.pop();
    }

    let max_len = max.map_or(events.len(), |m| m.min(events.len()));
    if min > max_len || must_include.is_some_and(|event| !events.contains(&event)) {
        return Vec::new();
    }

    let mut subsets: Vec<Vec<Event>> = Vec::new();
    build(
        0,
        events,
        min,
        max_len,
        must_include,
        false,
        &mut Vec::new(),
        &mut subsets,
    );
    subsets
}

#[cfg(test)]
mod tests {
    use REPLAY::{ReplayInformation, TopologicallySortedExecutionGraph};

    use super::*;

    use crate::{
        event::Event,
        loc::{CommunicationModel, SendLoc},
        thread::construct_thread_id,
        Config, LabelEnum,
    };

    fn setup_must_for_replay() -> Must {
        let main_tid = construct_thread_id(0);
        let config = Config::default();
        let mut must = Must::new(config.clone(), true);
        let mut tseg = TopologicallySortedExecutionGraph::new();
        let send_at_0 = LabelEnum::SendMsg(SendMsg::new(
            Event::new(main_tid, 0),
            SendLoc::new_empty(main_tid),
            CommunicationModel::default(),
            Val::new("bob"),
            MonitorSends::new(),
            false,
        ));
        tseg.insert_label(send_at_0.clone());
        let error_state = MustState::new();
        must.replay_info = ReplayInformation::create(tseg, error_state, config.clone());
        must.replay_info.next_task(); // Advance to (t0, 0)
        must
    }

    #[test]
    #[should_panic(expected = "Executing (t0, 1) instead of the counterexample's (t0, 0)")]
    fn test_try_consume_panic_on_index_mismatch() {
        let mut must = setup_must_for_replay();

        let tid = construct_thread_id(0);
        let send_at_1 = LabelEnum::SendMsg(SendMsg::new(
            Event::new(tid, 1),
            SendLoc::new_empty(tid),
            CommunicationModel::default(),
            Val::new("bob"),
            MonitorSends::new(),
            false,
        ));

        // Try to replay with the wrong thread.
        must.try_consume(&send_at_1);
    }
}
