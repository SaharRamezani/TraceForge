//! Delayed EndTxn in nisshi (formerly Tansu): KAFKA-17754 in a Rust broker.
//!
//! ## The bug class
//!
//! KAFKA-17754 (Apache Kafka, found by Jepsen, fixed 2026-08-12): "When a
//! server receives a commit (or abort) message, it has no way to know what
//! transaction the client intended to commit. It simply commits or aborts
//! whatever transaction happens to be in progress." A producer's EndTxn
//! request is delayed, the client times out and retries it on a new
//! connection, the retry succeeds, the client starts its next transaction,
//! and the original EndTxn is finally processed: it commits the NEXT
//! transaction, or part of it.
//!
//! ## What this model is faithful to
//!
//! Broker side: nisshi, <https://github.com/nisshi-io/nisshi>, commit
//! b3d0ceaf083aa79e6bf85fe2c21cf182415e36e2 (2026-09-01), PostgreSQL
//! storage engine, `nisshi-storage/src/pg.rs` and its SQL:
//!
//!   * a transactional id has ONE `txn_detail` row per producer epoch,
//!     reused by every transaction of that epoch
//!     (`ddl/040-txn-detail.sql`: `unique ("transaction", producer_epoch)`);
//!   * AddPartitionsToTxn v0-3 (`txn_add_partitions`, pg.rs:3603) inserts
//!     `txn_topition` rows for the request's epoch without comparing it to
//!     the current epoch, then sets the row to BEGIN when its status is
//!     NULL, COMMITTED or ABORTED (`sql/txn_detail_update_started_at.sql`);
//!   * transactional Produce (`produce_in_tx`, pg.rs:761) runs the
//!     idempotent sequence check (`sql.rs::idempotent_sequence_check`),
//!     appends the records, and records the offset range in
//!     `txn_produce_offset` ONLY when a `txn_topition` row exists for that
//!     partition (`sql/txn_produce_offset_insert.sql` joins on it);
//!   * EndTxn (`end_in_tx`, pg.rs:998) identifies the transaction by
//!     (transactional id, producer id, EPOCH) only: it compares the epoch
//!     with the current one, reads the row's status, writes a control
//!     marker to every produced partition when the status is NULL or
//!     BEGIN, and finalizes (deletes the bookkeeping, sets COMMITTED or
//!     ABORTED) unless an overlapping transaction is still open, in which
//!     case it parks the row in PREPARE_COMMIT or PREPARE_ABORT;
//!   * read_committed visibility: the last stable offset is the smallest
//!     `offset_start` of a transaction in BEGIN or PREPARE_* on that
//!     partition, else the high watermark (`sql/watermark_select.sql`).
//!
//! Every request runs in one database transaction in nisshi, so the
//! storage actor executes each request atomically, in arrival order. That
//! is one legal serialization of the real concurrent execution, so any
//! violation found here is reachable in nisshi (the converse is not
//! claimed).
//!
//! Client side: the Apache Kafka Java producer, 3.9 branch,
//! `clients/.../producer/internals/TransactionManager.java`: transactional
//! requests are sent one at a time; a partition is added to the
//! transaction (AddPartitionsToTxn) before its first batch is sent; when a
//! request times out the connection is closed and the handler sees a
//! disconnect, looks up the coordinator again (FindCoordinator) and
//! re-enqueues the SAME request (`TxnRequestHandler.onComplete`); EndTxn
//! NONE completes the transaction (`EndTxnHandler.handleResponse`).
//!
//! ## Actors
//!
//!   producer --(zero delay)--> connection k --[L, U]--> storage
//!   storage  --------------------[L, U]---------------> producer
//!
//! A connection actor is one TCP connection plus the stateless nisshi
//! broker reading from it: requests on one connection keep their order
//! (FIFO), requests on different connections do not. The [L, U] hop is
//! everything between the client's write and the database update: the
//! network, the broker's request handling, waiting for a pooled database
//! connection or a row lock. After a timeout the producer abandons
//! connection k and uses k + 1; a request already written to connection k
//! is still delivered and executed.
//!
//! Workload: InitProducerId, then T transactions, each writing one record
//! to partition 0 and committing. Request timeouts (W) apply to EndTxn;
//! the retry budget R bounds how many timeouts are explored. Other
//! requests wait without a timeout, which only removes behaviours.
//!
//! ## Threads and termination
//!
//! No thread is a daemon: an execution is complete only when the producer,
//! every connection and the storage have ended. The producer ends by
//! itself, after its T transactions or at a fatal reply. The connections
//! and the storage are reactors that serve a number of messages that no
//! count fixes: how many requests a connection relays, and the storage
//! serves, depends on how many EndTxn timeouts the producer takes (0 to R),
//! and the delays alone decide that even when nothing is lost. So the
//! producer, the one thread that knows its work is over, ends them with
//! HARNESS messages (not the protocol's), sent at the end of `producer`:
//!
//!   * a Stop to each connection, in order, joining the connection before
//!     it writes the next. The Stop travels the producer's own link, so FIFO
//!     delivers it after every request the producer wrote there, and the
//!     connection leaves its loop on reading it;
//!   * then a Stop { owed } to the storage, owed being the number of
//!     requests the producer wrote, which a run that loses nothing delivers.
//!     The storage leaves its loop when it has read the Stop and served that
//!     many requests, so a request still in flight when the producer ends is
//!     served as it was before. (A Stop without the count could be read
//!     ahead of such a request: FIFO orders the messages of one sender only,
//!     and the storage has one sender per connection. It would end with the
//!     request unread.)
//!
//! These are the Close and the Done { total } that the producer sent until
//! 2026-10-14, when the actors became daemons and the messages went. What is
//! new is the order: the Stops to the connections are chained by joins, so
//! that their fates are dependent, and the storage's comes last.
//!
//! Both are lossy like every send and carry no verdict. A lost Stop leaves
//! its connection blocked, the producer blocked at the join and the later
//! Stops unsent; a request that was lost, or expired unread, leaves the
//! storage waiting for it. So an execution that ends blocked without a
//! failed assertion is one in which a message was lost or expired. A
//! violation parks the storage: the producer then waits for ever for a
//! reply and sends no Stop.
//!
//! What this adds. Each execution is an execution of the daemon version
//! (the file of 2026-10-14, where the storage and the connections were
//! daemons) followed by the harness, which adds endings, not behaviours. An
//! execution in which the producer reaches the end of its work is explored
//! R + 3 times where the daemon version explored it once: once for every
//! place where a Stop can be lost (R + 1 connections, then the storage) and
//! once with every Stop delivered. Only that last one can be filed under
//! execs, and only if the storage served every request: the daemon version
//! filed an execution in which a request was lost or expired under execs
//! too, because nothing waited for the request, and this one files it under
//! blocked. An execution in which the producer blocks, the violating ones
//! among them, is explored once, as before. When the storage reads its Stop
//! after every request, which in timed mode is certain if U < W + 5L (a
//! request abandoned at s is in by s + U, and the Stop reaches the storage
//! at least W + 5L after s: the timeout, two round trips, one more hop; at
//! equality the two can arrive together), the count is exact:
//!
//!   explored = blocked + (R + 3) * execs of the daemon version.
//!
//! In baseline mode, and in timed mode with a larger U, the storage can
//! also read its Stop before a request still to be served (an abandoned
//! EndTxn, say), and every such reading is an execution of its own: the
//! sum above is then a lower bound. (With W < 2L every wait times out, so
//! the timeouts all fall in the first transaction, three more round trips
//! follow for each later one, and the bound is U < W + 5L + 6L(T - 1): the
//! timed rows below are exact for that reason.) The counters that sum over
//! explored executions (timeouts, completed_txns, ...) move with the number
//! of executions.
//!
//! execs / blocked / violating, --property stray --keep-going, daemons
//! (2026-10-14) then no daemons:
//!
//!   T R L U W B   mode       daemons                 no daemons
//!   2 1 1 5 1 0   timed          8 /    115 / 0           6 /    141 / 0
//!   2 2 1 5 1 0   timed         72 /    967 / 0          44 /  1,283 / 0
//!   2 3 1 8 1 0   timed      1,352 / 16,207 / 448       800 / 23,519 / 448
//!   2 1 1 5 1 0   baseline      21 /    175 / 4          21 /    242 / 4
//!   2 2 1 5 1 0   baseline     421 /  2,469 / 102       433 /  4,297 / 102
//!
//! ## Properties (checked at the storage, which sees every effect)
//!
//! Each request carries a GHOST field, the index of the client transaction
//! it belongs to. Ghosts are never read by the protocol logic.
//!
//!   P1 (KAFKA-17754, stray commit): a control marker written by
//!      EndTxn(g) seals only records of client transaction g.
//!   P2 (dirty read): a record of client transaction g is never below the
//!      last stable offset before an EndTxn(commit) of transaction g has
//!      been processed.
//!
//! ## Confirmed on the real code (2026-09-16)
//!
//! Both witness orders were replayed through nisshi's public `Storage`
//! trait and observed by a read_committed consumer through its real
//! `FetchService` (replay test in docs/research/nisshi_kafka_17754/). A
//! control run without the delayed EndTxn keeps T2 hidden; with it, T2's
//! record reaches the consumer while T2 is open, on the memory (dynostore),
//! sqlite and PostgreSQL 17 engines. The client's later abort of T2 then
//! returns NONE on memory and sqlite (the consumer has read a transaction
//! the producer was told is aborted) and INVALID_TXN_STATE on PostgreSQL
//! (fatal in the Java client).
//!
//! ## Blocked executions
//!
//! Every send is lossy, so most explored executions lose a message, and a
//! run ends blocked (the `blocked=` count) when a thread waits for ever for
//! one: the producer for a reply, the storage for a request it is owed, a
//! connection (and the producer at its join) for a Stop. A thread parked at
//! a failed assertion is blocked too: those are the violating executions,
//! all of them with the producer waiting for a reply the parked storage
//! never sends. A blocked execution only drops behaviours, never adds one:
//! every request that is processed is processed in arrival order, so the
//! violations found and the boundaries measured are those of the full
//! model. Raising --sd does not remove blocked executions (T=2 R=1 L=1 U=8
//! W=1, timed: sd 0/2/10 give blocked 153/161/161) and leaves the violations
//! unchanged (violating 6 at all three).
//!
//! ## CLI (absolute integer time units)
//!
//!   --mode baseline|timed|compare  (default compare)
//!   --l L --u U                    request and response delay window
//!   --sd SD                        message storage lifetime (default 0)
//!   --w W                          EndTxn request timeout (request.timeout.ms)
//!   --backoff B                    sleep before the retry (retry.backoff.ms)
//!   --txns T                       transactions (default 2)
//!   --retries R                    EndTxn timeouts explored (default 1)
//!   --fix none|epoch-bump          epoch-bump: bump the producer epoch at
//!                                  every commit, KIP-890 style (NOT nisshi
//!                                  code; the comparison fix)
//!   --property both|stray|dirty    which assertions abort (both always counted)
//!   --keep-going --parallel none|shared|partitioned --verbose N
//!
//! Exit codes: 0 hold, 101 violation, 2 CLI misuse.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

static STRAY_COMMITS: AtomicUsize = AtomicUsize::new(0);
static DIRTY_READS: AtomicUsize = AtomicUsize::new(0);
static TIMEOUTS: AtomicUsize = AtomicUsize::new(0);
static FATAL: AtomicUsize = AtomicUsize::new(0);
static COMPLETED_TXNS: AtomicUsize = AtomicUsize::new(0);

const PARTITION: u32 = 0;
const TAG_CONN_REQ: u32 = 1;
/// HARNESS: tag of the producer's Stop to a connection.
const TAG_CONN_STOP: u32 = 2;
const TAG_RESP_BASE: u32 = 1000;

fn resp_tag(corr: u32) -> u32 {
    TAG_RESP_BASE + corr
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ErrorCode {
    None,
    UnknownProducerId,
    ProducerFenced,
    InvalidProducerEpoch,
    InvalidTxnState,
    DuplicateSequenceNumber,
    OutOfOrderSequenceNumber,
}

#[derive(Clone, Debug, PartialEq)]
enum Req {
    FindCoordinator,
    InitProducerId,
    AddPartitionsToTxn { epoch: i16, partition: u32 },
    Produce { epoch: i16, partition: u32, base_sequence: i32, ghost: u32 },
    EndTxn { epoch: i16, committed: bool, ghost: u32 },
}

#[derive(Clone, Debug, PartialEq)]
struct Envelope {
    corr: u32,
    reply_to: ThreadId,
    req: Req,
}

#[derive(Clone, Debug, PartialEq)]
enum ConnMsg {
    Req(Envelope),
    /// HARNESS: end of run, written by the producer after its last request.
    Stop,
}

#[derive(Clone, Debug, PartialEq)]
enum StorageMsg {
    Req(Envelope),
    /// HARNESS: end of run, from the producer. `owed` is the number of
    /// requests the producer wrote, which a run that loses nothing delivers.
    Stop { owed: u32 },
}

#[derive(Clone, Debug, PartialEq)]
enum Resp {
    Coordinator,
    ProducerId { error: ErrorCode, epoch: i16 },
    AddPartitions { error: ErrorCode },
    Produce { error: ErrorCode },
    EndTxn { error: ErrorCode, next_epoch: Option<i16> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fix {
    None,
    EpochBump,
}

#[derive(Clone, Copy, Debug)]
struct Params {
    l: u64,
    u: u64,
    sd: u64,
    w: u64,
    backoff: u64,
    txns: u32,
    retries: u32,
    fix: Fix,
    check_stray: bool,
    check_dirty: bool,
}

fn cli_bail(msg: &str) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(2);
}

// =====================================================================
// Producer: Kafka Java client TransactionManager behaviour
// =====================================================================

struct Client {
    me: ThreadId,
    storage: ThreadId,
    conns: Vec<ThreadId>,
    conn: usize,
    corr: u32,
    /// Requests written so far (HARNESS: what the storage is owed).
    sent: u32,
}

impl Client {
    /// Write a request to the current connection; returns its correlation id.
    fn send(&mut self, req: Req) -> u32 {
        let corr = self.corr;
        self.corr += 1;
        self.sent += 1;
        let env = Envelope { corr, reply_to: self.me, req };
        traceforge::send_tagged_msg_timed(
            self.conns[self.conn],
            TAG_CONN_REQ,
            ConnMsg::Req(env),
            0,
            0,
        );
        corr
    }

    fn wait(&self, corr: u32) -> Resp {
        let storage = self.storage;
        traceforge::recv_tagged_msg_block_timed(move |s, tag| {
            s == storage && tag == Some(resp_tag(corr))
        })
    }

    fn wait_within(&self, corr: u32, w: u64) -> Option<Resp> {
        let storage = self.storage;
        traceforge::recv_tagged_msg_timed(
            move |s, tag| s == storage && tag == Some(resp_tag(corr)),
            WaitTime::Finite(w),
        )
    }

    fn call(&mut self, req: Req) -> Resp {
        let corr = self.send(req);
        self.wait(corr)
    }
}

fn producer(
    p: Params,
    storage: ThreadId,
    conns: Vec<ThreadId>,
    // HARNESS: join handles of the connections, in the order of `conns`.
    handles: Vec<thread::JoinHandle<()>>,
) {
    let mut c = Client { me: thread::current().id(), storage, conns, conn: 0, corr: 0, sent: 0 };
    let mut retries_left = p.retries;

    'run: {
        let mut epoch = match c.call(Req::InitProducerId) {
            Resp::ProducerId { error: ErrorCode::None, epoch } => epoch,
            _ => {
                FATAL.fetch_add(1, Ordering::Relaxed);
                break 'run;
            }
        };
        let mut sequence: i32 = 0;

        for ghost in 1..=p.txns {
            // send(record): the partition is new in this transaction, so
            // AddPartitionsToTxn precedes the batch.
            match c.call(Req::AddPartitionsToTxn { epoch, partition: PARTITION }) {
                Resp::AddPartitions { error: ErrorCode::None } => {}
                _ => {
                    FATAL.fetch_add(1, Ordering::Relaxed);
                    break 'run;
                }
            }
            match c.call(Req::Produce { epoch, partition: PARTITION, base_sequence: sequence, ghost })
            {
                Resp::Produce { error: ErrorCode::None } => sequence += 1,
                _ => {
                    FATAL.fetch_add(1, Ordering::Relaxed);
                    break 'run;
                }
            }

            // commitTransaction(): EndTxn, re-enqueued after a disconnect.
            let req = Req::EndTxn { epoch, committed: true, ghost };
            let resp = loop {
                let corr = c.send(req.clone());
                if retries_left == 0 {
                    break c.wait(corr);
                }
                match c.wait_within(corr, p.w) {
                    Some(resp) => break resp,
                    None => {
                        // request.timeout.ms elapsed: the connection is
                        // closed, the coordinator looked up again on a new
                        // connection, then the same EndTxn is re-sent.
                        TIMEOUTS.fetch_add(1, Ordering::Relaxed);
                        retries_left -= 1;
                        c.conn += 1;
                        if p.backoff > 0 {
                            traceforge::sleep(p.backoff);
                        }
                        match c.call(Req::FindCoordinator) {
                            Resp::Coordinator => {}
                            _ => unreachable!(),
                        }
                    }
                }
            };
            match resp {
                Resp::EndTxn { error: ErrorCode::None, next_epoch } => {
                    COMPLETED_TXNS.fetch_add(1, Ordering::Relaxed);
                    if let Some(e) = next_epoch {
                        epoch = e;
                        sequence = 0;
                    }
                }
                _ => {
                    FATAL.fetch_add(1, Ordering::Relaxed);
                    break 'run;
                }
            }
        }
    }

    // HARNESS: end of run, not the protocol's (see "Threads and
    // termination"). The producer's work is over, and nothing else can end
    // the connections and the storage. A Stop goes to each connection in
    // turn, and the producer joins the connection before it writes the next
    // one, so that a lost Stop leaves its connection blocked, the producer
    // blocked at the join and the later Stops unsent. The Stop comes from
    // the producer, so FIFO delivers it after every request it wrote to
    // that connection. The storage gets its Stop last, with the number of
    // requests it is owed.
    for (conn, handle) in c.conns.iter().copied().zip(handles) {
        traceforge::send_tagged_msg_timed(conn, TAG_CONN_STOP, ConnMsg::Stop, 0, 0);
        let _ = handle.join();
    }
    traceforge::send_msg(c.storage, StorageMsg::Stop { owed: c.sent });
}

// =====================================================================
// Connection: one TCP connection read by a stateless broker
// =====================================================================

fn connection(storage: ThreadId) {
    // A reactor: it relays whatever the producer writes to it, which no
    // count bounds, and leaves on the producer's Stop.
    loop {
        match traceforge::recv_msg_block_timed::<ConnMsg>() {
            ConnMsg::Req(env) => traceforge::send_msg(storage, StorageMsg::Req(env)),
            // HARNESS: end of run, read after every request written here.
            ConnMsg::Stop => break,
        }
    }
}

// =====================================================================
// Storage: nisshi PostgreSQL engine (pg.rs), one request = one DB tx
// =====================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TxnState {
    Begin,
    PrepareCommit,
    PrepareAbort,
    Committed,
    Aborted,
}

impl TxnState {
    fn is_prepared(self) -> bool {
        matches!(self, TxnState::PrepareCommit | TxnState::PrepareAbort)
    }
    fn is_open(self) -> bool {
        matches!(self, TxnState::Begin | TxnState::PrepareCommit | TxnState::PrepareAbort)
    }
}

/// One `txn_detail` row with its `txn_topition` and `txn_produce_offset` rows.
#[derive(Default)]
struct TxnDetail {
    status: Option<TxnState>,
    topitions: BTreeSet<u32>,
    produce_offsets: BTreeMap<u32, (u64, u64)>,
}

struct Record {
    control: bool,
    epoch: i16,
    ghost: Option<u32>,
}

#[derive(Default)]
struct Db {
    /// `producer_epoch` rows of producer PID, ascending; empty: no producer.
    epochs: Vec<i16>,
    details: BTreeMap<i16, TxnDetail>,
    /// `producer_detail.sequence` per (epoch, partition).
    sequences: BTreeMap<(i16, u32), i32>,
    log: BTreeMap<u32, Vec<Record>>,
    /// Largest ghost of an EndTxn(commit) processed so far (P2).
    commit_requested: u32,
    stray_seen: bool,
    dirty_seen: bool,
    check_stray: bool,
    check_dirty: bool,
}

impl Db {
    fn current_epoch(&self) -> Option<i16> {
        self.epochs.last().copied()
    }

    fn high(&self, partition: u32) -> u64 {
        self.log.get(&partition).map_or(0, |l| l.len() as u64)
    }

    /// `produce_in_tx` for a transactional batch of one record.
    fn append(&mut self, epoch: i16, partition: u32, control: bool, ghost: Option<u32>) -> u64 {
        let offset = self.high(partition);
        self.log.entry(partition).or_default().push(Record { control, epoch, ghost });
        // txn_produce_offset_insert.sql joins txn_topition: untracked when
        // the partition is not registered in this epoch's row.
        if let Some(d) = self.details.get_mut(&epoch) {
            if d.topitions.contains(&partition) {
                d.produce_offsets
                    .entry(partition)
                    .and_modify(|r| r.1 = offset)
                    .or_insert((offset, offset));
            }
        }
        offset
    }

    /// `bump_or_create_producer` (pg.rs:1397) for a transactional id.
    fn init_producer(&mut self) -> Resp {
        if let Some(current) = self.current_epoch() {
            if self.details.get(&current).and_then(|d| d.status) == Some(TxnState::Begin) {
                let error = self.end_in_tx(current, false, None);
                if error != ErrorCode::None {
                    return Resp::ProducerId { error, epoch: current };
                }
            }
        }
        let epoch = self.current_epoch().map_or(0, |e| e + 1);
        self.epochs.push(epoch);
        self.details.insert(epoch, TxnDetail::default());
        Resp::ProducerId { error: ErrorCode::None, epoch }
    }

    /// `txn_add_partitions` v0-3 (pg.rs:3603).
    fn add_partitions(&mut self, epoch: i16, partition: u32) -> Resp {
        if let Some(d) = self.details.get_mut(&epoch) {
            d.topitions.insert(partition);
            if matches!(d.status, None | Some(TxnState::Committed) | Some(TxnState::Aborted)) {
                d.status = Some(TxnState::Begin);
            }
        }
        Resp::AddPartitions { error: ErrorCode::None }
    }

    /// `idempotent_message_check` + `produce_in_tx` (pg.rs:258, 761).
    fn produce(&mut self, epoch: i16, partition: u32, base_sequence: i32, ghost: u32) -> Resp {
        let Some(current) = self.current_epoch() else {
            return Resp::Produce { error: ErrorCode::UnknownProducerId };
        };
        let sequence = self.sequences.get(&(epoch, partition)).copied().unwrap_or(0);
        let error = match current.cmp(&epoch) {
            std::cmp::Ordering::Equal => match sequence.cmp(&base_sequence) {
                std::cmp::Ordering::Equal => ErrorCode::None,
                std::cmp::Ordering::Greater => ErrorCode::DuplicateSequenceNumber,
                std::cmp::Ordering::Less => ErrorCode::OutOfOrderSequenceNumber,
            },
            std::cmp::Ordering::Greater => ErrorCode::ProducerFenced,
            std::cmp::Ordering::Less => ErrorCode::InvalidProducerEpoch,
        };
        if error == ErrorCode::None {
            *self.sequences.entry((epoch, partition)).or_insert(0) += 1;
            self.append(epoch, partition, false, Some(ghost));
        }
        Resp::Produce { error }
    }

    /// `end_in_tx` (pg.rs:998) with fence = false. `ghost` is the client
    /// transaction of the EndTxn request (None for the InitProducerId abort).
    fn end_in_tx(&mut self, epoch: i16, committed: bool, ghost: Option<u32>) -> ErrorCode {
        let Some(current) = self.current_epoch() else {
            return ErrorCode::UnknownProducerId;
        };
        match epoch.cmp(&current) {
            std::cmp::Ordering::Less => return ErrorCode::ProducerFenced,
            std::cmp::Ordering::Greater => return ErrorCode::InvalidProducerEpoch,
            std::cmp::Ordering::Equal => {}
        }
        let status = self.details.get(&epoch).and_then(|d| d.status);
        let write_marker = match status {
            Some(TxnState::Committed) => {
                return if committed { ErrorCode::None } else { ErrorCode::InvalidTxnState };
            }
            Some(TxnState::Aborted) => {
                return if committed { ErrorCode::InvalidTxnState } else { ErrorCode::None };
            }
            Some(TxnState::PrepareCommit) => {
                if !committed {
                    return ErrorCode::InvalidTxnState;
                }
                false
            }
            Some(TxnState::PrepareAbort) => {
                if committed {
                    return ErrorCode::InvalidTxnState;
                }
                false
            }
            None | Some(TxnState::Begin) => true,
        };

        // txn_select_produced_topitions.sql
        let produced: Vec<(u32, (u64, u64))> = self
            .details
            .get(&epoch)
            .map(|d| d.produce_offsets.iter().map(|(p, r)| (*p, *r)).collect())
            .unwrap_or_default();

        let mut overlaps: Vec<i16> = Vec::new();
        for (partition, (start, end)) in produced {
            if write_marker {
                if let Some(g) = ghost {
                    // P1: the records this marker seals belong to transaction g.
                    let stray = self.log[&partition][start as usize..=end as usize]
                        .iter()
                        .any(|r| !r.control && r.epoch == epoch && r.ghost != Some(g));
                    if stray && !self.stray_seen {
                        self.stray_seen = true;
                        STRAY_COMMITS.fetch_add(1, Ordering::Relaxed);
                    }
                    if self.check_stray {
                        traceforge::assert(!stray);
                    }
                }
                self.append(epoch, partition, true, None);
            }
            // txn_produce_offset_select_overlapping_txn.sql: other open
            // transactions on this partition starting before our end.
            let end = self.details[&epoch].produce_offsets[&partition].1;
            for (e, d) in &self.details {
                if *e != epoch
                    && d.status.is_some()
                    && d.produce_offsets.get(&partition).is_some_and(|r| r.0 < end)
                    && !overlaps.contains(e)
                {
                    overlaps.push(*e);
                }
            }
        }

        if overlaps.iter().all(|e| self.details[e].status.is_some_and(TxnState::is_prepared)) {
            let mine = if committed { TxnState::PrepareCommit } else { TxnState::PrepareAbort };
            let mut finals: Vec<(i16, TxnState)> =
                overlaps.iter().map(|e| (*e, self.details[e].status.unwrap())).collect();
            finals.push((epoch, mine));
            for (e, state) in finals {
                let d = self.details.get_mut(&e).expect("txn_detail row");
                d.produce_offsets.clear();
                d.topitions.clear();
                d.status = Some(match state {
                    TxnState::PrepareCommit => TxnState::Committed,
                    TxnState::PrepareAbort => TxnState::Aborted,
                    other => other,
                });
            }
        } else if let Some(d) = self.details.get_mut(&epoch) {
            d.status = Some(if committed { TxnState::PrepareCommit } else { TxnState::PrepareAbort });
        }
        ErrorCode::None
    }

    /// KIP-890-style comparison fix (not nisshi code): a completed commit
    /// bumps the epoch, and a retry of the previous epoch's completed
    /// commit is answered without touching the new epoch's transaction.
    fn end_txn_epoch_bump(&mut self, epoch: i16, committed: bool, ghost: u32) -> Resp {
        let current = self.current_epoch().unwrap_or(-1);
        if epoch + 1 == current
            && committed
            && self.details.get(&epoch).and_then(|d| d.status) == Some(TxnState::Committed)
        {
            return Resp::EndTxn { error: ErrorCode::None, next_epoch: Some(current) };
        }
        let error = self.end_in_tx(epoch, committed, Some(ghost));
        if error == ErrorCode::None
            && self.details.get(&epoch).and_then(|d| d.status) == Some(TxnState::Committed)
        {
            let next = epoch + 1;
            if self.current_epoch() == Some(epoch) {
                self.epochs.push(next);
                self.details.insert(next, TxnDetail::default());
            }
            return Resp::EndTxn { error, next_epoch: Some(next) };
        }
        Resp::EndTxn { error, next_epoch: None }
    }

    fn handle(&mut self, req: &Req, fix: Fix) -> Resp {
        match *req {
            Req::FindCoordinator => Resp::Coordinator,
            Req::InitProducerId => self.init_producer(),
            Req::AddPartitionsToTxn { epoch, partition } => self.add_partitions(epoch, partition),
            Req::Produce { epoch, partition, base_sequence, ghost } => {
                self.produce(epoch, partition, base_sequence, ghost)
            }
            Req::EndTxn { epoch, committed, ghost } => {
                if committed {
                    self.commit_requested = self.commit_requested.max(ghost);
                }
                match fix {
                    Fix::None => Resp::EndTxn {
                        error: self.end_in_tx(epoch, committed, Some(ghost)),
                        next_epoch: None,
                    },
                    Fix::EpochBump => self.end_txn_epoch_bump(epoch, committed, ghost),
                }
            }
        }
    }

    /// P2: read_committed never exposes a record of transaction g before
    /// an EndTxn(commit) of g was processed (watermark_select.sql).
    fn check_visibility(&mut self) {
        for (partition, log) in &self.log {
            let lso = self
                .details
                .values()
                .filter(|d| d.status.is_some_and(TxnState::is_open))
                .filter_map(|d| d.produce_offsets.get(partition).map(|r| r.0))
                .min()
                .unwrap_or(log.len() as u64);
            let dirty = log[..lso as usize]
                .iter()
                .any(|r| !r.control && r.ghost.is_some_and(|g| g > self.commit_requested));
            if dirty && !self.dirty_seen {
                self.dirty_seen = true;
                DIRTY_READS.fetch_add(1, Ordering::Relaxed);
            }
            if self.check_dirty {
                traceforge::assert(!dirty);
            }
        }
    }
}

fn storage(p: Params) {
    let fix = p.fix;
    let mut db = Db { check_stray: p.check_stray, check_dirty: p.check_dirty, ..Db::default() };
    // A reactor: it serves every request, in arrival order. How many there
    // are depends on the timeouts the producer takes, so the producer tells
    // it (HARNESS) once its work is over, and the storage leaves when it has
    // read that and served as many requests as it is owed: a request still
    // in flight when the producer ends is served as before.
    let mut served: u32 = 0;
    let mut owed: Option<u32> = None;
    while owed != Some(served) {
        match traceforge::recv_msg_block_timed::<StorageMsg>() {
            StorageMsg::Req(env) => {
                served += 1;
                let resp = db.handle(&env.req, fix);
                db.check_visibility();
                traceforge::send_tagged_msg(env.reply_to, resp_tag(env.corr), resp);
            }
            // HARNESS: end of run.
            StorageMsg::Stop { owed: n } => owed = Some(n),
        }
    }
}

// =====================================================================
// Verifier setup and reporting
// =====================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

static PARALLEL: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn build_config(mode: Mode, p: Params, keep_going: bool, verbose: usize) -> Config {
    let mut builder = Config::builder().with_all_sends_lossy().with_progress_report(usize::MAX).with_verbose(verbose);
    builder = match PARALLEL.get().map(|s| s.as_str()).unwrap_or("none") {
        "none" => builder,
        "shared" => builder.with_parallel(true),
        "partitioned" => builder.with_partitioned_parallelization(true),
        other => cli_bail(&format!("invalid --parallel: {other}")),
    };
    if keep_going {
        builder = builder.with_keep_going_after_error(true);
    }
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Timed => builder.with_timed(p.l, p.u, p.sd).build(),
    }
}

fn run(mode: Mode, p: Params, keep_going: bool, verbose: usize) -> (Stats, Duration) {
    for c in [&STRAY_COMMITS, &DIRTY_READS, &TIMEOUTS, &FATAL, &COMPLETED_TXNS] {
        c.store(0, Ordering::Relaxed);
    }
    let cfg = build_config(mode, p, keep_going, verbose);
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        // No daemons. The producer ends by itself; it also gets the join
        // handles of the connections, to end them (HARNESS, see "Threads and
        // termination"), and the storage ends on the producer's Stop.
        let storage_tid = thread::spawn(move || storage(p)).thread().id();
        let conns: Vec<thread::JoinHandle<()>> =
            (0..=p.retries).map(|_| thread::spawn(move || connection(storage_tid))).collect();
        let conn_ids: Vec<ThreadId> = conns.iter().map(|h| h.thread().id()).collect();
        let producer_h = thread::spawn(move || producer(p, storage_tid, conn_ids, conns));
        let _ = producer_h.join();
    });
    (stats, start.elapsed())
}

fn print_one(label: &str, p: Params, stats: &Stats, dur: Duration) {
    println!(
        "{label:<9} fix={fix:?} T={t} R={r} L={l} U={u} sd={sd} W={w} B={b}  execs={execs} blocked={block} \
         timeouts={to} completed_txns={ct} fatal={fa} stray_commits={sc} dirty_reads={dr} \
         violations={viol} time={dur:?}",
        fix = p.fix, t = p.txns, r = p.retries, l = p.l, u = p.u, sd = p.sd, w = p.w, b = p.backoff,
        execs = stats.execs, block = stats.block,
        to = TIMEOUTS.load(Ordering::Relaxed),
        ct = COMPLETED_TXNS.load(Ordering::Relaxed),
        fa = FATAL.load(Ordering::Relaxed),
        sc = STRAY_COMMITS.load(Ordering::Relaxed),
        dr = DIRTY_READS.load(Ordering::Relaxed),
        viol = STRAY_COMMITS.load(Ordering::Relaxed) + DIRTY_READS.load(Ordering::Relaxed),
    );
    if stats.execs == 0 {
        println!("WARNING ({label}): 0 complete executions, this run verified nothing");
    }
}

fn main() {
    let mut mode = String::from("compare");
    let mut p = Params {
        l: 0, u: 3, sd: 0, w: 2, backoff: 0, txns: 2, retries: 1, fix: Fix::None,
        check_stray: true, check_dirty: true,
    };
    let mut keep_going = false;
    let mut verbose = 0usize;
    let mut parallel = String::from("none");
    let mut args = std::env::args().skip(1);
    let val = |args: &mut std::iter::Skip<std::env::Args>, flag: &str| -> String {
        args.next().unwrap_or_else(|| cli_bail(&format!("{flag} needs a value")))
    };
    fn num<T: std::str::FromStr>(v: String, flag: &str) -> T {
        v.parse().unwrap_or_else(|_| cli_bail(&format!("{flag} got a malformed value")))
    }
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => mode = val(&mut args, "--mode"),
            "--l" => p.l = num(val(&mut args, "--l"), "--l"),
            "--u" => p.u = num(val(&mut args, "--u"), "--u"),
            "--sd" => p.sd = num(val(&mut args, "--sd"), "--sd"),
            "--w" => p.w = num(val(&mut args, "--w"), "--w"),
            "--backoff" => p.backoff = num(val(&mut args, "--backoff"), "--backoff"),
            "--txns" => p.txns = num(val(&mut args, "--txns"), "--txns"),
            "--retries" => p.retries = num(val(&mut args, "--retries"), "--retries"),
            "--fix" => {
                p.fix = match val(&mut args, "--fix").as_str() {
                    "none" => Fix::None,
                    "epoch-bump" => Fix::EpochBump,
                    other => cli_bail(&format!("invalid --fix: {other} (none|epoch-bump)")),
                }
            }
            "--property" => {
                (p.check_stray, p.check_dirty) = match val(&mut args, "--property").as_str() {
                    "both" => (true, true),
                    "stray" => (true, false),
                    "dirty" => (false, true),
                    other => cli_bail(&format!("invalid --property: {other} (both|stray|dirty)")),
                }
            }
            "--keep-going" => keep_going = true,
            "--verbose" => verbose = num(val(&mut args, "--verbose"), "--verbose"),
            "--parallel" => parallel = val(&mut args, "--parallel"),
            "--help" | "-h" => {
                eprintln!(
                    "Usage: nisshi_txn_timed [--mode baseline|timed|compare] [--l L] [--u U] [--sd SD] \
                     [--w W] [--backoff B] [--txns T] [--retries R] [--fix none|epoch-bump] \
                     [--keep-going] [--verbose N] [--parallel none|shared|partitioned]"
                );
                std::process::exit(0);
            }
            other => cli_bail(&format!("unknown argument: {other}")),
        }
    }
    if p.l > p.u {
        cli_bail("L must be <= U");
    }
    if p.w < 1 || p.txns < 1 {
        cli_bail("W and T must be >= 1");
    }
    PARALLEL.set(parallel).expect("set once");
    match mode.as_str() {
        "baseline" => {
            let (s, d) = run(Mode::Baseline, p, keep_going, verbose);
            print_one("baseline", p, &s, d);
        }
        "timed" => {
            let (s, d) = run(Mode::Timed, p, keep_going, verbose);
            print_one("timed", p, &s, d);
        }
        "compare" => {
            let (s, d) = run(Mode::Baseline, p, keep_going, verbose);
            print_one("baseline", p, &s, d);
            let (s, d) = run(Mode::Timed, p, keep_going, verbose);
            print_one("timed", p, &s, d);
        }
        other => cli_bail(&format!("invalid --mode: {other}")),
    }
}
