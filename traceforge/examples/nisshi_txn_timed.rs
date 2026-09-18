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
//! With sd = 0 a request that reaches the storage while it is busy with
//! another connection's request may expire unread; the storage then waits
//! forever and the execution ends blocked (the `blocked=` count). This only
//! drops executions, never adds one: every processed request is processed in
//! arrival order, so the violations found and the boundaries measured are
//! those of the full model. Raising --sd does not remove blocked executions
//! (L=1 U=8 W=1: sd 0/2/10 give blocked 6/7/8) and leaves the violations
//! unchanged (stray 1, dirty 2 at all three).
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
const TAG_CONN_CLOSE: u32 = 2;
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
    Close,
}

#[derive(Clone, Debug, PartialEq)]
enum StorageMsg {
    Req(Envelope),
    Done { total: u32 },
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

fn producer(p: Params, storage: ThreadId, conns: Vec<ThreadId>) {
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

    traceforge::send_msg(c.storage, StorageMsg::Done { total: c.sent });
    for conn in c.conns.clone() {
        traceforge::send_tagged_msg_timed(conn, TAG_CONN_CLOSE, ConnMsg::Close, 0, 0);
    }
}

// =====================================================================
// Connection: one TCP connection read by a stateless broker
// =====================================================================

fn connection(storage: ThreadId) {
    loop {
        match traceforge::recv_msg_block_timed::<ConnMsg>() {
            ConnMsg::Req(env) => traceforge::send_msg(storage, StorageMsg::Req(env)),
            ConnMsg::Close => break,
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
    let mut processed: u32 = 0;
    let mut total: Option<u32> = None;
    while total != Some(processed) {
        match traceforge::recv_msg_block_timed::<StorageMsg>() {
            StorageMsg::Done { total: t } => total = Some(t),
            StorageMsg::Req(env) => {
                processed += 1;
                let resp = db.handle(&env.req, fix);
                db.check_visibility();
                traceforge::send_tagged_msg(env.reply_to, resp_tag(env.corr), resp);
            }
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
    let mut builder = Config::builder().with_progress_report(usize::MAX).with_verbose(verbose);
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
        let storage_h = thread::spawn(move || storage(p));
        let storage_tid = storage_h.thread().id();
        let conns: Vec<_> = (0..=p.retries)
            .map(|_| thread::spawn(move || connection(storage_tid)))
            .collect();
        let conn_ids: Vec<ThreadId> = conns.iter().map(|h| h.thread().id()).collect();
        let producer_h = thread::spawn(move || producer(p, storage_tid, conn_ids));
        let _ = producer_h.join();
        for h in conns {
            let _ = h.join();
        }
        let _ = storage_h.join();
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
