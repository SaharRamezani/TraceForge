//! Runs nisshi's REAL storage engine under TraceForge and explores the
//! interleavings of two concurrent requests through their own code.
//!
//! Nothing here models nisshi. `DynoStore` is their engine, built over the
//! object-store service in `tf_store.rs`; every call below is their public
//! `Storage` API; their futures are polled by TraceForge's executor.
//!
//! Experiment 1, producer fencing: two clients call InitProducerId for the SAME
//! transaction id at the same time. Kafka's fencing rule is that each call
//! produces a strictly newer producer epoch, so at most one producer is current
//! and the older one is fenced off. If two concurrent calls can return the same
//! (producer id, epoch), two live producers believe they are current, and
//! nothing fences either of them.
//!
//! The invariant is checked in main, after both replies, so it cannot fire on a
//! partial execution.

mod tf_store;

use std::time::Instant;

use nisshi_storage::dynostore::DynoStore;
use nisshi_storage::{BrokerRegistrationRequest, Storage};
use tf_store::{Ctl, TfObjectStore, UNGATED_LIST, arbiter};
use traceforge::thread::{self, ThreadId};
use url::Url;
use uuid::Uuid;

const CLUSTER: &str = "tf-cluster";
const TXN: &str = "txn-1";

#[derive(Clone, Debug, PartialEq)]
struct Report {
    who: u8,
    producer_id: i64,
    epoch: i16,
}

/// One client: their InitProducerId, as a broker would call it.
fn client(who: u8, storage: DynoStore, main_tid: ThreadId) {
    let report = traceforge::future::block_on(async move {
        let producer = storage
            .init_producer(Some(TXN), 10_000, Some(-1), Some(-1))
            .await
            .expect("init_producer");
        Report { who, producer_id: producer.id, epoch: producer.epoch }
    });
    traceforge::send_msg(main_tid, report);
}

/// Experiment 2, zombie fencing. Broker A takes the transaction id (epoch 0),
/// then broker B takes the SAME transaction id (epoch 1). Kafka's rule is that
/// the older producer is now fenced: its writes must be rejected with
/// INVALID_PRODUCER_EPOCH. Here A tries to use its stale epoch AFTER B's bump
/// has returned, so no timing is involved: either their engine rejects the
/// stale writer or it does not.
fn fencing_after_bump() {
    use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
    use nisshi_sans_io::create_topics_request::CreatableTopic;
    use nisshi_sans_io::record::{Record, inflated};
    use nisshi_sans_io::BatchAttribute;
    use nisshi_storage::{Topition, TxnAddPartitionsRequest};

    let stats = traceforge::verify(
        traceforge::Config::builder().with_progress_report(usize::MAX).build(),
        || {
            let arb = thread::spawn(arbiter);
            let shared = std::sync::Arc::new(object_store::memory::InMemory::new());
            let store = DynoStore::new(CLUSTER, 111, TfObjectStore::new(arb.thread().id(), shared))
                .advertised_listener(Url::parse("tcp://localhost:9092").unwrap());

            traceforge::future::block_on(async {
                store
                    .register_broker(BrokerRegistrationRequest {
                        broker_id: 111,
                        cluster_id: CLUSTER.to_string(),
                        incarnation_id: Uuid::from_u128(0x1111_2222_3333_4444),
                        rack: None,
                    })
                    .await
                    .expect("register_broker");
                let _ = store
                    .create_topic(
                        CreatableTopic::default()
                            .name("t1".to_string())
                            .num_partitions(1)
                            .replication_factor(0)
                            .assignments(Some([].into()))
                            .configs(Some([].into())),
                        false,
                    )
                    .await
                    .expect("create_topic");

                // A takes the transaction id, then B takes it: A is now stale.
                let a = store
                    .init_producer(Some(TXN), 10_000, Some(-1), Some(-1))
                    .await
                    .expect("init A");
                let b = store
                    .init_producer(Some(TXN), 10_000, Some(-1), Some(-1))
                    .await
                    .expect("init B");
                println!("A: id={} epoch={} | B: id={} epoch={}", a.id, a.epoch, b.id, b.epoch);

                // A, fenced, tries to use the transaction id with its old epoch.
                let add = store
                    .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                        transaction_id: TXN.to_string(),
                        producer_id: a.id,
                        producer_epoch: a.epoch,
                        topics: vec![
                            AddPartitionsToTxnTopic::default()
                                .name("t1".to_string())
                                .partitions(Some(vec![0])),
                        ],
                    })
                    .await;
                let add_codes: Vec<i16> = match &add {
                    Ok(r) => r
                        .zero_to_three()
                        .iter()
                        .flat_map(|t| t.results_by_partition.iter().flatten())
                        .map(|p| p.partition_error_code)
                        .collect(),
                    Err(e) => {
                        println!("stale AddPartitionsToTxn -> Err({e})");
                        vec![-1]
                    }
                };
                println!("stale AddPartitionsToTxn -> codes {add_codes:?}");

                let batch = inflated::Batch::builder()
                    .record(Record::builder().value(bytes::Bytes::from_static(b"zombie").into()))
                    .attributes(BatchAttribute::default().transaction(true).into())
                    .producer_id(a.id)
                    .producer_epoch(a.epoch)
                    .base_sequence(0)
                    .build()
                    .and_then(TryInto::try_into)
                    .expect("batch");
                let produced = store
                    .produce(Some(TXN), &Topition::new("t1".to_string(), 0), batch)
                    .await;
                match &produced {
                    Ok(offset) => println!("stale Produce -> ACCEPTED at offset {offset}"),
                    Err(e) => println!("stale Produce -> rejected: {e}"),
                }

                // The fenced producer must not be able to write.
                traceforge::assert(produced.is_err());
            });

            traceforge::send_msg(arb.thread().id(), Ctl::Stop);
            let _ = arb.join();
        },
    );
    println!("fencing_after_bump: execs={} blocked={}", stats.execs, stats.block);
}

/// Experiment 3, a write racing its own fencing. A is a live transactional
/// producer (epoch 0) that has already added its partition. Concurrently, B
/// takes the same transaction id, which bumps the epoch to 1 and fences A.
/// Whatever the engine decides, the client's answer and the log must agree:
/// a rejected write must not be visible, and an accepted write must be.
/// A disagreement is a bug with no parameters attached.
fn produce_races_bump() {
    use nisshi_sans_io::IsolationLevel;
    use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
    use nisshi_sans_io::create_topics_request::CreatableTopic;
    use nisshi_sans_io::record::{Record, inflated};
    use nisshi_sans_io::BatchAttribute;
    use nisshi_storage::{Topition, TxnAddPartitionsRequest};
    use std::time::Duration;

    static ACCEPTED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    static REJECTED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    #[derive(Clone, Debug, PartialEq)]
    struct Produced(bool);
    #[derive(Clone, Debug, PartialEq)]
    struct Bumped(i16);

    let start = Instant::now();
    let stats = traceforge::verify(
        traceforge::Config::builder()
            .with_progress_report(usize::MAX)
            .with_keep_going_after_error(true)
            .build(),
        || {
            let main_tid = thread::current().id();
            let arb = thread::spawn(arbiter);
            let arb_tid = arb.thread().id();
            let shared = std::sync::Arc::new(object_store::memory::InMemory::new());
            let broker = |shared: std::sync::Arc<object_store::memory::InMemory>| {
                DynoStore::new(CLUSTER, 111, TfObjectStore::new(arb_tid, shared))
                    .advertised_listener(Url::parse("tcp://localhost:9092").unwrap())
            };
            let setup = broker(shared.clone());
            let topition = Topition::new("t1".to_string(), 0);

            let producer_a = traceforge::future::block_on(async {
                setup
                    .register_broker(BrokerRegistrationRequest {
                        broker_id: 111,
                        cluster_id: CLUSTER.to_string(),
                        incarnation_id: Uuid::from_u128(0x1111_2222_3333_4444),
                        rack: None,
                    })
                    .await
                    .expect("register_broker");
                let _ = setup
                    .create_topic(
                        CreatableTopic::default()
                            .name("t1".to_string())
                            .num_partitions(1)
                            .replication_factor(0)
                            .assignments(Some([].into()))
                            .configs(Some([].into())),
                        false,
                    )
                    .await
                    .expect("create_topic");
                let a = setup
                    .init_producer(Some(TXN), 10_000, Some(-1), Some(-1))
                    .await
                    .expect("init A");
                let _ = setup
                    .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                        transaction_id: TXN.to_string(),
                        producer_id: a.id,
                        producer_epoch: a.epoch,
                        topics: vec![
                            AddPartitionsToTxnTopic::default()
                                .name("t1".to_string())
                                .partitions(Some(vec![0])),
                        ],
                    })
                    .await
                    .expect("add partitions");
                a
            });

            // A writes with its (still current) epoch.
            let writer = {
                let s = broker(shared.clone());
                let tp = topition.clone();
                thread::spawn(move || {
                    let ok = traceforge::future::block_on(async {
                        let batch = inflated::Batch::builder()
                            .record(Record::builder().value(bytes::Bytes::from_static(b"a1").into()))
                            .attributes(BatchAttribute::default().transaction(true).into())
                            .producer_id(producer_a.id)
                            .producer_epoch(producer_a.epoch)
                            .base_sequence(0)
                            .build()
                            .and_then(TryInto::try_into)
                            .expect("batch");
                        let r = s.produce(Some(TXN), &tp, batch).await;
                        if r.is_ok() {
                            ACCEPTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            REJECTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        r.is_ok()
                    });
                    traceforge::send_msg(main_tid, Produced(ok));
                })
            };

            // B takes the transaction id, fencing A.
            let bumper = {
                let s = broker(shared.clone());
                thread::spawn(move || {
                    let epoch = traceforge::future::block_on(async {
                        s.init_producer(Some(TXN), 10_000, Some(-1), Some(-1))
                            .await
                            .expect("init B")
                            .epoch
                    });
                    traceforge::send_msg(main_tid, Bumped(epoch));
                })
            };

            // Filter by sender: a typed receive in TraceForge takes the next
            // message and then checks its type, so two reply types need filters.
            let writer_tid = writer.thread().id();
            let bumper_tid = bumper.thread().id();
            let Produced(accepted) = traceforge::recv_tagged_msg_block::<_, Produced>(move |t, _| t == writer_tid);
            let Bumped(_epoch_b) = traceforge::recv_tagged_msg_block::<_, Bumped>(move |t, _| t == bumper_tid);
            let _ = writer.join();
            let _ = bumper.join();

            // Read the log back through a FRESH broker: a reader that has just
            // connected, with no cached watermarks of its own. Reading through
            // the setup instance instead would only show that instance's cache.
            let reader = broker(shared.clone());
            let visible = traceforge::future::block_on(async {
                let batches = reader
                    .fetch(&topition, 0, 0, 1_048_576, IsolationLevel::ReadUncommitted, Duration::from_secs(5))
                    .await
                    .expect("fetch");
                batches
                    .into_iter()
                    .filter_map(|b| inflated::Batch::try_from(b).ok())
                    .flat_map(|b| b.records)
                    .any(|r| r.value.as_ref().map(|v| v.as_ref() == b"a1").unwrap_or(false))
            });

            traceforge::send_msg(arb_tid, Ctl::Stop);
            let _ = arb.join();

            if accepted != visible {
                println!("DISAGREEMENT: produce accepted={accepted} but record visible={visible}");
            }
            traceforge::assert(accepted == visible);
        },
    );
    println!(
        "produce_races_bump: write ACCEPTED in {} executions, FENCED in {} (a zero here would mean \
         the fencing window never opened and the invariant was trivially true)",
        ACCEPTED.load(std::sync::atomic::Ordering::Relaxed),
        REJECTED.load(std::sync::atomic::Ordering::Relaxed),
    );
    println!(
        "produce_races_bump: execs={} blocked={} explored={} time={:.3}s",
        stats.execs,
        stats.block,
        stats.execs + stats.block,
        start.elapsed().as_secs_f64()
    );
}


/// POSITIVE CONTROL. Everything else in this file reports "no violation found",
/// which is worth nothing unless the harness can detect a violation that is
/// known to be there. This experiment drives the KAFKA-17754 order through the
/// same harness and the same real code: T1's EndTxn times out on one connection
/// and is retried on another, T2 starts and writes, and then the ORIGINAL
/// EndTxn for T1 arrives late. nisshi keys transaction state by (transaction
/// id, producer epoch), so the late EndTxn commits whatever transaction that
/// epoch is running now, which is T2.
///
/// Property: a read_committed consumer must not see T2's record while T2 is
/// open. This assertion is EXPECTED TO FIRE. If it ever stops firing, the
/// harness has stopped being able to see real violations.
fn control_stray_endtxn() {
    use nisshi_sans_io::IsolationLevel;
    use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
    use nisshi_sans_io::create_topics_request::CreatableTopic;
    use nisshi_sans_io::record::{Record, inflated};
    use nisshi_sans_io::BatchAttribute;
    use nisshi_storage::{Topition, TxnAddPartitionsRequest};
    use std::time::Duration;

    let stats = traceforge::verify(
        traceforge::Config::builder()
            .with_progress_report(usize::MAX)
            .with_keep_going_after_error(true)
            .build(),
        || {
            let arb = thread::spawn(arbiter);
            let shared = std::sync::Arc::new(object_store::memory::InMemory::new());
            let store = DynoStore::new(CLUSTER, 111, TfObjectStore::new(arb.thread().id(), shared))
                .advertised_listener(Url::parse("tcp://localhost:9092").unwrap());
            let topition = Topition::new("t1".to_string(), 0);

            let leaked = traceforge::future::block_on(async {
                store
                    .register_broker(BrokerRegistrationRequest {
                        broker_id: 111,
                        cluster_id: CLUSTER.to_string(),
                        incarnation_id: Uuid::from_u128(0x1111_2222_3333_4444),
                        rack: None,
                    })
                    .await
                    .expect("register_broker");
                let _ = store
                    .create_topic(
                        CreatableTopic::default()
                            .name("t1".to_string())
                            .num_partitions(1)
                            .replication_factor(0)
                            .assignments(Some([].into()))
                            .configs(Some([].into())),
                        false,
                    )
                    .await
                    .expect("create_topic");
                let p = store
                    .init_producer(Some(TXN), 10_000, Some(-1), Some(-1))
                    .await
                    .expect("init");

                let add = |n: &'static str| {
                    let store = store.clone();
                    let (id, epoch) = (p.id, p.epoch);
                    async move {
                        let _ = n;
                        store
                            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                                transaction_id: TXN.to_string(),
                                producer_id: id,
                                producer_epoch: epoch,
                                topics: vec![
                                    AddPartitionsToTxnTopic::default()
                                        .name("t1".to_string())
                                        .partitions(Some(vec![0])),
                                ],
                            })
                            .await
                            .expect("add partitions")
                    }
                };
                let produce = |value: &'static [u8], seq: i32| {
                    let store = store.clone();
                    let tp = topition.clone();
                    let (id, epoch) = (p.id, p.epoch);
                    async move {
                        let batch = inflated::Batch::builder()
                            .record(Record::builder().value(bytes::Bytes::from(value).into()))
                            .attributes(BatchAttribute::default().transaction(true).into())
                            .producer_id(id)
                            .producer_epoch(epoch)
                            .base_sequence(seq)
                            .build()
                            .and_then(TryInto::try_into)
                            .expect("batch");
                        store.produce(Some(TXN), &tp, batch).await
                    }
                };

                // T1: open, write, and commit (the retry that got through).
                let _ = add("T1").await;
                let _ = produce(b"t1", 0).await.expect("produce t1");
                let _ = store.txn_end(TXN, p.id, p.epoch, true).await.expect("end T1");

                // T2: open and write. T2 is NOT committed by its client.
                let _ = add("T2").await;
                let _ = produce(b"t2", 1).await.expect("produce t2");

                // The original EndTxn for T1, delayed on the old connection.
                let code = store.txn_end(TXN, p.id, p.epoch, true).await.expect("stray end");
                println!("stray EndTxn(T1) -> {code:?}");

                // A read_committed consumer looks while T2 is still open.
                let batches = store
                    .fetch(&topition, 0, 0, 1_048_576, IsolationLevel::ReadCommitted, Duration::from_secs(5))
                    .await
                    .expect("fetch");
                let seen: Vec<String> = batches
                    .into_iter()
                    .filter_map(|b| inflated::Batch::try_from(b).ok())
                    .flat_map(|b| b.records)
                    .filter_map(|r| r.value.as_ref().map(|v| String::from_utf8_lossy(v).into_owned()))
                    .collect();
                println!("read_committed sees {seen:?} while T2 is open");
                seen.iter().any(|v| v == "t2")
            });

            traceforge::send_msg(arb.thread().id(), Ctl::Stop);
            let _ = arb.join();

            if leaked {
                println!("CONTROL FIRED: T2's record is visible to a read_committed consumer while T2 is open");
            }
            traceforge::assert(!leaked);
        },
    );
    println!(
        "control_stray_endtxn: execs={} blocked={} (the assertion above is EXPECTED to fire)",
        stats.execs, stats.block
    );
}


/// Experiment 4, no lost write. An idempotent producer's write is a two-step
/// affair in their engine: the per-epoch sequence number is advanced in
/// meta.json FIRST (dynostore.rs, the `with_mut` that returns
/// OutOfOrderSequenceNumber / DuplicateSequenceNumber), and the record object is
/// written AFTER. A broker that dies between the two leaves the sequence
/// advanced with no record.
///
/// The client then does what Kafka clients do: it retries the same batch, same
/// producer id, same epoch, same base sequence. If the engine answers
/// DuplicateSequenceNumber, it is telling the client "you already wrote this",
/// so the record must actually be there. The property is therefore:
///
///     retry rejected as duplicate  =>  the record is in the log
///     retry accepted               =>  the record is in the log exactly once
///
/// The crash point is a nondeterministic choice, so TraceForge enumerates every
/// place the first broker can die. This assertion CAN fail: unlike the earlier
/// race, nothing in the code shape makes it true by construction.
fn no_lost_write() {
    use nisshi_sans_io::IsolationLevel;
    use nisshi_sans_io::create_topics_request::CreatableTopic;
    use nisshi_sans_io::record::{Record, inflated};
    use nisshi_storage::Topition;
    use std::time::Duration;

    static DUP_REJECTED_AND_ABSENT: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);
    static OUTCOMES: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

    let start = Instant::now();
    let stats = traceforge::verify(
        traceforge::Config::builder()
            .with_progress_report(usize::MAX)
            .with_keep_going_after_error(true)
            .build(),
        || {
            let arb = thread::spawn(arbiter);
            let arb_tid = arb.thread().id();
            let shared = std::sync::Arc::new(object_store::memory::InMemory::new());
            let broker = |shared: std::sync::Arc<object_store::memory::InMemory>| {
                DynoStore::new(CLUSTER, 111, TfObjectStore::new(arb_tid, shared))
                    .advertised_listener(Url::parse("tcp://localhost:9092").unwrap())
            };
            let topition = Topition::new("t1".to_string(), 0);
            let setup = broker(shared.clone());

            let producer = traceforge::future::block_on(async {
                setup
                    .register_broker(BrokerRegistrationRequest {
                        broker_id: 111,
                        cluster_id: CLUSTER.to_string(),
                        incarnation_id: Uuid::from_u128(0x1111_2222_3333_4444),
                        rack: None,
                    })
                    .await
                    .expect("register_broker");
                let _ = setup
                    .create_topic(
                        CreatableTopic::default()
                            .name("t1".to_string())
                            .num_partitions(1)
                            .replication_factor(0)
                            .assignments(Some([].into()))
                            .configs(Some([].into())),
                        false,
                    )
                    .await
                    .expect("create_topic");
                setup
                    .init_producer(None, 10_000, Some(-1), Some(-1))
                    .await
                    .expect("init producer")
            });

            let batch = || {
                inflated::Batch::builder()
                    .record(Record::builder().value(bytes::Bytes::from_static(b"w1").into()))
                    .producer_id(producer.id)
                    .producer_epoch(producer.epoch)
                    .base_sequence(0)
                    .build()
                    .and_then(TryInto::try_into)
                    .expect("batch")
            };

            // Broker 1 writes, and dies at its k-th store operation. k is a
            // nondeterministic choice, so every crash point is explored,
            // including "does not crash".
            let first = {
                let s = broker(shared.clone());
                let tp = topition.clone();
                let b = batch();
                thread::spawn(move || {
                    let mut k = 1usize;
                    while k < 5 && traceforge::nondet() {
                        k += 1;
                    }
                    if k < 5 {
                        tf_store::crash_after(k);
                    }
                    let r = traceforge::future::block_on(async { s.produce(None, &tp, b).await });
                    tf_store::clear_crash();
                    format!("{r:?}")
                })
            };
            let first_outcome = first.join().unwrap_or_else(|_| "panic".to_string());

            // The client retries the SAME batch on a healthy broker.
            let retry = {
                let s = broker(shared.clone());
                let tp = topition.clone();
                let b = batch();
                thread::spawn(move || {
                    let r = traceforge::future::block_on(async { s.produce(None, &tp, b).await });
                    format!("{r:?}")
                })
            };
            let retry_outcome = retry.join().unwrap_or_else(|_| "panic".to_string());

            // What is actually in the log, read through a fresh broker.
            let reader = broker(shared.clone());
            let copies = traceforge::future::block_on(async {
                let batches = reader
                    .fetch(
                        &topition,
                        0,
                        0,
                        1_048_576,
                        IsolationLevel::ReadUncommitted,
                        Duration::from_secs(5),
                    )
                    .await
                    .expect("fetch");
                batches
                    .into_iter()
                    .filter_map(|b| inflated::Batch::try_from(b).ok())
                    .flat_map(|b| b.records)
                    .filter(|r| r.value.as_ref().map(|v| v.as_ref() == b"w1").unwrap_or(false))
                    .count()
            });

            traceforge::send_msg(arb_tid, Ctl::Stop);
            let _ = arb.join();

            let duplicate_rejected = retry_outcome.contains("DuplicateSequenceNumber");
            let retry_ok = retry_outcome.starts_with("Ok");
            OUTCOMES.lock().unwrap().push(format!(
                "first={first_outcome} retry={retry_outcome} copies={copies}"
            ));

            if duplicate_rejected && copies == 0 {
                DUP_REJECTED_AND_ABSENT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                println!(
                    "LOST WRITE: retry rejected as duplicate but the record is absent \
                     (first={first_outcome})"
                );
            }
            // Told "already written" => it must be written.
            traceforge::assert(!(duplicate_rejected && copies == 0));
            // Accepted => exactly one copy.
            traceforge::assert(!(retry_ok && copies != 1));
        },
    );
    let mut seen: std::collections::BTreeMap<String, usize> = Default::default();
    for o in OUTCOMES.lock().unwrap().iter() {
        *seen.entry(o.clone()).or_default() += 1;
    }
    println!("no_lost_write: distinct outcomes:");
    for (o, n) in &seen {
        println!("   x{n:<5} {o}");
    }
    println!(
        "no_lost_write: execs={} blocked={} explored={} lost_writes={} time={:.3}s",
        stats.execs,
        stats.block,
        stats.execs + stats.block,
        DUP_REJECTED_AND_ABSENT.load(std::sync::atomic::Ordering::Relaxed),
        start.elapsed().as_secs_f64()
    );
}

fn main() {
    if std::env::var("EXPERIMENT").map(|v| v == "lostwrite").unwrap_or(false) {
        no_lost_write();
        return;
    }
    if std::env::var("EXPERIMENT").map(|v| v == "control").unwrap_or(false) {
        control_stray_endtxn();
        return;
    }
    if std::env::var("EXPERIMENT").map(|v| v == "race").unwrap_or(false) {
        produce_races_bump();
        return;
    }
    if std::env::var("EXPERIMENT").map(|v| v == "fence").unwrap_or(false) {
        fencing_after_bump();
        return;
    }
    let start = Instant::now();
    let stats = traceforge::verify(
        traceforge::Config::builder().with_progress_report(usize::MAX).build(),
        || {
            let main_tid = thread::current().id();

            // The object store is a service thread, so their reads and writes
            // of meta.json become message events TraceForge can order.
            let arb = thread::spawn(arbiter);
            let arb_tid = arb.thread().id();
            // One object store, two brokers over it: each broker gets its own
            // engine (its own caches), which is how nisshi is deployed. Sharing
            // one engine would also share its in-process cache, and TraceForge
            // orders messages, not shared memory.
            let shared = std::sync::Arc::new(object_store::memory::InMemory::new());
            let broker = |shared: std::sync::Arc<object_store::memory::InMemory>| {
                DynoStore::new(CLUSTER, 111, TfObjectStore::new(arb_tid, shared))
                    .advertised_listener(Url::parse("tcp://localhost:9092").unwrap())
            };
            let store = broker(shared.clone());

            // Setup, before any concurrency.
            traceforge::future::block_on(async {
                store
                    .register_broker(BrokerRegistrationRequest {
                        broker_id: 111,
                        cluster_id: CLUSTER.to_string(),
                        incarnation_id: Uuid::from_u128(0x1111_2222_3333_4444),
                        rack: None,
                    })
                    .await
                    .expect("register_broker");
            });

            let two = std::env::var("NCLIENTS").map(|v| v != "1").unwrap_or(true);
            let a = {
                let s = broker(shared.clone());
                thread::spawn(move || client(0, s, main_tid))
            };
            let b = two.then(|| {
                let s = broker(shared.clone());
                thread::spawn(move || client(1, s, main_tid))
            });

            let r1: Report = traceforge::recv_msg_block();
            let r2: Report = if two { traceforge::recv_msg_block() } else { r1.clone() };
            let _ = a.join();
            if let Some(b) = b {
                let _ = b.join();
            }
            traceforge::send_msg(arb.thread().id(), Ctl::Stop);
            let _ = arb.join();

            // Fencing: two concurrent InitProducerId for one transaction id must
            // not leave two producers holding the same (id, epoch).
            if two {
                let mut pair = [(r1.who, r1.producer_id, r1.epoch), (r2.who, r2.producer_id, r2.epoch)];
                pair.sort();
                println!(
                    "outcome: A(id={},epoch={}) B(id={},epoch={}) gated_ops={}",
                    pair[0].1, pair[0].2, pair[1].1, pair[1].2,
                    tf_store::GATED_OPS.load(std::sync::atomic::Ordering::Relaxed),
                );
                traceforge::assert(
                    !(r1.producer_id == r2.producer_id && r1.epoch == r2.epoch),
                );
            } else {
                println!("single client: id={} epoch={}", r1.producer_id, r1.epoch);
            }
        },
    );
    let ungated = UNGATED_LIST.load(std::sync::atomic::Ordering::Relaxed);
    if ungated > 0 {
        println!("WARNING: {ungated} ungated stream operations: this result is not trustworthy");
    }
    println!(
        "fencing: execs={} blocked={} explored={} time={:.3}s",
        stats.execs,
        stats.block,
        stats.execs + stats.block,
        start.elapsed().as_secs_f64()
    );
}
