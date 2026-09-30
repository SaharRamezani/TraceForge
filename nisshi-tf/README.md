# nisshi-tf: running nisshi's real code under TraceForge

This crate does not model nisshi. It links nisshi as a library and runs **their**
storage engine, their transaction state machine and their fetch path inside
TraceForge executions. TraceForge supplies the threads, the executor that polls
their futures, and the exploration of orders.

Branch `nissih`. Their source is cloned by `tools/fetch-nisshi.sh` into
`.nisshi-src/` (gitignored), pinned at commit b3d0cea, with one recorded change:
`tools/nisshi-visibility.patch` turns `mod dynostore` into `pub mod dynostore`,
a visibility change only, so the harness can hand their engine an object store.

## How their concurrency is made visible

nisshi's in-memory engine keeps cluster state (producers, transactions, topics)
in ONE object, `meta.json`, updated by read-modify-conditional-put with a retry
loop. Two concurrent requests therefore race **through the object store**, not
through messages, and a message-passing checker would see nothing.

`src/tf_store.rs` puts that race where TraceForge can explore it: the object
store becomes a service. Every access asks an arbiter thread for permission,
performs the operation, and releases. The arbiter serves one operation at a
time, so the order of store operations is decided by which request the arbiter
reads, which is exactly the choice TraceForge enumerates.

Two design points that took a failed attempt each to find:

* **One engine per broker, one object store for all of them.** Sharing a single
  `DynoStore` across threads also shares its in-process caches, and TraceForge
  orders messages, not shared memory, so replay diverged. Separate engines over
  one shared store is also what nisshi actually deploys.
* **Distinct message types per role** (`Ctl`, `Grant`, `Release`). A typed
  receive in TraceForge takes the next message and then checks its type, so two
  reply types on one thread need sender filters.

`list` and `delete_stream` return lazily-consumed streams and cannot be gated at
the call; `UNGATED_LIST` counts them so an experiment that touches such a path
reports that its result is not trustworthy.

## Experiments (`EXPERIMENT=`)

| name | what it asks | result |
|---|---|---|
| (default) | Two brokers call InitProducerId for the same transaction id at once. May two live producers end up with the same (id, epoch)? | **No.** 4 interleavings, invariant holds. Non-vacuous: every execution takes the conditional-put conflict path (both attempt `Create`, the loser re-reads and retries with `Update`, giving epoch 1). |
| `fence` | After a second InitProducerId bumps the epoch, may the older producer still write? | **No.** Their engine rejects it: AddPartitionsToTxn returns code 90, produce returns `ProducerFenced`. |
| `race` | A transactional write racing the epoch bump that fences it. Do the client's answer and the log ever disagree? | **WITHDRAWN as evidence.** An adversarial review showed the invariant cannot fail here: in `produce` the `ProducerFenced` check (dynostore.rs, first `with_mut` on meta) precedes the watermark bump and the record write, so a fenced write does nothing and an accepted write does everything. `accepted == visible` is true by construction, not because nisshi is correct. The 19,588 executions (76 fenced, 19,512 accepted) establish nothing about their code. Kept only as a regression that the search runs. |
| `lostwrite` | An idempotent producer's write, with a broker that can die at any of its store writes, then the client's retry of the same batch. If the retry is told "already stored", is the record actually there? | **NO: a lost write.** Fires at every crash point after the sequence advance. See the finding below. |
| `control` | POSITIVE CONTROL, expected to fire: the KAFKA-17754 order (T1's EndTxn retried, T2 opened and written, then T1's original EndTxn arrives late). May a read_committed consumer see T2's record while T2 is open? | **Yes, it fires.** `read_committed sees ["t1", <marker>, "t2", <marker>]` while T2 is open. The harness detects a real violation in their real code, so a "no violation" result elsewhere is evidence rather than silence. |

No new bug so far. These are exhaustive negative results on real code, which is
the point of the harness: each question above took minutes to ask once the
harness existed.

## Finding: a silently lost write in the dynostore (memory and S3) engine

Their idempotent-producer write is three object-store writes in this order:

1. `meta.json`: advance the producer's per-epoch sequence number (the check that
   returns `OutOfOrderSequenceNumber` / `DuplicateSequenceNumber` lives in the same
   `with_mut`), committed;
2. `watermark.json`: advance the partition high watermark, committed;
3. `records/<offset>.batch`: write the record itself.

Nothing rolls step 1 back. If the store write fails at step 2 or step 3 (a transient
S3 error, throttling, a dropped connection, a broker that dies), the sequence number
says the batch was stored and the record is not there. The client then does what a
Kafka client does, it retries the same batch with the same producer id, epoch and base
sequence, and the engine answers `DuplicateSequenceNumber`, whose meaning is "I already
have this batch".

Found by `EXPERIMENT=lostwrite` (the crash point is a nondeterministic choice, so every
one is explored), and then reproduced **without TraceForge at all** by
`cargo run --bin repro_lost_write`: plain tokio, plain nisshi, an object store whose
Nth write fails.

```
--- object store fails put #2
  put #1 clusters/repro/meta.json                                  <- sequence advanced, committed
  put #2 .../watermark.json -> INJECTED FAILURE
  produce #1      -> Err(ObjectStore(...))
  produce #2 (the client's retry of the same batch) -> Err(Api(DuplicateSequenceNumber))
  records in the log: 0                                            <- the write is gone
```

Failing put #1 instead is safe: the retry succeeds and the record lands. The bug is
exactly the window after the sequence has been committed.

How bad it is depends on how the failure reaches the client, and both paths are bad:

* **Broker dies or the connection drops** (what the model-checked experiment models):
  the client retries, is told `DuplicateSequenceNumber`, treats the batch as stored, and
  the record is silently lost.
* **The broker answers**: `ProduceService` maps a non-API error to
  `ErrorCode::UnknownServerError` for that partition, so the client at least sees a
  failure, but the sequence number and an offset have been burned, which can turn later
  batches from that producer into spurious `OutOfOrderSequenceNumber`.

Verification (2026-09-21), so that "real bug" is not a figure of speech:

* **nisshi is unmodified.** `git diff` in `.nisshi-src/nisshi` is exactly the one-line
  `mod dynostore` to `pub mod dynostore` change, visibility only. The engine under test is
  built by the same `DynoStore::new(cluster, node, store)` call their production path uses
  for `memory://` (`nisshi-storage/src/lib.rs`, the `"memory"` arm), with their `Cache` and
  `Metron` wrappers in place.
* **The failure is theirs, not the injection's.** The injected error is
  `object_store::Error::Generic`, which takes exactly the arm a real S3 5xx or network error
  takes: `Err(otherwise) => Err(otherwise.into())` in `DynoStore::put`,
  `Err(err) => return Err(err.into())` in `OptiCon::with_mut`, and a bare `?` on the record
  `put_opts`. There is no compensation after any of them.
* **Present at their current HEAD.** Upstream `origin/main` is 870ba9f; zero commits have
  touched `dynostore.rs` since the pin, and `repro_lost_write` run against HEAD (same one-line
  patch) loses the write at the same two failure points.
* **The client really treats it as success.** Kafka 3.9 `Sender.java`, on
  `DUPLICATE_SEQUENCE_NUMBER`: "The only thing we can do is to return success to the user"
  and `completeBatch(batch, response)`; a batch re-sent after a disconnect or timeout is
  re-enqueued with its sequence number unchanged (`reenqueueBatch`).
* **A drop-in test in their own tree fails.** `tools/nisshi-lost-write-test.rs` appended to
  `nisshi-storage/src/dynostore/tests.rs` (no visibility patch needed there) compiles and fails
  on their code: lost writes at store writes #2 and #3, safe at #1 and #4. Their tree was
  reverted afterwards. The text to report it is `nisshi-tf/ISSUE_DRAFT.md`.
* **Not already reported.** The only related upstream issue is #647 (contention from updating
  the stored sequence on gcloud): same design, different symptom, no data loss mentioned.

Scope: the `dynostore` engine (`memory://` and S3 backends). The Postgres backend runs its
produce inside a SQL transaction (`pg.rs`, `c.transaction()` / `tx.commit()`), so it is not
exposed this way; the SQLite backend was not checked. The finding needs no timing
parameters and no concurrency: it is a failure window in a non-atomic three-step write.
Not reported upstream; ask Sahar first.

## Why a negative result here means something

A checker that cannot fail is worthless, so the claims above rest on four checks, not on the
absence of output:

1. **Positive control** (`EXPERIMENT=control`): a known real violation of a real property is
   detected through this harness, in their code. Keep it green-means-fires: if it ever stops
   firing, the harness has stopped seeing violations. Note what it does and does not show: it
   explores ONE execution, so it validates the assertion path and the property, not the search.
   The search is validated by `lostwrite`, where the violating executions are found by
   exploration rather than written by hand.
2. **Non-vacuity, measured, not assumed**: with `TF_TRACE=1` every explored execution of the
   default experiment takes the conditional-put conflict path (both brokers attempt `Create`, the
   loser re-reads and retries with `Update`). In `EXPERIMENT=race` the counters report the write
   fenced in 76 executions and accepted in 19,512, so the fencing window really opens and the
   invariant is checked on both sides rather than being trivially true.
3. **Ungated-path guard**: `UNGATED_LIST` counts stream APIs that cannot be gated, so an
   experiment that silently escapes the arbiter's ordering reports itself.
4. **Harness bugs found and fixed before trusting any result**: a shared engine (shared caches)
   made replay diverge; two reply types on one thread needed sender filters; `max_wait = 0` made
   their fetch return nothing, which looked exactly like a lost write.

What this does NOT establish is stated under "Out of reach" below, plus: each client runs its own
engine, so races between two connections INSIDE one broker are not explored by these three
experiments (the control drives that path sequentially instead).

## Determinism hazards found in their code

These matter for anyone checking nisshi, and two of them cost debugging time here:

1. **Wall-clock TTL cache.** `dynostore/metadata.rs` caches object versions in an
   `ExpiringSizedCache` with a 5 s retention hard-coded in `DynoStore::new`.
   Whether a read reaches the store depends on elapsed real time.
2. **Wall-clock fetch deadline.** `fetch` computes `has_deadline_expired()` from
   `SystemTime::now()`, and its list loop runs only while the deadline holds.
   With `max_wait = 0` it returns nothing, which looked exactly like a lost
   write until traced. Any fetch slower than `max_wait` changes behaviour.
3. **`Uuid::now_v7()` and `SystemTime::now()`** appear in request paths (e.g.
   broker registration, transaction start times).

A checker running their unmodified code needs shims for all three.

## Out of reach without more work

* The transaction-timeout reaper: `maintain_transactions` is a no-op in both the
  in-memory and SQLite engines; only Postgres implements it, so the
  reaper-versus-commit race cannot be checked in process.
* Anything whose timing lives inside their async code (the Fetch long-poll,
  session and connection timeouts). That needs timed futures in TraceForge: the
  async layer currently has no connection to the timed engine.
