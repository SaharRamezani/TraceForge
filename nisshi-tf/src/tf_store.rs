//! nisshi's object store, turned into a TraceForge service.
//!
//! nisshi's in-memory engine (`DynoStore`) keeps all of its cluster state,
//! including producers and transactions, in ONE object (`meta.json`) which it
//! updates with a read-modify-conditional-put loop. Two concurrent requests
//! therefore race through the object store, not through messages, and a
//! message-passing checker would never see that race.
//!
//! This wrapper puts the race back where TraceForge can see it: every operation
//! on a gated path asks an arbiter thread for permission, does the operation,
//! and reports back. The arbiter serves one operation at a time, so the order
//! of operations is decided by which request the arbiter reads, which is
//! exactly the choice TraceForge explores exhaustively.
//!
//! Gating only `meta.json` is deliberate: it is the contended object, and
//! leaving the uncontended ones ungated keeps the state space on the race that
//! matters. Everything else is nisshi's own code, unchanged.

use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use traceforge::thread::ThreadId;

/// Arbiter protocol. Three distinct message TYPES, so no receive can mistake
/// one role for another: `Ctl` goes to the arbiter (a request, or the stop
/// signal from main), `Grant` comes back to the requester, and `Release` is the
/// requester saying its operation is done.
#[derive(Clone, Debug, PartialEq)]
pub enum Ctl {
    Req { from: ThreadId },
    Stop,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Grant;

#[derive(Clone, Debug, PartialEq)]
pub struct Release;

/// Runs in its own TraceForge thread: serve one gated operation at a time.
/// The `recv` below is the choice point: when several threads have a request
/// pending, TraceForge explores every order of serving them.
pub fn arbiter() {
    loop {
        match traceforge::recv_msg_block::<Ctl>() {
            Ctl::Req { from } => {
                GATED_OPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                traceforge::send_msg(from, Grant);
                let _release: Release =
                    traceforge::recv_tagged_msg_block::<_, Release>(move |t, _| t == from);
            }
            Ctl::Stop => return,
        }
    }
}

/// Crash injection. A broker can die mid-request, so the harness can stop a
/// chosen thread at its N-th gated store operation by failing that operation.
/// Keyed by TraceForge thread id, NOT a thread_local: TraceForge threads are
/// coroutines on one OS thread, so a thread_local would be shared by all of them.
static CRASH: std::sync::Mutex<Option<(ThreadId, usize, usize)>> = std::sync::Mutex::new(None);

/// Stop the calling thread's `n`-th gated store operation (1-based).
pub fn crash_after(n: usize) {
    *CRASH.lock().unwrap() = Some((traceforge::thread::current().id(), n, 0));
}

pub fn clear_crash() {
    *CRASH.lock().unwrap() = None;
}

fn crash_now() -> bool {
    let me = traceforge::thread::current().id();
    let mut g = CRASH.lock().unwrap();
    match g.as_mut() {
        Some((tid, at, seen)) if *tid == me => {
            *seen += 1;
            *seen == *at
        }
        _ => false,
    }
}

fn trace_on() -> bool {
    std::env::var("TF_TRACE").map(|v| v == "1").unwrap_or(false)
}

/// Counts uses of the two lazy stream APIs, which cannot be gated at the call.
/// Nonzero means the experiment touched a path whose accesses are not ordered
/// by messages, so its result must not be trusted.
/// Gated operations served, over all explored executions: zero would mean the
/// race was never exercised.
pub static GATED_OPS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub static UNGATED_LIST: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[derive(Debug)]
pub struct TfObjectStore {
    /// ONE store, shared by every broker in the harness: the object store is
    /// the only state nisshi's brokers share, exactly as in a deployment over
    /// S3. Every access below is gated, so the order in which brokers touch it
    /// is decided by messages and therefore explored by TraceForge.
    inner: std::sync::Arc<InMemory>,
    arbiter: ThreadId,
}

impl TfObjectStore {
    pub fn new(arbiter: ThreadId, inner: std::sync::Arc<InMemory>) -> Self {
        Self { inner, arbiter }
    }

    fn enter(&self, location: &Path, op: &'static str) -> bool {
        if trace_on() {
            println!("    [{:?}] {op} {location}", traceforge::thread::current().id());
        }
        let me = traceforge::thread::current().id();
        let arb = self.arbiter;
        traceforge::send_msg(arb, Ctl::Req { from: me });
        let _grant: Grant = traceforge::recv_tagged_msg_block::<_, Grant>(move |t, _| t == arb);
        true
    }

    fn leave(&self, held: bool) {
        if held {
            traceforge::send_msg(self.arbiter, Release);
        }
    }
}

impl Display for TfObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TfObjectStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for TfObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> OsResult<PutResult> {
        let mode = format!("{:?}", options.mode);
        let held = self.enter(location, "put");
        if crash_now() {
            if trace_on() {
                println!("      put mode={mode} -> SIMULATED BROKER CRASH");
            }
            self.leave(held);
            return Err(object_store::Error::Generic {
                store: "tf",
                source: "simulated broker crash".into(),
            });
        }
        let r = self.inner.put_opts(location, payload, options).await;
        if trace_on() {
            println!(
                "      put mode={mode} -> {}",
                match &r {
                    Ok(_) => "ok".to_string(),
                    Err(e) => format!("{e}"),
                }
            );
        }
        self.leave(held);
        r
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        let held = self.enter(location, "put_multipart");
        let r = self.inner.put_multipart_opts(location, options).await;
        self.leave(held);
        r
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let held = self.enter(location, "get");
        let r = self.inner.get_opts(location, options).await;
        self.leave(held);
        r
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        let held = self.enter(location, "get_ranges");
        let r = self.inner.get_ranges(location, ranges).await;
        self.leave(held);
        r
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        UNGATED_LIST.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.delete_stream(locations)
    }

    /// NOT gated: a stream is consumed lazily, after this call returns, so a
    /// gate here would not cover the accesses. The harness asserts that the
    /// paths under test never list (the counter is checked in main).
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        UNGATED_LIST.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        let held = self.enter(&Path::from("list"), "list_with_delimiter");
        let r = self.inner.list_with_delimiter(prefix).await;
        self.leave(held);
        r
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        let held = self.enter(from, "copy");
        let r = self.inner.copy_opts(from, to, options).await;
        self.leave(held);
        r
    }
}
