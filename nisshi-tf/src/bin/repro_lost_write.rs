//! Standalone reproduction, NO TraceForge, NO model checking: plain tokio, plain
//! nisshi, and an object store whose Nth put fails the way a real one does
//! (network error, throttling, or a broker that dies at that moment).
//!
//! Their idempotent-producer write is two steps in this order:
//!   1. advance the producer's sequence number in meta.json  (committed)
//!   2. advance the partition watermark                      (committed)
//!   3. write the record object                              (fails here)
//! Nothing rolls step 1 back, so the client's retry of the SAME batch is
//! answered DuplicateSequenceNumber, which Kafka clients read as "already
//! stored, carry on". The record is never written: a silent lost write.
//!
//! Run: cargo run --bin repro_lost_write

use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::stream::BoxStream;
use nisshi_sans_io::IsolationLevel;
use nisshi_sans_io::create_topics_request::CreatableTopic;
use nisshi_sans_io::record::{Record, inflated};
use nisshi_storage::dynostore::DynoStore;
use nisshi_storage::{BrokerRegistrationRequest, Storage, Topition};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use url::Url;
use uuid::Uuid;

/// An object store that fails its `fail_on`-th put, then behaves normally.
#[derive(Debug)]
struct FlakyStore {
    inner: InMemory,
    puts: AtomicUsize,
    /// 0 = disarmed (setup); set after setup so only the produce path can fail.
    fail_on: AtomicUsize,
}

impl Display for FlakyStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FlakyStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FlakyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> OsResult<PutResult> {
        let fail_on = self.fail_on.load(Ordering::SeqCst);
        if fail_on == 0 {
            return self.inner.put_opts(location, payload, options).await;
        }
        let n = self.puts.fetch_add(1, Ordering::SeqCst) + 1;
        if n == fail_on {
            println!("  [store] put #{n} {location} -> INJECTED FAILURE");
            return Err(object_store::Error::Generic {
                store: "flaky",
                source: "injected object store failure".into(),
            });
        }
        println!("  [store] put #{n} {location}");
        self.inner.put_opts(location, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Lets the test keep a handle on the store that DynoStore owns.
#[derive(Debug)]
struct ArcStore(Arc<FlakyStore>);

impl Display for ArcStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArcStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for ArcStore {
    async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> OsResult<PutResult> {
        self.0.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(
        &self,
        l: &Path,
        o: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &Path, o: GetOptions) -> OsResult<GetResult> {
        self.0.get_opts(l, o).await
    }
    async fn get_ranges(&self, l: &Path, r: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        self.0.get_ranges(l, r).await
    }
    fn delete_stream(
        &self,
        l: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        self.0.delete_stream(l)
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.0.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> OsResult<ListResult> {
        self.0.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> OsResult<()> {
        self.0.copy_opts(f, t, o).await
    }
}

async fn run(fail_on: usize) -> bool {
    let flaky = Arc::new(FlakyStore {
        inner: InMemory::new(),
        puts: AtomicUsize::new(0),
        fail_on: AtomicUsize::new(0),
    });
    let store = DynoStore::new("repro", 111, ArcStore(flaky.clone()))
        .advertised_listener(Url::parse("tcp://localhost:9092").unwrap());

    store
        .register_broker(BrokerRegistrationRequest {
            broker_id: 111,
            cluster_id: "repro".into(),
            incarnation_id: Uuid::now_v7(),
            rack: None,
        })
        .await
        .expect("register_broker");
    let _ = store
        .create_topic(
            CreatableTopic::default()
                .name("t1".into())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await
        .expect("create_topic");
    let producer = store
        .init_producer(None, 10_000, Some(-1), Some(-1))
        .await
        .expect("init_producer");

    // Setup is done: arm the injection so only the produce path can fail.
    flaky.fail_on.store(fail_on, Ordering::SeqCst);

    let topition = Topition::new("t1".to_string(), 0);
    let batch = || {
        inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"w1").into()))
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)
            .expect("batch")
    };

    let first = store.produce(None, &topition, batch()).await;
    println!("  produce #1      -> {first:?}");
    let retry = store.produce(None, &topition, batch()).await;
    println!("  produce #2 (the client's retry of the same batch) -> {retry:?}");

    let copies = store
        .fetch(&topition, 0, 0, 1_048_576, IsolationLevel::ReadUncommitted, Duration::from_secs(5))
        .await
        .expect("fetch")
        .into_iter()
        .filter_map(|b| inflated::Batch::try_from(b).ok())
        .flat_map(|b| b.records)
        .filter(|r| r.value.as_ref().map(|v| v.as_ref() == b"w1").unwrap_or(false))
        .count();
    println!("  records in the log: {copies}");

    let lost = format!("{retry:?}").contains("DuplicateSequenceNumber") && copies == 0;
    if lost {
        println!("  => LOST WRITE: the client was told the batch was already stored, and it is not there");
    }
    lost
}

#[tokio::main]
async fn main() {
    let mut lost = vec![];
    for fail_on in 1..=8 {
        println!("--- object store fails put #{fail_on}");
        if run(fail_on).await {
            lost.push(fail_on);
        }
    }
    println!("\nlost writes at failure points: {lost:?}");
}
