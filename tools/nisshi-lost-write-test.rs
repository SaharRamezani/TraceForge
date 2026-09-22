// Reproducer for a lost idempotent-producer write in the dynostore engine.
// Drop into nisshi-storage/src/dynostore/tests.rs (no other change needed):
//   cargo test -p nisshi-storage --no-default-features --features dynostore lost_write
// The test FAILS on current main: the retry is answered DuplicateSequenceNumber
// while the record was never written.
mod lost_write {
    use super::super::DynoStore;
    use crate::{BrokerRegistrationRequest, Result, Storage, Topition};
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use nisshi_sans_io::{
        IsolationLevel,
        create_topics_request::CreatableTopic,
        record::{Record, inflated},
    };
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
    };
    use std::fmt::{Display, Formatter};
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use url::Url;
    use uuid::Uuid;

    /// Which put (1-based, counted after arming) fails. 0 = nothing fails.
    static FAIL_ON: AtomicUsize = AtomicUsize::new(0);
    static PUTS: AtomicUsize = AtomicUsize::new(0);

    /// An in-memory object store whose FAIL_ON-th put fails, the way S3 does
    /// under a transient error, or the way a broker does when it dies there.
    #[derive(Debug)]
    struct Flaky(InMemory);

    impl Display for Flaky {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "Flaky")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for Flaky {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            let fail_on = FAIL_ON.load(Ordering::SeqCst);
            if fail_on != 0 && PUTS.fetch_add(1, Ordering::SeqCst) + 1 == fail_on {
                return Err(object_store::Error::Generic {
                    store: "flaky",
                    source: format!("injected failure writing {location}").into(),
                });
            }
            self.0.put_opts(location, payload, options).await
        }
        async fn put_multipart_opts(
            &self,
            l: &Path,
            o: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.0.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
            self.0.get_opts(l, o).await
        }
        async fn get_ranges(&self, l: &Path, r: &[Range<u64>]) -> object_store::Result<Vec<Bytes>> {
            self.0.get_ranges(l, r).await
        }
        fn delete_stream(
            &self,
            l: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.0.delete_stream(l)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.0.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
            self.0.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
            self.0.copy_opts(f, t, o).await
        }
    }

    /// One idempotent write whose `fail_on`-th store write fails, then the
    /// client's retry of the same batch. Returns (retry outcome, copies in log).
    async fn write_then_retry(fail_on: usize) -> Result<(String, usize)> {
        FAIL_ON.store(0, Ordering::SeqCst);
        PUTS.store(0, Ordering::SeqCst);
        let store = DynoStore::new("lost-write", 111, Flaky(InMemory::new()))
            .advertised_listener(Url::parse("tcp://localhost:9092")?);
        store
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: "lost-write".into(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;
        let _ = store
            .create_topic(
                CreatableTopic::default()
                    .name("t".into())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;
        let producer = store.init_producer(None, 10_000, Some(-1), Some(-1)).await?;
        let topition = Topition::new("t".to_string(), 0);
        let batch = || {
            inflated::Batch::builder()
                .record(Record::builder().value(Bytes::from_static(b"w1").into()))
                .producer_id(producer.id)
                .producer_epoch(producer.epoch)
                .base_sequence(0)
                .build()
                .and_then(TryInto::try_into)
        };

        // Setup done: from here on, the fail_on-th store write fails.
        FAIL_ON.store(fail_on, Ordering::SeqCst);
        let first = store.produce(None, &topition, batch()?).await;
        FAIL_ON.store(0, Ordering::SeqCst);
        println!("store write #{fail_on} fails -> first produce answered {first:?}");

        // The client retries the same batch: same producer id, epoch, base sequence.
        let retry = store.produce(None, &topition, batch()?).await;

        let copies = store
            .fetch(&topition, 0, 0, 1_048_576, IsolationLevel::ReadUncommitted, Duration::from_secs(5))
            .await?
            .into_iter()
            .filter_map(|b| inflated::Batch::try_from(b).ok())
            .flat_map(|b| b.records)
            .filter(|r| r.value.as_ref().map(|v| v.as_ref() == b"w1").unwrap_or(false))
            .count();
        Ok((format!("{retry:?}"), copies))
    }

    /// An idempotent produce is three store writes: the producer's sequence
    /// number in meta.json, the partition watermark, the record object. If a
    /// write AFTER the sequence advance fails, the sequence is already
    /// committed, so the client's retry is told DuplicateSequenceNumber
    /// ("already stored") and the record is never written. Kafka clients
    /// complete the batch as a success on that error, so the loss is silent.
    /// Every failure point is reported, then the test fails if any lost a write.
    #[tokio::test]
    async fn retry_told_duplicate_must_mean_the_record_is_stored() -> Result<()> {
        let mut lost = vec![];
        for fail_on in 1..=4 {
            let (retry, copies) = write_then_retry(fail_on).await?;
            println!("store write #{fail_on} fails -> retry answered {retry}, records in log: {copies}");
            if retry.contains("DuplicateSequenceNumber") && copies == 0 {
                lost.push(fail_on);
            }
        }
        assert!(lost.is_empty(), "lost writes when store write #{lost:?} fails");
        Ok(())
    }
}
