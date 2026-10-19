//! Stops and restarts the shared Redis container. It lives in its own test binary because cargo
//! runs test binaries one at a time, so no other Redis test sees the outage.

mod common;

use std::{collections::HashSet, ops::Range, path::Path, process, time::Duration};

use bstream::sinks::{BATCH_LEN, SinkKind};
use bstream_client::Client;
use bstream_common::frame::{FrameHeader, HEADER_LEN};
use common::{
    READ_TIMEOUT,
    kafka::{BROKERS, consume, create_topic, next_record},
    redis_streams::{URL, connect, unique_stream, xrange},
    start, subscribe, wait_until,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    process::Command,
    sync::mpsc,
    time::{sleep, timeout},
};

const QUEUE: u64 = 64;
const PAYLOAD_LEN: usize = 4096;
/// 32 MB in all, far more than the sink queues and both sockets' buffers can hold.
const N: u32 = 8000;

fn compose_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}

async fn compose(args: &[&str]) {
    let status = Command::new("docker")
        .arg("compose")
        .args(args)
        .current_dir(compose_dir())
        .status()
        .await
        .unwrap();
    assert!(status.success(), "docker compose {args:?} failed");
}

/// Brings Redis back even if the test panics while it is stopped.
struct RestartRedis;

impl Drop for RestartRedis {
    fn drop(&mut self) {
        let _ = process::Command::new("docker")
            .args(["compose", "up", "-d", "--wait", "redis"])
            .current_dir(compose_dir())
            .status();
    }
}

fn frames(seqs: Range<u32>) -> Vec<u8> {
    let mut client = Client::new(Vec::new());
    let mut payload = vec![0; PAYLOAD_LEN];
    for seq in seqs {
        payload[..4].copy_from_slice(&seq.to_le_bytes());
        client.publish(1, b"key", &payload).unwrap();
    }
    client.into_inner()
}

fn seq(payload: &[u8]) -> u32 {
    u32::from_le_bytes(payload[..4].try_into().unwrap())
}

/// Reports the sequence number of every frame the subscriber receives, with no read timeout,
/// since the router may legitimately go quiet while Redis is down.
fn read_seqs(mut subscriber: TcpStream) -> mpsc::UnboundedReceiver<u32> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut header = [0; HEADER_LEN];
        while subscriber.read_exact(&mut header).await.is_ok() {
            let header = FrameHeader::from_bytes(&header).unwrap();
            let mut payload = vec![0; header.payload_len as usize];
            subscriber.read_exact(&mut payload).await.unwrap();
            tx.send(seq(&payload)).unwrap();
        }
    });
    rx
}

async fn expect_seqs(rx: &mut mpsc::UnboundedReceiver<u32>, seqs: Range<u32>) {
    for expected in seqs {
        let got = timeout(READ_TIMEOUT, rx.recv()).await.unwrap();
        assert_eq!(got, Some(expected));
    }
}

/// Given sink_buffer = 64, route 1 writing to Kafka, Redis, and a local subscriber, and 100
/// frames already in Redis,
/// when Redis is stopped, 50 more frames are sent, then a producer sends 32 MB more, and Redis
/// is restarted,
/// then while Redis is down Kafka and the subscriber receive the 50 frames and the producer
/// stalls with a bounded Redis backlog, and after the restart Redis, Kafka, and the subscriber
/// each hold every frame in order.
#[tokio::test]
#[ignore = "needs Kafka and Redis: docker compose up -d --wait; stops and restarts Redis"]
async fn redis_outage_stalls_producer_and_loses_nothing() {
    let topic = create_topic("outage", 1).await;
    let stream = unique_stream("outage");
    let router = start(&format!(
        "sink_buffer = {QUEUE}\nsubscriber_buffer = {N}\n\
         routes = [{{ id = 1, kafka = {{ topic = \"{topic}\" }}, \
         redis = {{ stream = \"{stream}\" }} }}]\n\
         [kafka]\nbrokers = \"{BROKERS}\"\n[redis]\nurl = \"{URL}\""
    ))
    .await;
    let kafka = router.stats.sink(1, SinkKind::Kafka).unwrap();
    let redis = router.stats.sink(1, SinkKind::Redis).unwrap();
    let mut received = read_seqs(subscribe(router.subscribe, 1).await);
    let mut producer = TcpStream::connect(router.ingest).await.unwrap();

    producer.write_all(&frames(0..100)).await.unwrap();
    wait_until("100 frames in redis", || async { redis.delivered() == 100 }).await;

    let _restart = RestartRedis;
    compose(&["stop", "redis"]).await;
    producer.write_all(&frames(100..150)).await.unwrap();
    expect_seqs(&mut received, 0..150).await;
    wait_until("150 frames in kafka", || async { kafka.delivered() == 150 }).await;
    wait_until("a failed redis write", || async { redis.retries() > 0 }).await;

    let rest = frames(150..N);
    let writer = tokio::spawn(async move {
        producer.write_all(&rest).await.unwrap();
        producer
    });
    wait_until("the redis queue to fill and kafka to drain", || async {
        redis.pending() >= QUEUE && kafka.pending() == 0
    })
    .await;
    let stalled_at = kafka.delivered();
    sleep(Duration::from_secs(1)).await;
    assert!(!writer.is_finished(), "producer wrote all 32 MB");
    assert_eq!(kafka.delivered(), stalled_at, "router kept reading");
    assert!(stalled_at < u64::from(N));
    assert!(redis.pending() <= QUEUE + BATCH_LEN as u64);

    compose(&["up", "-d", "--wait", "redis"]).await;
    let _producer = timeout(Duration::from_secs(60), writer)
        .await
        .expect("producer still stalled after redis restarted")
        .unwrap();
    expect_seqs(&mut received, 150..N).await;
    wait_until("every frame in kafka", || async {
        kafka.delivered() == u64::from(N)
    })
    .await;
    wait_until("redis to drain", || async { redis.pending() == 0 }).await;
    assert_eq!((redis.failed(), kafka.failed()), (0, 0));

    // A batch cut off by the outage may be written twice, so compare first occurrences.
    let mut seen = HashSet::new();
    let in_redis: Vec<_> = xrange(&mut connect().await, &stream)
        .await
        .into_iter()
        .map(|(_, payload)| seq(&payload))
        .filter(|seq| seen.insert(*seq))
        .collect();
    assert_eq!(in_redis, (0..N).collect::<Vec<_>>());

    let consumer = consume(&topic, 1);
    for expected in 0..N {
        assert_eq!(seq(&next_record(&consumer).await.payload), expected);
    }
}
