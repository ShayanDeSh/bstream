mod common;

use std::time::Duration;

use bstream::{
    Router,
    config::Config,
    sinks::{BATCH_LEN, Sink as _, SinkKind, redis::RedisSink},
};
use bstream_client::{Client, key_hash};
use bstream_common::frame::FrameHeader;
use bytes::Bytes;
use common::{
    encode, expect_frame, read_frame,
    redis_streams::{URL, connect, unique_stream, wait_for_len, xrange},
    send, start, subscribe, wait_until,
};
use redis::AsyncCommands as _;
use tokio::{
    io::AsyncWriteExt as _,
    net::{TcpListener, TcpStream},
    time::sleep,
};

fn redis_config(routes: &str) -> String {
    format!("routes = [{routes}]\n[redis]\nurl = \"{URL}\"")
}

fn frame(payload: &[u8]) -> (FrameHeader, Bytes) {
    let header = FrameHeader::new(1, key_hash(b"key"), payload.len()).unwrap();
    (
        header,
        Bytes::from([&header.to_bytes()[..], payload].concat()),
    )
}

/// Given route 1 writing to a Redis stream and a local subscriber on route 1,
/// when a producer publishes 1000 frames to route 1 spread over 10 keys,
/// then XRANGE returns exactly those 1000 entries in publish order with their key_hash and
/// payload, and the local subscriber also receives all 1000.
#[tokio::test]
#[ignore = "needs Redis: docker compose up -d --wait"]
async fn frames_reach_redis_stream_in_order() {
    const N: u32 = 1000;
    const KEYS: u32 = 10;
    let stream = unique_stream("ordered");
    let router = start(&redis_config(&format!(
        "{{ id = 1, redis = {{ stream = \"{stream}\" }} }}"
    )))
    .await;
    let mut subscriber = subscribe(router.subscribe, 1).await;

    let mut client = Client::new(Vec::new());
    for i in 0..N {
        let key = format!("key-{}", i % KEYS);
        client.publish(1, key.as_bytes(), &i.to_le_bytes()).unwrap();
    }
    send(router.ingest, &client.into_inner()).await;

    let mut conn = connect().await;
    wait_for_len(&conn, &stream, N as usize).await;
    let expected: Vec<_> = (0..N)
        .map(|i| {
            let hash = key_hash(format!("key-{}", i % KEYS).as_bytes());
            (hash, i.to_le_bytes().to_vec())
        })
        .collect();
    assert_eq!(xrange(&mut conn, &stream).await, expected);

    for i in 0..N {
        let (_, payload) = read_frame(&mut subscriber)
            .await
            .expect("subscription closed");
        assert_eq!(payload, i.to_le_bytes());
    }
}

/// Given route 1 writing to a Redis stream and route 2 with no stream,
/// when a producer publishes frames to route 2 and then one frame to route 1,
/// then the stream holds only the route 1 frame.
#[tokio::test]
#[ignore = "needs Redis: docker compose up -d --wait"]
async fn route_without_stream_is_not_written() {
    let stream = unique_stream("unrouted");
    let router = start(&redis_config(&format!(
        "{{ id = 1, redis = {{ stream = \"{stream}\" }} }}, {{ id = 2 }}"
    )))
    .await;

    send(
        router.ingest,
        &encode(&[(2, b"local only"), (2, b"local only"), (1, b"to redis")]),
    )
    .await;

    let mut conn = connect().await;
    wait_for_len(&conn, &stream, 1).await;
    let entries = xrange(&mut conn, &stream).await;
    let payloads: Vec<_> = entries.iter().map(|(_, payload)| &payload[..]).collect();
    assert_eq!(payloads, [b"to redis"]);
}

/// Given route 1's stream key already holding a plain string, so XADD fails with WRONGTYPE,
/// when a frame is published, the key is then deleted, and a second frame is published,
/// then the first frame counts as failed without a retry and the sink recovers to append the
/// second.
#[tokio::test]
#[ignore = "needs Redis: docker compose up -d --wait"]
async fn failed_write_does_not_stop_later_frames() {
    let stream = unique_stream("wrongtype");
    let mut conn = connect().await;
    let _: () = conn.set(&stream, "not a stream").await.unwrap();
    let router = start(&redis_config(&format!(
        "{{ id = 1, redis = {{ stream = \"{stream}\" }} }}"
    )))
    .await;
    let redis = router.stats.sink(1, SinkKind::Redis).unwrap();

    send(router.ingest, &encode(&[(1, b"dropped")])).await;
    wait_until("the XADD to fail", || async { redis.failed() == 1 }).await;
    let _: () = conn.del(&stream).await.unwrap();
    send(router.ingest, &encode(&[(1, b"delivered")])).await;

    wait_for_len(&conn, &stream, 1).await;
    let entries = xrange(&mut conn, &stream).await;
    let payloads: Vec<_> = entries.iter().map(|(_, payload)| &payload[..]).collect();
    assert_eq!(payloads, [b"delivered"]);
    assert_eq!(redis.retries(), 0);
}

/// Given a stream key holding a plain string,
/// when a Redis sink writes a batch of two frames to it,
/// then the write returns with both frames rejected, since retrying cannot fix WRONGTYPE.
#[tokio::test]
#[ignore = "needs Redis: docker compose up -d --wait"]
async fn wrongtype_rejects_batch() {
    let stream = unique_stream("wrongtype-sink");
    let mut conn = connect().await;
    let _: () = conn.set(&stream, "not a stream").await.unwrap();
    let mut sink = RedisSink::new(redis::Client::open(URL).unwrap(), stream);

    let rejected = sink.write(&[frame(b"a"), frame(b"b")]).await.unwrap();

    assert_eq!(rejected, 2);
}

/// Given a Redis sink pointed at an address where nothing listens,
/// when it writes a frame,
/// then the write fails with an error naming the stream, so the queue retries it.
#[tokio::test]
async fn unreachable_redis_write_fails_for_retry() {
    let client = redis::Client::open("redis://127.0.0.1:1").unwrap();
    let mut sink = RedisSink::new(client, "s".into());

    let err = sink.write(&[frame(b"a")]).await.unwrap_err();

    assert!(err.to_string().contains("redis stream s"), "{err}");
}

/// Given sink_buffer = 4 and route 1 writing to a Redis server that accepts connections but
/// never replies,
/// when a producer sends 32 MB of frames to route 1,
/// then the producer's write stalls, and the Redis sink holds a bounded number of frames that
/// stops growing, instead of the router reading everything into memory.
#[tokio::test]
async fn full_sink_queue_stalls_producer() {
    const QUEUE: u64 = 4;
    let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let router = start(&format!(
        "sink_buffer = {QUEUE}\nroutes = [{{ id = 1, redis = {{ stream = \"s\" }} }}]\n\
         [redis]\nurl = \"redis://{}\"",
        silent.local_addr().unwrap()
    ))
    .await;
    let redis = router.stats.sink(1, SinkKind::Redis).unwrap();
    let mut client = Client::new(Vec::new());
    for _ in 0..8 * 1024 {
        client.publish(1, b"key", &[0; 4096]).unwrap();
    }
    let bytes = client.into_inner();
    let mut producer = TcpStream::connect(router.ingest).await.unwrap();
    let writer = tokio::spawn(async move { producer.write_all(&bytes).await });

    wait_until("the redis queue to fill", || async {
        redis.pending() >= QUEUE
    })
    .await;
    let pending = redis.pending();
    sleep(Duration::from_secs(1)).await;

    assert!(!writer.is_finished(), "producer wrote all 32 MB");
    assert_eq!(redis.pending(), pending, "router kept reading");
    assert!(
        pending <= QUEUE + BATCH_LEN as u64,
        "{pending} frames pending"
    );
    assert_eq!(redis.delivered(), 0);
}

/// Given a config whose Redis URL is not a valid redis:// URL,
/// when the router is bound,
/// then binding fails instead of starting a router that can never write to Redis.
#[tokio::test]
async fn malformed_redis_url_fails_bind() {
    let config = Config::from_toml(
        "ingest = \"127.0.0.1:0\"\nsubscribe = \"127.0.0.1:0\"\nroutes = []\n[redis]\nurl = \"not a url\"",
    )
    .unwrap();

    assert!(Router::bind(&config).await.is_err());
}

/// Given route 1 writing to Redis at an address where nothing listens, and a local subscriber,
/// when a producer publishes frames to route 1,
/// then the subscriber still receives every frame and none are counted as unknown.
#[tokio::test]
async fn unreachable_redis_does_not_block_local_delivery() {
    let router = start(
        "routes = [{ id = 1, redis = { stream = \"s\" } }]\n[redis]\nurl = \"redis://127.0.0.1:1\"",
    )
    .await;
    let mut subscriber = subscribe(router.subscribe, 1).await;

    send(router.ingest, &encode(&[(1, b"a"), (1, b"b")])).await;

    expect_frame(&mut subscriber, 1, b"a").await;
    expect_frame(&mut subscriber, 1, b"b").await;
    assert_eq!(router.stats.unknown_route(), 0);
}
