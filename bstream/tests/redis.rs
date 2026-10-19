mod common;

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bstream::{
    Router,
    config::Config,
    sinks::{Sink as _, redis::RedisSink},
};
use bstream_client::{Client, key_hash};
use bstream_common::frame::FrameHeader;
use bytes::Bytes;
use common::{READ_TIMEOUT, encode, expect_frame, read_frame, send, start, subscribe};
use redis::{AsyncCommands as _, aio::MultiplexedConnection};
use tokio::{net::TcpListener, time::sleep};

/// Matches the port in `docker-compose.yml`.
const URL: &str = "redis://127.0.0.1:6379";

fn unique_stream(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("bstream-{name}-{nanos}")
}

fn redis_config(routes: &str) -> String {
    format!("routes = [{routes}]\n[redis]\nurl = \"{URL}\"")
}

async fn connect() -> MultiplexedConnection {
    redis::Client::open(URL)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

/// Polls until `done` returns true, failing the test after `READ_TIMEOUT`.
async fn wait_until<F: Future<Output = bool>>(what: &str, mut done: impl FnMut() -> F) {
    let deadline = Instant::now() + READ_TIMEOUT;
    while !done().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_len(conn: &MultiplexedConnection, stream: &str, len: usize) {
    wait_until(&format!("{len} entries in {stream}"), || async {
        let actual: usize = redis::cmd("XLEN")
            .arg(stream)
            .query_async(&mut conn.clone())
            .await
            .unwrap();
        actual >= len
    })
    .await;
}

/// An XRANGE entry: its id and its field/value pairs.
type StreamEntry = (String, Vec<(String, Vec<u8>)>);

/// Returns `(key_hash, payload)` for every entry, oldest first.
async fn xrange(conn: &mut MultiplexedConnection, stream: &str) -> Vec<(u32, Vec<u8>)> {
    let entries: Vec<StreamEntry> = redis::cmd("XRANGE")
        .arg(stream)
        .arg("-")
        .arg("+")
        .query_async(conn)
        .await
        .unwrap();
    entries
        .into_iter()
        .map(|(_, fields)| {
            let [(k, key_hash), (p, payload)] = <[_; 2]>::try_from(fields).unwrap();
            assert_eq!((k.as_str(), p.as_str()), ("key_hash", "payload"));
            let key_hash = String::from_utf8(key_hash).unwrap().parse().unwrap();
            (key_hash, payload)
        })
        .collect()
}

async fn wrongtype_errors(conn: &mut MultiplexedConnection) -> u64 {
    let info: String = redis::cmd("INFO")
        .arg("errorstats")
        .query_async(conn)
        .await
        .unwrap();
    info.lines()
        .find_map(|line| line.strip_prefix("errorstat_WRONGTYPE:count="))
        .map_or(0, |count| count.parse().unwrap())
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
/// then the first frame is dropped and the sink recovers to append the second.
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
    let errors_before = wrongtype_errors(&mut conn).await;

    send(router.ingest, &encode(&[(1, b"dropped")])).await;
    wait_until("the XADD to fail with WRONGTYPE", || {
        let mut conn = conn.clone();
        async move { wrongtype_errors(&mut conn).await > errors_before }
    })
    .await;
    let _: () = conn.del(&stream).await.unwrap();
    send(router.ingest, &encode(&[(1, b"delivered")])).await;

    wait_for_len(&conn, &stream, 1).await;
    let entries = xrange(&mut conn, &stream).await;
    let payloads: Vec<_> = entries.iter().map(|(_, payload)| &payload[..]).collect();
    assert_eq!(payloads, [b"delivered"]);
}

/// Given a Redis sink pointed at a listener that accepts connections but never replies,
/// when 2000 frames are published to it back to back,
/// then the first publishes succeed and later ones fail with a full-queue error instead of
/// blocking.
#[tokio::test]
async fn full_queue_drops_frames_without_blocking() {
    let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("redis://{}", silent.local_addr().unwrap());
    let sink = RedisSink::new(redis::Client::open(url).unwrap(), "s".into());
    let header = FrameHeader::new(1, 0, 1).unwrap();
    let frame = Bytes::from([&header.to_bytes()[..], b"x"].concat());

    let results: Vec<_> = (0..2000).map(|_| sink.publish(&header, &frame)).collect();

    assert!(results[0].is_ok());
    let err = results
        .iter()
        .find_map(|result| result.as_ref().err())
        .expect("every publish succeeded");
    assert!(err.to_string().contains("redis stream s"), "{err}");
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
