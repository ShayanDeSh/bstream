mod common;

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use bstream::sinks::{
    Sink as _,
    kafka::{self, KafkaSink},
};
use bstream_client::{Client, key_hash};
use bstream_common::frame::{FrameHeader, MAX_PAYLOAD_LEN};
use bytes::Bytes;
use common::{
    encode, expect_frame,
    kafka::{BROKERS, consume, create_topic, next_record},
    read_frame, send, start, subscribe,
};
use tokio::time::timeout;

fn kafka_config(routes: &str) -> String {
    format!("routes = [{routes}]\n[kafka]\nbrokers = \"{BROKERS}\"")
}

fn frame(payload: &[u8]) -> (FrameHeader, Bytes) {
    let header = FrameHeader::new(1, key_hash(b"key"), payload.len()).unwrap();
    (
        header,
        Bytes::from([&header.to_bytes()[..], payload].concat()),
    )
}

/// Given a 3-partition topic, route 1 sending to it, and a local subscriber on route 1,
/// when a producer publishes 1000 frames to route 1 spread over 10 keys,
/// then the topic holds exactly those 1000 payloads, each keyed by its key_hash, with every key
/// on a single partition in publish order, and the local subscriber also receives all 1000.
#[tokio::test]
#[ignore = "needs Kafka: docker compose up -d --wait"]
async fn frames_reach_kafka_keyed_by_key_hash() {
    const N: u32 = 1000;
    const KEYS: u32 = 10;
    const PARTITIONS: i32 = 3;
    let topic = create_topic("keyed", PARTITIONS).await;
    let router = start(&kafka_config(&format!(
        "{{ id = 1, kafka = {{ topic = \"{topic}\" }} }}"
    )))
    .await;
    let mut subscriber = subscribe(router.subscribe, 1).await;

    let mut client = Client::new(Vec::new());
    for i in 0..N {
        let key = format!("key-{}", i % KEYS);
        client.publish(1, key.as_bytes(), &i.to_le_bytes()).unwrap();
    }
    send(router.ingest, &client.into_inner()).await;

    let consumer = consume(&topic, PARTITIONS);
    let mut partition_of_key = HashMap::new();
    let mut next_seq_of_key = HashMap::new();
    for _ in 0..N {
        let record = next_record(&consumer).await;
        let seq = u32::from_le_bytes(record.payload.try_into().expect("4-byte payload"));
        let hash = key_hash(format!("key-{}", seq % KEYS).as_bytes());
        assert_eq!(record.key, hash.to_le_bytes(), "record {seq} key");
        let partition = *partition_of_key.entry(hash).or_insert(record.partition);
        assert_eq!(record.partition, partition, "key of record {seq} split");
        let next_seq = next_seq_of_key.entry(hash).or_insert(seq % KEYS);
        assert_eq!(seq, *next_seq, "record out of order for its key");
        *next_seq += KEYS;
    }
    let partitions_used: HashSet<_> = partition_of_key.values().collect();
    assert!(
        partitions_used.len() > 1,
        "all keys landed on one partition"
    );

    for i in 0..N {
        let (_, payload) = read_frame(&mut subscriber)
            .await
            .expect("subscription closed");
        assert_eq!(payload, i.to_le_bytes());
    }
}

/// Given route 1 sending to a single-partition topic and route 2 with no Kafka topic,
/// when a producer publishes frames to route 2 and then one frame to route 1, all with one key,
/// then the first record in the topic is the route 1 frame, so no route 2 frame was produced.
#[tokio::test]
#[ignore = "needs Kafka: docker compose up -d --wait"]
async fn route_without_topic_is_not_produced() {
    let topic = create_topic("unrouted", 1).await;
    let router = start(&kafka_config(&format!(
        "{{ id = 1, kafka = {{ topic = \"{topic}\" }} }}, {{ id = 2 }}"
    )))
    .await;

    send(
        router.ingest,
        &encode(&[(2, b"local only"), (2, b"local only"), (1, b"to kafka")]),
    )
    .await;

    let record = next_record(&consume(&topic, 1)).await;
    assert_eq!(record.payload, b"to kafka");
}

/// Given route 1 sending to Kafka at an address where no broker listens, and a local subscriber,
/// when a producer publishes frames to route 1,
/// then the subscriber still receives every frame and none are counted as unknown.
#[tokio::test]
async fn unreachable_kafka_does_not_block_local_delivery() {
    let router = start(
        "routes = [{ id = 1, kafka = { topic = \"t\" } }]\n[kafka]\nbrokers = \"127.0.0.1:1\"",
    )
    .await;
    let mut subscriber = subscribe(router.subscribe, 1).await;

    send(router.ingest, &encode(&[(1, b"a"), (1, b"b")])).await;

    expect_frame(&mut subscriber, 1, b"a").await;
    expect_frame(&mut subscriber, 1, b"b").await;
    assert_eq!(router.stats.unknown_route(), 0);
}

/// Given a Kafka sink pointed at an address where no broker listens,
/// when it writes one frame,
/// then the write is still waiting two seconds later instead of rejecting the frame, since
/// librdkafka retries it without a deadline.
#[tokio::test]
async fn unreachable_broker_write_keeps_waiting() {
    let mut sink = KafkaSink::new(kafka::producer("127.0.0.1:1").unwrap(), "t".into());

    let batch = [frame(b"a")];

    assert!(
        timeout(Duration::from_secs(2), sink.write(&batch))
            .await
            .is_err(),
        "write returned while the broker was unreachable"
    );
}

/// Given a Kafka sink, whose broker need not be reachable,
/// when it writes one frame whose payload exceeds librdkafka's default `message.max.bytes`,
/// then the write returns at once with that frame rejected, not retried.
#[tokio::test]
async fn oversized_record_is_rejected() {
    let mut sink = KafkaSink::new(kafka::producer("127.0.0.1:1").unwrap(), "t".into());

    let rejected = timeout(
        Duration::from_secs(2),
        sink.write(&[frame(&vec![0; MAX_PAYLOAD_LEN])]),
    )
    .await
    .expect("write did not return")
    .unwrap();

    assert_eq!(rejected, 1);
}
