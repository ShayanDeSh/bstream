mod common;

use std::{
    collections::{HashMap, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};

use bstream_client::{Client, key_hash};
use common::{READ_TIMEOUT, encode, expect_frame, read_frame, send, start, subscribe};
use rdkafka::{
    ClientConfig, Message as _, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer as _, StreamConsumer},
};
use tokio::time::timeout;

/// Matches the listener in `docker-compose.yml`.
const BROKERS: &str = "127.0.0.1:9092";

async fn create_topic(name: &str, partitions: i32) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let topic = format!("bstream-{name}-{nanos}");
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .create()
        .unwrap();
    let results = admin
        .create_topics(
            &[NewTopic::new(
                &topic,
                partitions,
                TopicReplication::Fixed(1),
            )],
            &AdminOptions::new().operation_timeout(Some(READ_TIMEOUT)),
        )
        .await
        .unwrap();
    for result in results {
        result.unwrap();
    }
    topic
}

fn kafka_config(routes: &str) -> String {
    format!("routes = [{routes}]\n[kafka]\nbrokers = \"{BROKERS}\"")
}

/// Reads every partition from the beginning, without a consumer group rebalance.
fn consume(topic: &str, partitions: i32) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", "bstream-tests")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut assignment = TopicPartitionList::new();
    for partition in 0..partitions {
        assignment
            .add_partition_offset(topic, partition, Offset::Beginning)
            .unwrap();
    }
    consumer.assign(&assignment).unwrap();
    consumer
}

struct Record {
    partition: i32,
    key: Vec<u8>,
    payload: Vec<u8>,
}

async fn next_record(consumer: &StreamConsumer) -> Record {
    let message = timeout(READ_TIMEOUT, consumer.recv())
        .await
        .expect("no kafka record before timeout")
        .unwrap();
    Record {
        partition: message.partition(),
        key: message.key().unwrap_or_default().to_vec(),
        payload: message.payload().unwrap_or_default().to_vec(),
    }
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
