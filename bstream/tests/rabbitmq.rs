mod common;

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bstream::{
    Router,
    config::Config,
    sinks::{Sink as _, rabbitmq::RabbitMqSink},
};
use bstream_client::{Client, key_hash};
use bstream_common::frame::FrameHeader;
use bytes::Bytes;
use common::{READ_TIMEOUT, encode, expect_frame, read_frame, send, start, subscribe, wait_until};
use lapin::{
    Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{BasicGetOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable, ShortString},
};
use tokio::{net::TcpListener, time::sleep};

/// Matches the port in `docker-compose.yml`; the image lets `guest` log in from other hosts.
const URL: &str = "amqp://guest:guest@127.0.0.1:5672/%2f";

fn unique(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("bstream-{name}-{nanos}")
}

fn rabbitmq_config(routes: &str) -> String {
    format!("routes = [{routes}]\n[rabbitmq]\nurl = \"{URL}\"")
}

/// The connection must outlive the channel and any exclusive queue declared on it.
async fn connect() -> (Connection, Channel) {
    let connection = Connection::connect(URL, ConnectionProperties::default())
        .await
        .unwrap();
    let channel = connection.create_channel().await.unwrap();
    (connection, channel)
}

/// Declares a queue that is deleted when the test's connection closes.
async fn declare_queue(channel: &Channel, name: &str, arguments: FieldTable) {
    let options = QueueDeclareOptions {
        exclusive: true,
        ..Default::default()
    };
    channel
        .queue_declare(name.into(), options, arguments)
        .await
        .unwrap();
}

async fn message_count(channel: &Channel, queue: &str) -> u32 {
    let options = QueueDeclareOptions {
        passive: true,
        ..Default::default()
    };
    channel
        .queue_declare(queue.into(), options, FieldTable::default())
        .await
        .unwrap()
        .message_count()
}

/// Pulls `n` messages and returns `(key_hash header, body)` for each, in queue order.
async fn get_messages(channel: &Channel, queue: &str, n: usize) -> Vec<(u32, Vec<u8>)> {
    let deadline = Instant::now() + READ_TIMEOUT;
    let mut messages = Vec::with_capacity(n);
    while messages.len() < n {
        let options = BasicGetOptions { no_ack: true };
        let Some(message) = channel.basic_get(queue.into(), options).await.unwrap() else {
            assert!(
                Instant::now() < deadline,
                "got {} of {n} messages",
                messages.len()
            );
            sleep(Duration::from_millis(20)).await;
            continue;
        };
        assert_eq!(*message.properties.delivery_mode(), Some(2), "persistent");
        let headers = message.properties.headers().as_ref().expect("headers");
        let key_hash = match headers.inner().get(&ShortString::from("key_hash")) {
            Some(AMQPValue::LongUInt(key_hash)) => *key_hash,
            other => panic!("key_hash header: {other:?}"),
        };
        messages.push((key_hash, message.delivery.data));
    }
    messages
}

fn frame(payload: &[u8]) -> (FrameHeader, Bytes) {
    let header = FrameHeader::new(1, key_hash(b"key"), payload.len()).unwrap();
    (
        header,
        Bytes::from([&header.to_bytes()[..], payload].concat()),
    )
}

async fn wait_for_outcomes(sink: &RabbitMqSink, n: u64) {
    wait_until(&format!("{n} confirm outcomes"), || async {
        sink.delivered() + sink.failed() >= n
    })
    .await;
}

/// Given a direct exchange with a queue bound under key `k`, route 1 publishing to that exchange
/// with key `k`, and a local subscriber on route 1,
/// when a producer publishes 1000 frames to route 1 spread over 10 keys,
/// then the bound queue holds exactly those 1000 persistent messages in publish order, each with
/// its key_hash header, and the local subscriber also receives all 1000.
#[tokio::test]
#[ignore = "needs RabbitMQ: docker compose up -d --wait"]
async fn frames_reach_bound_queue_in_order() {
    const N: u32 = 1000;
    const KEYS: u32 = 10;
    let (_connection, channel) = connect().await;
    let exchange = unique("exchange");
    let queue = unique("bound");
    let options = ExchangeDeclareOptions {
        auto_delete: true,
        ..Default::default()
    };
    channel
        .exchange_declare(
            exchange.as_str().into(),
            ExchangeKind::Direct,
            options,
            FieldTable::default(),
        )
        .await
        .unwrap();
    declare_queue(&channel, &queue, FieldTable::default()).await;
    channel
        .queue_bind(
            queue.as_str().into(),
            exchange.as_str().into(),
            "k".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let router = start(&rabbitmq_config(&format!(
        "{{ id = 1, rabbitmq = {{ exchange = \"{exchange}\", routing_key = \"k\" }} }}"
    )))
    .await;
    let mut subscriber = subscribe(router.subscribe, 1).await;

    let mut client = Client::new(Vec::new());
    for i in 0..N {
        let key = format!("key-{}", i % KEYS);
        client.publish(1, key.as_bytes(), &i.to_le_bytes()).unwrap();
    }
    send(router.ingest, &client.into_inner()).await;

    let expected: Vec<_> = (0..N)
        .map(|i| {
            let hash = key_hash(format!("key-{}", i % KEYS).as_bytes());
            (hash, i.to_le_bytes().to_vec())
        })
        .collect();
    assert_eq!(get_messages(&channel, &queue, N as usize).await, expected);
    assert_eq!(message_count(&channel, &queue).await, 0);

    for i in 0..N {
        let (_, payload) = read_frame(&mut subscriber)
            .await
            .expect("subscription closed");
        assert_eq!(payload, i.to_le_bytes());
    }
}

/// Given route 1 publishing to a queue through the default exchange and route 2 with no
/// RabbitMQ target,
/// when a producer publishes frames to route 2 and then one frame to route 1,
/// then the first message in the queue is the route 1 frame.
#[tokio::test]
#[ignore = "needs RabbitMQ: docker compose up -d --wait"]
async fn route_without_rabbitmq_is_not_published() {
    let (_connection, channel) = connect().await;
    let queue = unique("unrouted");
    declare_queue(&channel, &queue, FieldTable::default()).await;
    let router = start(&rabbitmq_config(&format!(
        "{{ id = 1, rabbitmq = {{ routing_key = \"{queue}\" }} }}, {{ id = 2 }}"
    )))
    .await;

    send(
        router.ingest,
        &encode(&[(2, b"local only"), (2, b"local only"), (1, b"to rabbitmq")]),
    )
    .await;

    let messages = get_messages(&channel, &queue, 1).await;
    assert_eq!(messages[0].1, b"to rabbitmq");
}

/// Given a sink publishing to an existing queue through the default exchange,
/// when three frames are published,
/// then all three are counted as delivered once confirmed, none as failed, and the queue holds
/// all three.
#[tokio::test]
#[ignore = "needs RabbitMQ: docker compose up -d --wait"]
async fn confirmed_frames_count_as_delivered() {
    let (_connection, channel) = connect().await;
    let queue = unique("confirmed");
    declare_queue(&channel, &queue, FieldTable::default()).await;
    let sink = RabbitMqSink::new(URL.parse().unwrap(), String::new(), queue.clone());

    for payload in [b"a", b"b", b"c"] {
        let (header, frame) = frame(payload);
        sink.publish(&header, &frame).unwrap();
    }

    wait_for_outcomes(&sink, 3).await;
    assert_eq!((sink.delivered(), sink.failed()), (3, 0));
    assert_eq!(message_count(&channel, &queue).await, 3);
}

/// Given a queue limited to one message that rejects publishes when full,
/// when a sink publishes three frames to it,
/// then the broker nacks two, so one frame counts as delivered and two as failed.
#[tokio::test]
#[ignore = "needs RabbitMQ: docker compose up -d --wait"]
async fn nacked_frames_count_as_failed() {
    let (_connection, channel) = connect().await;
    let queue = unique("full");
    let mut arguments = FieldTable::default();
    arguments.insert("x-max-length".into(), AMQPValue::LongInt(1));
    arguments.insert(
        "x-overflow".into(),
        AMQPValue::LongString("reject-publish".into()),
    );
    declare_queue(&channel, &queue, arguments).await;
    let sink = RabbitMqSink::new(URL.parse().unwrap(), String::new(), queue.clone());

    for payload in [b"a", b"b", b"c"] {
        let (header, frame) = frame(payload);
        sink.publish(&header, &frame).unwrap();
    }

    wait_for_outcomes(&sink, 3).await;
    assert_eq!((sink.delivered(), sink.failed()), (1, 2));
    assert_eq!(get_messages(&channel, &queue, 1).await[0].1, b"a");
}

/// Given a sink whose routing key matches no queue on the default exchange,
/// when two frames are published,
/// then the broker returns them as unroutable and both count as failed.
#[tokio::test]
#[ignore = "needs RabbitMQ: docker compose up -d --wait"]
async fn unroutable_frames_count_as_failed() {
    let sink = RabbitMqSink::new(URL.parse().unwrap(), String::new(), unique("no-such-queue"));

    for payload in [b"a", b"b"] {
        let (header, frame) = frame(payload);
        sink.publish(&header, &frame).unwrap();
    }

    wait_for_outcomes(&sink, 2).await;
    assert_eq!((sink.delivered(), sink.failed()), (0, 2));
}

/// Given a sink pointed at an address where nothing listens,
/// when two frames are published,
/// then both count as failed.
#[tokio::test]
async fn unreachable_broker_frames_count_as_failed() {
    let uri = "amqp://guest:guest@127.0.0.1:1/%2f".parse().unwrap();
    let sink = RabbitMqSink::new(uri, String::new(), "q".into());

    for payload in [b"a", b"b"] {
        let (header, frame) = frame(payload);
        sink.publish(&header, &frame).unwrap();
    }

    wait_for_outcomes(&sink, 2).await;
    assert_eq!((sink.delivered(), sink.failed()), (0, 2));
}

/// Given a sink pointed at a listener that accepts connections but never replies,
/// when 2000 frames are published to it back to back,
/// then the first publishes succeed and later ones fail with a full-queue error instead of
/// blocking.
#[tokio::test]
async fn full_queue_drops_frames_without_blocking() {
    let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("amqp://{}/%2f", silent.local_addr().unwrap())
        .parse()
        .unwrap();
    let sink = RabbitMqSink::new(uri, String::new(), "q".into());
    let (header, frame) = frame(b"x");

    let results: Vec<_> = (0..2000).map(|_| sink.publish(&header, &frame)).collect();

    assert!(results[0].is_ok());
    let err = results
        .iter()
        .find_map(|result| result.as_ref().err())
        .expect("every publish succeeded");
    assert!(err.to_string().contains("rabbitmq"), "{err}");
}

/// Given route 1 publishing to RabbitMQ at an address where nothing listens, and a local
/// subscriber,
/// when a producer publishes frames to route 1,
/// then the subscriber still receives every frame and none are counted as unknown.
#[tokio::test]
async fn unreachable_rabbitmq_does_not_block_local_delivery() {
    let router = start(
        "routes = [{ id = 1, rabbitmq = { routing_key = \"q\" } }]\n\
         [rabbitmq]\nurl = \"amqp://127.0.0.1:1/%2f\"",
    )
    .await;
    let mut subscriber = subscribe(router.subscribe, 1).await;

    send(router.ingest, &encode(&[(1, b"a"), (1, b"b")])).await;

    expect_frame(&mut subscriber, 1, b"a").await;
    expect_frame(&mut subscriber, 1, b"b").await;
    assert_eq!(router.stats.unknown_route(), 0);
}

/// Given a config whose RabbitMQ URL is not an amqp:// URL,
/// when the router is bound,
/// then binding fails with an error naming the RabbitMQ URL.
#[tokio::test]
async fn malformed_rabbitmq_url_fails_bind() {
    let config = Config::from_toml(
        "ingest = \"127.0.0.1:0\"\nsubscribe = \"127.0.0.1:0\"\nroutes = []\n\
         [rabbitmq]\nurl = \"http://example.com\"",
    )
    .unwrap();

    let err = Router::bind(&config).await.err().expect("bind succeeded");

    assert!(err.to_string().contains("rabbitmq url"), "{err}");
}
