use std::time::{SystemTime, UNIX_EPOCH};

use rdkafka::{
    ClientConfig, Message as _, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer as _, StreamConsumer},
};
use tokio::time::timeout;

use super::READ_TIMEOUT;

/// Matches the listener in `docker-compose.yml`.
pub const BROKERS: &str = "127.0.0.1:9092";

pub async fn create_topic(name: &str, partitions: i32) -> String {
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

/// Reads every partition from the beginning, without a consumer group rebalance.
pub fn consume(topic: &str, partitions: i32) -> StreamConsumer {
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

pub struct Record {
    pub partition: i32,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

pub async fn next_record(consumer: &StreamConsumer) -> Record {
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
