//! Produces frames to Kafka. The record key is `key_hash` as 4 little-endian bytes, so Kafka
//! keeps every frame of a key on one partition, in order. The record value is the payload alone.
//!
//! librdkafka retries transient broker errors itself, without a deadline. A batch completes once
//! every record is acked or fails for good, such as a record over the broker's size limit.

use anyhow::anyhow;
use bstream_common::frame::HEADER_LEN;
use log::warn;
use rdkafka::{
    ClientConfig,
    error::{KafkaError, KafkaResult, RDKafkaErrorCode},
    producer::{FutureProducer, FutureRecord},
};

use crate::sinks::{Frame, Sink};

pub type Producer = FutureProducer;

pub fn producer(brokers: &str) -> KafkaResult<Producer> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        // Retries cannot reorder or duplicate records within a partition.
        .set("enable.idempotence", "true")
        // Never give up on a record, so a broker outage applies backpressure instead of dropping.
        .set("message.timeout.ms", "0")
        .create()
}

pub struct KafkaSink {
    producer: Producer,
    topic: String,
}

impl KafkaSink {
    pub fn new(producer: Producer, topic: String) -> Self {
        Self { producer, topic }
    }
}

impl Sink for KafkaSink {
    async fn write(&mut self, batch: &[Frame]) -> anyhow::Result<usize> {
        let mut rejected = 0;
        let mut deliveries = Vec::with_capacity(batch.len());
        for (header, frame) in batch {
            let key = header.key_hash.to_le_bytes();
            let record = FutureRecord::to(&self.topic)
                .key(&key)
                .payload(&frame[HEADER_LEN..]);
            match self.producer.send_result(record) {
                Ok(delivery) => deliveries.push(delivery),
                Err((e @ KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                    return Err(anyhow!("kafka topic {}: {e}", self.topic));
                }
                Err((e, _)) => {
                    warn!("kafka topic {}: {e}", self.topic);
                    rejected += 1;
                }
            }
        }
        for delivery in deliveries {
            match delivery.await {
                Ok(Ok(_)) => {}
                Ok(Err((e, _))) => {
                    warn!("kafka topic {}: {e}", self.topic);
                    rejected += 1;
                }
                Err(_) => rejected += 1,
            }
        }
        Ok(rejected)
    }
}
