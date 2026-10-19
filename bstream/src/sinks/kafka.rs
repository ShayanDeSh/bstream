//! Produces frames to Kafka. The record key is `key_hash` as 4 little-endian bytes, so Kafka
//! keeps every frame of a key on one partition, in order. The record value is the payload alone.

use std::sync::Arc;

use anyhow::anyhow;
use bstream_common::frame::{FrameHeader, HEADER_LEN};
use bytes::Bytes;
use log::warn;
use rdkafka::{
    ClientConfig, ClientContext,
    error::KafkaResult,
    producer::{BaseRecord, DeliveryResult, ProducerContext, ThreadedProducer},
};

use crate::sinks::Sink;

pub type Producer = Arc<ThreadedProducer<LogFailures>>;

pub fn producer(brokers: &str) -> KafkaResult<Producer> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        // Retries cannot reorder or duplicate records within a partition.
        .set("enable.idempotence", "true")
        .create_with_context(LogFailures)
        .map(Arc::new)
}

pub struct LogFailures;

impl ClientContext for LogFailures {}

impl ProducerContext for LogFailures {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        if let Err((e, _)) = result {
            warn!("kafka delivery failed: {e}");
        }
    }
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
    fn publish(&self, header: &FrameHeader, frame: &Bytes) -> anyhow::Result<()> {
        let key = header.key_hash.to_le_bytes();
        let record = BaseRecord::to(&self.topic)
            .key(&key)
            .payload(&frame[HEADER_LEN..]);
        self.producer
            .send(record)
            .map_err(|(e, _)| anyhow!("kafka topic {}: {e}", self.topic))
    }
}
