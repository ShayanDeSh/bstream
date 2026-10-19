//! Publishes frames to a RabbitMQ exchange with publisher confirms. The message body is the
//! payload; `key_hash` travels in a header of the same name. Messages are persistent and
//! mandatory, so an unroutable message is returned instead of silently discarded.
//!
//! A frame counts as delivered only once the broker acks it without returning it. One worker
//! task per route drains a bounded queue over one channel, so messages are published in ingest
//! order.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::anyhow;
use bstream_common::frame::{FrameHeader, HEADER_LEN};
use bytes::Bytes;
use lapin::{
    BasicProperties, Channel, Confirmation, Connection, ConnectionProperties,
    options::{BasicPublishOptions, ConfirmSelectOptions},
    types::{AMQPValue, FieldTable},
    uri::AMQPUri,
};
use log::warn;
use tokio::sync::mpsc;

use crate::sinks::Sink;

/// Frames waiting per route. When full, `publish` drops the frame instead of blocking ingest.
const QUEUE_LEN: usize = 1024;
/// Most messages published before awaiting their confirms.
const BATCH_LEN: usize = 256;
const PERSISTENT: u8 = 2;

pub struct RabbitMqSink {
    target: String,
    tx: mpsc::Sender<(u32, Bytes)>,
    counts: Arc<Counts>,
}

#[derive(Default)]
struct Counts {
    delivered: AtomicU64,
    failed: AtomicU64,
}

struct Target {
    uri: AMQPUri,
    exchange: String,
    routing_key: String,
}

impl RabbitMqSink {
    /// Must be called inside a tokio runtime. Connects lazily, so RabbitMQ may be down at
    /// startup.
    pub fn new(uri: AMQPUri, exchange: String, routing_key: String) -> Self {
        let (tx, rx) = mpsc::channel(QUEUE_LEN);
        let counts = Arc::<Counts>::default();
        let target = format!("rabbitmq exchange {exchange:?} key {routing_key:?}");
        tokio::spawn(publish_batches(
            Target {
                uri,
                exchange,
                routing_key,
            },
            rx,
            Arc::clone(&counts),
        ));
        Self { target, tx, counts }
    }

    /// Frames the broker has confirmed.
    pub fn delivered(&self) -> u64 {
        self.counts.delivered.load(Ordering::Relaxed)
    }

    /// Frames that were nacked, returned as unroutable, or lost to a connection error.
    pub fn failed(&self) -> u64 {
        self.counts.failed.load(Ordering::Relaxed)
    }
}

impl Sink for RabbitMqSink {
    fn publish(&self, header: &FrameHeader, frame: &Bytes) -> anyhow::Result<()> {
        self.tx
            .try_send((header.key_hash, frame.slice(HEADER_LEN..)))
            .map_err(|e| anyhow!("{}: {e}", self.target))
    }
}

/// Runs until the sink is dropped. After a connection error the batch is dropped and the
/// connection reopened for the next one.
async fn publish_batches(
    target: Target,
    mut rx: mpsc::Receiver<(u32, Bytes)>,
    counts: Arc<Counts>,
) {
    let mut channel = None;
    let mut batch = Vec::with_capacity(BATCH_LEN);
    while rx.recv_many(&mut batch, BATCH_LEN).await > 0 {
        let delivered = match publish_confirmed(&target, &mut channel, &batch).await {
            Ok(delivered) => delivered,
            Err(e) => {
                warn!("rabbitmq: {e}");
                channel = None;
                0
            }
        };
        let failed = batch.len() - delivered;
        if failed > 0 {
            warn!(
                "rabbitmq exchange {:?} key {:?}: {failed} of {} frames not confirmed",
                target.exchange,
                target.routing_key,
                batch.len()
            );
        }
        counts
            .delivered
            .fetch_add(delivered as u64, Ordering::Relaxed);
        counts.failed.fetch_add(failed as u64, Ordering::Relaxed);
        batch.clear();
    }
}

/// Returns how many frames of `batch` the broker acked without returning.
async fn publish_confirmed(
    target: &Target,
    channel: &mut Option<(Connection, Channel)>,
    batch: &[(u32, Bytes)],
) -> lapin::Result<usize> {
    let (_, channel) = match channel {
        Some(open) => open,
        None => channel.insert(open_channel(&target.uri).await?),
    };
    let mut confirms = Vec::with_capacity(batch.len());
    for (key_hash, payload) in batch {
        let mut headers = FieldTable::default();
        headers.insert("key_hash".into(), AMQPValue::LongUInt(*key_hash));
        let properties = BasicProperties::default()
            .with_delivery_mode(PERSISTENT)
            .with_headers(headers);
        let options = BasicPublishOptions {
            mandatory: true,
            ..Default::default()
        };
        confirms.push(
            channel
                .basic_publish(
                    target.exchange.as_str().into(),
                    target.routing_key.as_str().into(),
                    options,
                    payload,
                    properties,
                )
                .await?,
        );
    }
    let mut delivered = 0;
    for confirm in confirms {
        if matches!(confirm.await?, Confirmation::Ack(None)) {
            delivered += 1;
        }
    }
    Ok(delivered)
}

async fn open_channel(uri: &AMQPUri) -> lapin::Result<(Connection, Channel)> {
    let connection = Connection::connect_uri(uri.clone(), ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await?;
    Ok((connection, channel))
}
