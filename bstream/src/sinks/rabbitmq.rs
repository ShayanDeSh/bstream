//! Publishes frames to a RabbitMQ exchange with publisher confirms. The message body is the
//! payload; `key_hash` travels in a header of the same name. Messages are persistent and
//! mandatory, so an unroutable message is returned instead of silently discarded.
//!
//! A frame counts as delivered only once the broker acks it without returning it. A nacked or
//! returned frame is rejected; a connection or channel error retries the batch on a new channel.

use anyhow::anyhow;
use bstream_common::frame::HEADER_LEN;
use lapin::{
    BasicProperties, Channel, Confirmation, Connection, ConnectionProperties,
    options::{BasicPublishOptions, ConfirmSelectOptions},
    types::{AMQPValue, FieldTable},
    uri::AMQPUri,
};

use crate::sinks::{Frame, Sink};

const PERSISTENT: u8 = 2;

pub struct RabbitMqSink {
    uri: AMQPUri,
    exchange: String,
    routing_key: String,
    /// The connection must outlive its channel.
    channel: Option<(Connection, Channel)>,
}

impl RabbitMqSink {
    /// Connects lazily, so RabbitMQ may be down at startup.
    pub fn new(uri: AMQPUri, exchange: String, routing_key: String) -> Self {
        Self {
            uri,
            exchange,
            routing_key,
            channel: None,
        }
    }

    /// Returns how many frames of `batch` the broker acked without returning.
    async fn publish_confirmed(&mut self, batch: &[Frame]) -> lapin::Result<usize> {
        let (_, channel) = match &mut self.channel {
            Some(open) => open,
            None => self.channel.insert(open_channel(&self.uri).await?),
        };
        let mut confirms = Vec::with_capacity(batch.len());
        for (header, frame) in batch {
            let mut headers = FieldTable::default();
            headers.insert("key_hash".into(), AMQPValue::LongUInt(header.key_hash));
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
                        self.exchange.as_str().into(),
                        self.routing_key.as_str().into(),
                        options,
                        &frame[HEADER_LEN..],
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
}

impl Sink for RabbitMqSink {
    async fn write(&mut self, batch: &[Frame]) -> anyhow::Result<usize> {
        match self.publish_confirmed(batch).await {
            Ok(delivered) => Ok(batch.len() - delivered),
            Err(e) => {
                self.channel = None;
                Err(anyhow!(
                    "rabbitmq exchange {:?} key {:?}: {e}",
                    self.exchange,
                    self.routing_key
                ))
            }
        }
    }
}

async fn open_channel(uri: &AMQPUri) -> lapin::Result<(Connection, Channel)> {
    let connection = Connection::connect_uri(uri.clone(), ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await?;
    Ok((connection, channel))
}
