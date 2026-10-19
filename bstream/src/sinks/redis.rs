//! Appends frames to a Redis stream with `XADD`. Each entry has two fields: `key_hash` (decimal)
//! and `payload` (raw bytes). A batch is one pipelined round trip.
//!
//! Connection and loading errors are retried. An error reply that retrying cannot fix, such as
//! `WRONGTYPE`, rejects the whole batch.

use anyhow::anyhow;
use bstream_common::frame::HEADER_LEN;
use log::warn;
use redis::{Client, RetryMethod, aio::MultiplexedConnection};

use crate::sinks::{Frame, Sink};

pub struct RedisSink {
    client: Client,
    stream: String,
    conn: Option<MultiplexedConnection>,
}

impl RedisSink {
    /// Connects lazily, so Redis may be down at startup.
    pub fn new(client: Client, stream: String) -> Self {
        Self {
            client,
            stream,
            conn: None,
        }
    }
}

impl Sink for RedisSink {
    async fn write(&mut self, batch: &[Frame]) -> anyhow::Result<usize> {
        let conn = match &mut self.conn {
            Some(conn) => conn,
            None => self.conn.insert(
                self.client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| anyhow!("redis stream {}: {e}", self.stream))?,
            ),
        };
        let mut pipe = redis::pipe();
        for (header, frame) in batch {
            pipe.cmd("XADD")
                .arg(&self.stream)
                .arg("*")
                .arg("key_hash")
                .arg(header.key_hash)
                .arg("payload")
                .arg(&frame[HEADER_LEN..])
                .ignore();
        }
        match pipe.query_async::<()>(conn).await {
            Ok(()) => Ok(0),
            Err(e) if matches!(e.retry_method(), RetryMethod::NoRetry) => {
                warn!("redis stream {}: {e}", self.stream);
                Ok(batch.len())
            }
            Err(e) => {
                self.conn = None;
                Err(anyhow!("redis stream {}: {e}", self.stream))
            }
        }
    }
}
