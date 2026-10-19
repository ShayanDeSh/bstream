//! Appends frames to a Redis stream with `XADD`. Each entry has two fields: `key_hash` (decimal)
//! and `payload` (raw bytes).
//!
//! One worker task per route drains a bounded queue, so entries are appended in ingest order.

use anyhow::anyhow;
use bstream_common::frame::{FrameHeader, HEADER_LEN};
use bytes::Bytes;
use log::warn;
use redis::{Client, RedisResult, aio::MultiplexedConnection};
use tokio::sync::mpsc;

use crate::sinks::Sink;

/// Frames waiting per route. When full, `publish` drops the frame instead of blocking ingest.
const QUEUE_LEN: usize = 1024;
/// Most `XADD`s sent in one pipelined round trip.
const BATCH_LEN: usize = 256;

pub struct RedisSink {
    stream: String,
    tx: mpsc::Sender<(u32, Bytes)>,
}

impl RedisSink {
    /// Must be called inside a tokio runtime. Connects lazily, so Redis may be down at startup.
    pub fn new(client: Client, stream: String) -> Self {
        let (tx, rx) = mpsc::channel(QUEUE_LEN);
        tokio::spawn(write_entries(client, stream.clone(), rx));
        Self { stream, tx }
    }
}

impl Sink for RedisSink {
    fn publish(&self, header: &FrameHeader, frame: &Bytes) -> anyhow::Result<()> {
        self.tx
            .try_send((header.key_hash, frame.slice(HEADER_LEN..)))
            .map_err(|e| anyhow!("redis stream {}: {e}", self.stream))
    }
}

/// Runs until the sink is dropped. A failed batch is dropped and the connection reopened for the
/// next one.
async fn write_entries(client: Client, stream: String, mut rx: mpsc::Receiver<(u32, Bytes)>) {
    let mut conn = None;
    let mut batch = Vec::with_capacity(BATCH_LEN);
    while rx.recv_many(&mut batch, BATCH_LEN).await > 0 {
        if let Err(e) = xadd(&client, &mut conn, &stream, &batch).await {
            warn!("redis stream {stream}: dropped {} frames: {e}", batch.len());
            conn = None;
        }
        batch.clear();
    }
}

async fn xadd(
    client: &Client,
    conn: &mut Option<MultiplexedConnection>,
    stream: &str,
    batch: &[(u32, Bytes)],
) -> RedisResult<()> {
    let conn = match conn {
        Some(conn) => conn,
        None => conn.insert(client.get_multiplexed_async_connection().await?),
    };
    let mut pipe = redis::pipe();
    for (key_hash, payload) in batch {
        pipe.cmd("XADD")
            .arg(stream)
            .arg("*")
            .arg("key_hash")
            .arg(*key_hash)
            .arg("payload")
            .arg(&payload[..])
            .ignore();
    }
    pipe.query_async(conn).await
}
