use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::bail;
use bstream_common::frame;
use bytes::BytesMut;
use log::warn;
use tokio::{
    io::AsyncReadExt as _,
    net::{TcpListener, TcpStream},
};

use crate::sinks::local::LocalSink;

#[derive(Default)]
pub struct Stats {
    unknown_route: AtomicU64,
}

impl Stats {
    pub fn unknown_route(&self) -> u64 {
        self.unknown_route.load(Ordering::Relaxed)
    }
}

pub async fn serve(
    listener: TcpListener,
    local: Arc<LocalSink>,
    stats: Arc<Stats>,
) -> anyhow::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let local = Arc::clone(&local);
        let stats = Arc::clone(&stats);
        tokio::spawn(async move {
            if let Err(e) = read_frames(stream, &local, &stats).await {
                warn!("producer {peer}: {e:#}");
            }
        });
    }
}

/// Reads frames until the producer disconnects. A malformed header ends the connection, since
/// the stream can no longer be split into frames.
async fn read_frames(
    mut stream: TcpStream,
    local: &LocalSink,
    stats: &Stats,
) -> anyhow::Result<()> {
    let mut buf = BytesMut::new();
    loop {
        while let Some((header, _)) = frame::decode(&buf)? {
            let frame = buf.split_to(header.frame_len()).freeze();
            if !local.publish(header.route_id, frame) {
                stats.unknown_route.fetch_add(1, Ordering::Relaxed);
            }
        }
        if stream.read_buf(&mut buf).await? == 0 {
            if !buf.is_empty() {
                bail!("disconnected mid-frame with {} bytes buffered", buf.len());
            }
            return Ok(());
        }
    }
}
