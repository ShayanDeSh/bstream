use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::bail;
use bstream_common::frame;
use bytes::BytesMut;
use log::warn;
use tokio::{
    io::AsyncReadExt as _,
    net::{TcpListener, TcpStream},
};

use crate::sinks::{Metrics, Queue, SinkKind, local::LocalSink};

/// Broker sink queues for each configured route id. A frame whose route is missing is counted
/// and dropped.
pub type Routes = HashMap<u32, Vec<Queue>>;

pub struct Stats {
    unknown_route: AtomicU64,
    sinks: HashMap<(u32, SinkKind), Arc<Metrics>>,
}

impl Stats {
    pub fn new(sinks: HashMap<(u32, SinkKind), Arc<Metrics>>) -> Self {
        Self {
            unknown_route: AtomicU64::new(0),
            sinks,
        }
    }

    pub fn unknown_route(&self) -> u64 {
        self.unknown_route.load(Ordering::Relaxed)
    }

    pub fn sink(&self, route_id: u32, kind: SinkKind) -> Option<&Metrics> {
        self.sinks.get(&(route_id, kind)).map(Arc::as_ref)
    }
}

pub async fn serve(
    listener: TcpListener,
    local: Arc<LocalSink>,
    routes: Arc<Routes>,
    stats: Arc<Stats>,
) -> anyhow::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let local = Arc::clone(&local);
        let routes = Arc::clone(&routes);
        let stats = Arc::clone(&stats);
        tokio::spawn(async move {
            if let Err(e) = read_frames(stream, &local, &routes, &stats).await {
                warn!("producer {peer}: {e:#}");
            }
        });
    }
}

/// Reads frames until the producer disconnects. A malformed header ends the connection, since
/// the stream can no longer be split into frames. While any sink queue of a frame's route is
/// full, the socket is not read, so TCP flow control slows the producer down.
async fn read_frames(
    mut stream: TcpStream,
    local: &LocalSink,
    routes: &Routes,
    stats: &Stats,
) -> anyhow::Result<()> {
    let mut buf = BytesMut::new();
    loop {
        while let Some((header, _)) = frame::decode(&buf)? {
            let frame = buf.split_to(header.frame_len()).freeze();
            let Some(queues) = routes.get(&header.route_id) else {
                stats.unknown_route.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            if let Err(e) = local.publish(&header, &frame) {
                warn!("route {}: {e:#}", header.route_id);
            }
            for queue in queues {
                if let Err(e) = queue.push((header, frame.clone())).await {
                    warn!("route {}: {e:#}", header.route_id);
                }
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
