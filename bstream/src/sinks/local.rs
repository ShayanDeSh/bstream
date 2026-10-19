//! Fans frames out to local apps over TCP.
//!
//! Handshake: the subscriber sends a `u32` little-endian route id and the router replies with one
//! status byte. After `SUBSCRIBE_OK`, the router streams every frame of that route, header
//! included, in the order it was ingested. A subscriber either sees every frame or is
//! disconnected; it never silently skips one.

use std::{collections::HashMap, sync::Arc};

use anyhow::{Context as _, bail};
use bstream_common::frame::FrameHeader;
use bytes::Bytes;
use log::warn;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::broadcast::{self, error::RecvError},
};

use crate::sinks::Sink;

pub const SUBSCRIBE_OK: u8 = 0;
pub const SUBSCRIBE_UNKNOWN_ROUTE: u8 = 1;

pub struct LocalSink {
    routes: HashMap<u32, broadcast::Sender<Bytes>>,
}

impl LocalSink {
    pub fn new(route_ids: impl IntoIterator<Item = u32>, buffer: usize) -> Self {
        let routes = route_ids
            .into_iter()
            .map(|id| (id, broadcast::channel(buffer).0))
            .collect();
        Self { routes }
    }

    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> anyhow::Result<()> {
        loop {
            let (stream, peer) = listener.accept().await?;
            let sink = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = sink.serve_subscriber(stream).await {
                    warn!("subscriber {peer}: {e:#}");
                }
            });
        }
    }

    async fn serve_subscriber(&self, mut stream: TcpStream) -> anyhow::Result<()> {
        let route_id = stream.read_u32_le().await?;
        let Some(tx) = self.routes.get(&route_id) else {
            stream.write_u8(SUBSCRIBE_UNKNOWN_ROUTE).await?;
            bail!("unknown route {route_id}");
        };
        // Subscribe before acknowledging, so the subscriber gets every frame ingested after it
        // reads `SUBSCRIBE_OK`.
        let mut rx = tx.subscribe();
        stream.write_u8(SUBSCRIBE_OK).await?;
        loop {
            match rx.recv().await {
                Ok(frame) => stream.write_all(&frame).await?,
                Err(RecvError::Lagged(missed)) => bail!("lagged behind by {missed} frames"),
                Err(RecvError::Closed) => return Ok(()),
            }
        }
    }
}

impl Sink for LocalSink {
    fn publish(&self, header: &FrameHeader, frame: &Bytes) -> anyhow::Result<()> {
        let tx = self
            .routes
            .get(&header.route_id)
            .with_context(|| format!("no local channel for route {}", header.route_id))?;
        // Sending fails only when the route has no subscribers.
        let _ = tx.send(frame.clone());
        Ok(())
    }
}
