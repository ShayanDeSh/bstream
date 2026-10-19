//! Delivery to external systems. Each broker sink on a route gets its own bounded [`Queue`] and
//! worker task. `push` waits while the queue is full, so a slow or unreachable sink stops ingest
//! from reading the producer socket and TCP pushes back on the producer. A failed write is
//! retried with exponential backoff until it succeeds, so delivery is at-least-once.

pub mod kafka;
pub mod local;
pub mod rabbitmq;
pub mod redis;

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::anyhow;
use bstream_common::frame::FrameHeader;
use bytes::Bytes;
use log::warn;
use tokio::{sync::mpsc, time::sleep};

/// A whole frame, header included, with its decoded header.
pub type Frame = (FrameHeader, Bytes);

/// Most frames handed to one `Sink::write`.
pub const BATCH_LEN: usize = 256;
const RETRY_MIN: Duration = Duration::from_millis(50);
const RETRY_MAX: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SinkKind {
    Kafka,
    Redis,
    RabbitMq,
}

pub trait Sink: Send + 'static {
    /// Delivers `batch` in order and returns how many of its frames were rejected for good. An
    /// error means the attempt failed as a whole; the same batch is retried after a backoff, so
    /// frames written before the error may be delivered twice.
    fn write(&mut self, batch: &[Frame]) -> impl Future<Output = anyhow::Result<usize>> + Send;
}

#[derive(Default)]
pub struct Metrics {
    pending: AtomicU64,
    delivered: AtomicU64,
    failed: AtomicU64,
    retries: AtomicU64,
}

impl Metrics {
    /// Frames queued or being written. At most the queue capacity plus `BATCH_LEN`.
    pub fn pending(&self) -> u64 {
        self.pending.load(Ordering::Relaxed)
    }

    pub fn delivered(&self) -> u64 {
        self.delivered.load(Ordering::Relaxed)
    }

    /// Frames the sink rejected for good. They are not retried.
    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    /// Failed write attempts, each followed by a retry of the whole batch.
    pub fn retries(&self) -> u64 {
        self.retries.load(Ordering::Relaxed)
    }
}

pub struct Queue {
    name: String,
    tx: mpsc::Sender<Frame>,
    metrics: Arc<Metrics>,
}

impl Queue {
    /// Must be called inside a tokio runtime. The worker stops once the queue is dropped and
    /// drained.
    pub fn spawn(name: String, sink: impl Sink, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        let metrics = Arc::<Metrics>::default();
        tokio::spawn(drain(name.clone(), sink, rx, Arc::clone(&metrics)));
        Self { name, tx, metrics }
    }

    /// Waits while the queue is full.
    pub async fn push(&self, frame: Frame) -> anyhow::Result<()> {
        let permit = self
            .tx
            .reserve()
            .await
            .map_err(|_| anyhow!("{}: worker stopped", self.name))?;
        // Counted before the worker can see the frame, so `pending` never underflows.
        self.metrics.pending.fetch_add(1, Ordering::Relaxed);
        permit.send(frame);
        Ok(())
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }
}

async fn drain(
    name: String,
    mut sink: impl Sink,
    mut rx: mpsc::Receiver<Frame>,
    metrics: Arc<Metrics>,
) {
    let mut batch = Vec::with_capacity(BATCH_LEN);
    while rx.recv_many(&mut batch, BATCH_LEN).await > 0 {
        let mut backoff = RETRY_MIN;
        let rejected = loop {
            match sink.write(&batch).await {
                Ok(rejected) => break rejected,
                Err(e) => {
                    metrics.retries.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        "{name}: retrying {} frames in {backoff:?}: {e:#}",
                        batch.len()
                    );
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(RETRY_MAX);
                }
            }
        };
        if rejected > 0 {
            warn!("{name}: {rejected} of {} frames rejected", batch.len());
        }
        let len = batch.len() as u64;
        metrics
            .delivered
            .fetch_add(len - rejected as u64, Ordering::Relaxed);
        metrics.failed.fetch_add(rejected as u64, Ordering::Relaxed);
        metrics.pending.fetch_sub(len, Ordering::Relaxed);
        batch.clear();
    }
}
