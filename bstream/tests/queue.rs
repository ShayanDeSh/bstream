mod common;

use std::{future, time::Duration};

use anyhow::bail;
use bstream::sinks::{BATCH_LEN, Frame, Queue, Sink};
use bstream_common::frame::{FrameHeader, HEADER_LEN};
use bytes::Bytes;
use common::{READ_TIMEOUT, wait_until};
use tokio::{
    sync::mpsc,
    time::{sleep, timeout},
};

fn frame(seq: u32) -> Frame {
    let header = FrameHeader::new(1, 0, 4).unwrap();
    let bytes = Bytes::from([&header.to_bytes()[..], &seq.to_le_bytes()].concat());
    (header, bytes)
}

fn seq((_, bytes): &Frame) -> u32 {
    u32::from_le_bytes(bytes[HEADER_LEN..].try_into().unwrap())
}

/// Fails its first `failures` writes, then reports every frame it writes.
struct Flaky {
    failures: usize,
    written: mpsc::UnboundedSender<u32>,
}

impl Sink for Flaky {
    async fn write(&mut self, batch: &[Frame]) -> anyhow::Result<usize> {
        if self.failures > 0 {
            self.failures -= 1;
            bail!("down");
        }
        for frame in batch {
            self.written.send(seq(frame)).unwrap();
        }
        Ok(0)
    }
}

fn flaky(failures: usize) -> (Flaky, mpsc::UnboundedReceiver<u32>) {
    let (written, rx) = mpsc::unbounded_channel();
    (Flaky { failures, written }, rx)
}

struct Rejecting;

impl Sink for Rejecting {
    async fn write(&mut self, batch: &[Frame]) -> anyhow::Result<usize> {
        Ok(batch.len())
    }
}

struct Stuck;

impl Sink for Stuck {
    async fn write(&mut self, _: &[Frame]) -> anyhow::Result<usize> {
        future::pending().await
    }
}

struct Panicking;

impl Sink for Panicking {
    async fn write(&mut self, _: &[Frame]) -> anyhow::Result<usize> {
        panic!("sink bug");
    }
}

async fn expect_written(rx: &mut mpsc::UnboundedReceiver<u32>, seqs: impl Iterator<Item = u32>) {
    for expected in seqs {
        let got = timeout(READ_TIMEOUT, rx.recv()).await.unwrap();
        assert_eq!(got, Some(expected));
    }
}

/// Given a queue in front of a healthy sink,
/// when 1000 frames are pushed,
/// then the sink writes them in push order and the metrics count all 1000 as delivered with
/// nothing pending, failed, or retried.
#[tokio::test]
async fn frames_are_delivered_in_order() {
    let (sink, mut written) = flaky(0);
    let queue = Queue::spawn("test".into(), sink, 16);

    for i in 0..1000 {
        queue.push(frame(i)).await.unwrap();
    }

    expect_written(&mut written, 0..1000).await;
    let metrics = queue.metrics();
    wait_until("delivered count", || async { metrics.delivered() == 1000 }).await;
    assert_eq!(
        (metrics.pending(), metrics.failed(), metrics.retries()),
        (0, 0, 0)
    );
}

/// Given a queue in front of a sink whose first three writes fail,
/// when three frames are pushed,
/// then the sink retries until it writes all three once, in order, and the metrics count three
/// retries and three deliveries.
#[tokio::test]
async fn failed_writes_are_retried_until_delivered() {
    let (sink, mut written) = flaky(3);
    let queue = Queue::spawn("test".into(), sink, 16);

    for i in 0..3 {
        queue.push(frame(i)).await.unwrap();
    }

    expect_written(&mut written, 0..3).await;
    let metrics = queue.metrics();
    wait_until("delivered count", || async { metrics.delivered() == 3 }).await;
    assert_eq!((metrics.retries(), metrics.failed()), (3, 0));
    assert!(written.try_recv().is_err(), "a frame was written twice");
}

/// Given a queue in front of a sink that rejects every frame,
/// when five frames are pushed,
/// then all five count as failed and none are retried.
#[tokio::test]
async fn rejected_frames_count_as_failed_without_retry() {
    let queue = Queue::spawn("test".into(), Rejecting, 16);

    for i in 0..5 {
        queue.push(frame(i)).await.unwrap();
    }

    let metrics = queue.metrics();
    wait_until("failed count", || async { metrics.failed() == 5 }).await;
    assert_eq!(
        (metrics.delivered(), metrics.retries(), metrics.pending()),
        (0, 0, 0)
    );
}

/// Given a queue of capacity 4 in front of a sink whose write never completes,
/// when frames are pushed until a push does not complete within 100 ms,
/// then at least 4 and at most 4 + BATCH_LEN frames are accepted, all of them pending.
#[tokio::test]
async fn full_queue_makes_push_wait() {
    let queue = Queue::spawn("test".into(), Stuck, 4);

    let mut accepted = 0;
    while timeout(Duration::from_millis(100), queue.push(frame(accepted)))
        .await
        .is_ok()
    {
        accepted += 1;
        assert!(accepted as usize <= 4 + BATCH_LEN, "push never waited");
    }

    assert!(accepted >= 4, "only {accepted} frames accepted");
    assert_eq!(queue.metrics().pending(), u64::from(accepted));
}

/// Given a queue whose sink panics on its first write,
/// when a frame is pushed and pushes continue,
/// then a later push fails with an error naming the queue instead of waiting forever.
#[tokio::test]
async fn push_fails_after_worker_dies() {
    let queue = Queue::spawn("doomed".into(), Panicking, 4);
    queue.push(frame(0)).await.unwrap();

    let err = timeout(READ_TIMEOUT, async {
        loop {
            if let Err(e) = queue.push(frame(1)).await {
                break e;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pushes kept succeeding");

    assert!(err.to_string().contains("doomed"), "{err}");
}
