pub mod kafka;
pub mod local;
pub mod rabbitmq;
pub mod redis;

use bstream_common::frame::FrameHeader;
use bytes::Bytes;

pub trait Sink: Send + Sync {
    /// Hands off one frame, header included, without blocking ingest. An error means this sink
    /// dropped the frame; other sinks on the route are unaffected.
    fn publish(&self, header: &FrameHeader, frame: &Bytes) -> anyhow::Result<()>;
}
