//! Producer client that publishes framed messages to a bstream router.

use std::{
    io::{self, Write},
    net::{TcpStream, ToSocketAddrs},
};

use bstream_common::frame::FrameHeader;

/// 32-bit FNV-1a. The router and kernel pick consumer-group members by this value, so every
/// producer must hash keys the same way.
pub fn key_hash(key: &[u8]) -> u32 {
    key.iter().fold(0x811C_9DC5, |hash, &byte| {
        (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
    })
}

pub struct Client<W> {
    writer: W,
}

impl Client<TcpStream> {
    pub fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        Ok(Self::new(TcpStream::connect(addr)?))
    }
}

impl<W: Write> Client<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub fn publish(&mut self, route: u32, key: &[u8], payload: &[u8]) -> io::Result<()> {
        let header = FrameHeader::new(route, key_hash(key), payload.len())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        self.writer.write_all(&header.to_bytes())?;
        self.writer.write_all(payload)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}
