//! Wire format shared by the eBPF stream parser and the user-space router.
//!
//! A frame is a fixed 16-byte little-endian header followed by `payload_len` bytes of payload.
//! The header size is fixed so the kernel parser can find the frame length at a constant offset.

use core::fmt;

pub const MAGIC: u16 = 0xB57E;
pub const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 16;
/// Byte offset of `payload_len`, read directly by the eBPF stream parser.
pub const PAYLOAD_LEN_OFFSET: usize = 12;
/// Upper bound on payload size, so a corrupt or hostile header cannot make the router buffer
/// an unbounded amount of data.
pub const MAX_PAYLOAD_LEN: usize = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    pub flags: u8,
    pub route_id: u32,
    pub key_hash: u32,
    pub payload_len: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    BadMagic(u16),
    UnsupportedVersion(u8),
    PayloadTooLarge(usize),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic(magic) => write!(f, "bad frame magic {magic:#06x}"),
            Self::UnsupportedVersion(version) => write!(f, "unsupported frame version {version}"),
            Self::PayloadTooLarge(len) => {
                write!(
                    f,
                    "payload of {len} bytes exceeds limit of {MAX_PAYLOAD_LEN}"
                )
            }
        }
    }
}

impl core::error::Error for FrameError {}

impl FrameHeader {
    pub fn new(route_id: u32, key_hash: u32, payload_len: usize) -> Result<Self, FrameError> {
        if payload_len > MAX_PAYLOAD_LEN {
            return Err(FrameError::PayloadTooLarge(payload_len));
        }
        Ok(Self {
            flags: 0,
            route_id,
            key_hash,
            payload_len: payload_len as u32,
        })
    }

    pub fn frame_len(&self) -> usize {
        HEADER_LEN + self.payload_len as usize
    }

    pub fn to_bytes(&self) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[0..2].copy_from_slice(&MAGIC.to_le_bytes());
        out[2] = VERSION;
        out[3] = self.flags;
        out[4..8].copy_from_slice(&self.route_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.key_hash.to_le_bytes());
        out[PAYLOAD_LEN_OFFSET..16].copy_from_slice(&self.payload_len.to_le_bytes());
        out
    }

    pub fn from_bytes(b: &[u8; HEADER_LEN]) -> Result<Self, FrameError> {
        let magic = u16::from_le_bytes([b[0], b[1]]);
        if magic != MAGIC {
            return Err(FrameError::BadMagic(magic));
        }
        if b[2] != VERSION {
            return Err(FrameError::UnsupportedVersion(b[2]));
        }
        let payload_len = u32::from_le_bytes([b[12], b[13], b[14], b[15]]);
        if payload_len as usize > MAX_PAYLOAD_LEN {
            return Err(FrameError::PayloadTooLarge(payload_len as usize));
        }
        Ok(Self {
            flags: b[3],
            route_id: u32::from_le_bytes([b[4], b[5], b[6], b[7]]),
            key_hash: u32::from_le_bytes([b[8], b[9], b[10], b[11]]),
            payload_len,
        })
    }
}

/// Decodes the frame at the front of `buf`; it occupies `header.frame_len()` bytes.
///
/// Returns `Ok(None)` while `buf` holds less than a full frame, so a reader can append the next
/// read and retry. The header is validated as soon as its 16 bytes arrive, so a bad stream is
/// rejected without waiting for the payload.
pub fn decode(buf: &[u8]) -> Result<Option<(FrameHeader, &[u8])>, FrameError> {
    let Some(header_bytes) = buf.first_chunk::<HEADER_LEN>() else {
        return Ok(None);
    };
    let header = FrameHeader::from_bytes(header_bytes)?;
    let Some(payload) = buf.get(HEADER_LEN..header.frame_len()) else {
        return Ok(None);
    };
    Ok(Some((header, payload)))
}
