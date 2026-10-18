use bstream_common::frame::{
    FrameError, FrameHeader, HEADER_LEN, MAGIC, MAX_PAYLOAD_LEN, PAYLOAD_LEN_OFFSET, decode,
};

fn encode(route_id: u32, key_hash: u32, payload: &[u8]) -> Vec<u8> {
    let header = FrameHeader::new(route_id, key_hash, payload.len()).unwrap();
    let mut out = header.to_bytes().to_vec();
    out.extend_from_slice(payload);
    out
}

/// Given a frame encoded with a route, key hash, and payload,
/// when it is decoded,
/// then every header field and the payload come back unchanged.
#[test]
fn frame_round_trips() {
    let bytes = encode(7, 0xDEAD_BEEF, b"hello");

    let (header, payload) = decode(&bytes).unwrap().unwrap();

    assert_eq!(header.route_id, 7);
    assert_eq!(header.key_hash, 0xDEAD_BEEF);
    assert_eq!(header.payload_len, 5);
    assert_eq!(header.flags, 0);
    assert_eq!(header.frame_len(), bytes.len());
    assert_eq!(payload, b"hello");
}

/// Given a frame with an empty payload,
/// when it is decoded,
/// then it decodes from exactly the 16 header bytes.
#[test]
fn empty_payload_round_trips() {
    let bytes = encode(1, 2, b"");

    let (header, payload) = decode(&bytes).unwrap().unwrap();

    assert_eq!(bytes.len(), HEADER_LEN);
    assert_eq!(header.frame_len(), HEADER_LEN);
    assert!(payload.is_empty());
}

/// Given a header,
/// when it is encoded,
/// then the layout is little-endian with magic first and payload_len at the offset the eBPF
/// parser reads.
#[test]
fn header_layout_is_fixed_little_endian() {
    let bytes = FrameHeader::new(0x0403_0201, 0x0807_0605, 0x0C0B)
        .unwrap()
        .to_bytes();

    assert_eq!(&bytes[0..2], &MAGIC.to_le_bytes());
    assert_eq!(bytes[2], 1);
    assert_eq!(&bytes[4..8], &[1, 2, 3, 4]);
    assert_eq!(&bytes[8..12], &[5, 6, 7, 8]);
    assert_eq!(
        &bytes[PAYLOAD_LEN_OFFSET..PAYLOAD_LEN_OFFSET + 4],
        &[0x0B, 0x0C, 0, 0]
    );
}

/// Given a frame delivered one byte at a time,
/// when decode is retried after each byte,
/// then it reports an incomplete frame until the last byte arrives and then decodes it.
#[test]
fn frame_split_across_reads() {
    let bytes = encode(3, 4, b"split payload");
    let mut buf = Vec::new();

    for (i, byte) in bytes.iter().enumerate() {
        buf.push(*byte);
        let result = decode(&buf).unwrap();
        if i + 1 < bytes.len() {
            assert!(result.is_none(), "decoded early at {} bytes", buf.len());
        } else {
            let (header, payload) = result.unwrap();
            assert_eq!(header.route_id, 3);
            assert_eq!(payload, b"split payload");
        }
    }
}

/// Given a buffer holding two frames back to back,
/// when the first is decoded and its frame_len bytes are consumed,
/// then the second decodes from the remainder.
#[test]
fn back_to_back_frames_decode_in_order() {
    let mut bytes = encode(1, 0, b"first");
    bytes.extend(encode(2, 0, b"second"));

    let (first, payload) = decode(&bytes).unwrap().unwrap();
    assert_eq!((first.route_id, payload), (1, &b"first"[..]));

    let rest = &bytes[first.frame_len()..];
    let (second, payload) = decode(rest).unwrap().unwrap();
    assert_eq!((second.route_id, payload), (2, &b"second"[..]));
    assert_eq!(second.frame_len(), rest.len());
}

/// Given a header whose magic bytes are wrong,
/// when it is decoded,
/// then decoding fails with BadMagic carrying the value read.
#[test]
fn bad_magic_is_rejected() {
    let mut bytes = encode(1, 0, b"x");
    bytes[0..2].copy_from_slice(&0x1234u16.to_le_bytes());

    assert_eq!(decode(&bytes), Err(FrameError::BadMagic(0x1234)));
}

/// Given only the 16 header bytes of a frame with bad magic,
/// when it is decoded before any payload arrives,
/// then it fails immediately instead of waiting for more bytes.
#[test]
fn bad_magic_is_rejected_before_payload_arrives() {
    let mut bytes = encode(1, 0, b"payload");
    bytes[0] ^= 0xFF;

    assert!(matches!(
        decode(&bytes[..HEADER_LEN]),
        Err(FrameError::BadMagic(_))
    ));
}

/// Given a header with an unknown version byte,
/// when it is decoded,
/// then decoding fails with UnsupportedVersion.
#[test]
fn unknown_version_is_rejected() {
    let mut bytes = encode(1, 0, b"");
    bytes[2] = 9;

    assert_eq!(decode(&bytes), Err(FrameError::UnsupportedVersion(9)));
}

/// Given a payload one byte over the limit,
/// when a header is built for it,
/// then construction fails with PayloadTooLarge.
#[test]
fn oversized_payload_cannot_be_encoded() {
    assert_eq!(
        FrameHeader::new(1, 0, MAX_PAYLOAD_LEN + 1),
        Err(FrameError::PayloadTooLarge(MAX_PAYLOAD_LEN + 1))
    );
}

/// Given a payload exactly at the limit,
/// when a header is built and decoded with its payload,
/// then the frame round-trips.
#[test]
fn max_payload_round_trips() {
    let bytes = encode(1, 0, &vec![0xAB; MAX_PAYLOAD_LEN]);

    let (header, payload) = decode(&bytes).unwrap().unwrap();

    assert_eq!(header.payload_len as usize, MAX_PAYLOAD_LEN);
    assert_eq!(payload.len(), MAX_PAYLOAD_LEN);
}

/// Given a received header that claims a payload larger than the limit,
/// when it is decoded,
/// then decoding fails with PayloadTooLarge without waiting for the payload.
#[test]
fn oversized_payload_len_on_the_wire_is_rejected() {
    let mut bytes = encode(1, 0, b"");
    bytes[PAYLOAD_LEN_OFFSET..].copy_from_slice(&u32::MAX.to_le_bytes());

    assert_eq!(
        decode(&bytes),
        Err(FrameError::PayloadTooLarge(u32::MAX as usize))
    );
}

/// Given fewer bytes than a header,
/// when decoded,
/// then decode reports an incomplete frame rather than an error.
#[test]
fn partial_header_is_incomplete() {
    assert_eq!(decode(&[]), Ok(None));
    assert_eq!(decode(&MAGIC.to_le_bytes()), Ok(None));
}
