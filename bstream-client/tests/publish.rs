use std::{io::Read, net::TcpListener, thread};

use bstream_client::{Client, key_hash};
use bstream_common::frame::{MAX_PAYLOAD_LEN, decode};

/// Given a client writing into a buffer,
/// when it publishes a message,
/// then the buffer holds one frame with the route, the key's hash, and the payload.
#[test]
fn publish_writes_one_frame() {
    let mut client = Client::new(Vec::new());

    client.publish(42, b"user-1", b"payload").unwrap();

    let bytes = client.into_inner();
    let (header, payload) = decode(&bytes).unwrap().unwrap();
    assert_eq!(header.route_id, 42);
    assert_eq!(header.key_hash, key_hash(b"user-1"));
    assert_eq!(payload, b"payload");
    assert_eq!(header.frame_len(), bytes.len());
}

/// Given a client that publishes several messages,
/// when the output is decoded frame by frame,
/// then the messages come back in publish order.
#[test]
fn publishes_are_written_in_order() {
    let mut client = Client::new(Vec::new());
    for i in 0..3u32 {
        client.publish(i, b"k", &i.to_le_bytes()).unwrap();
    }

    let bytes = client.into_inner();
    let mut rest = &bytes[..];
    for i in 0..3u32 {
        let (header, payload) = decode(rest).unwrap().unwrap();
        assert_eq!(header.route_id, i);
        assert_eq!(payload, i.to_le_bytes());
        rest = &rest[header.frame_len()..];
    }
    assert!(rest.is_empty());
}

/// Given two keys,
/// when they are hashed,
/// then equal keys hash equally, different keys differ, and values match FNV-1a.
#[test]
fn key_hash_is_stable_fnv1a() {
    assert_eq!(key_hash(b""), 0x811C_9DC5);
    assert_eq!(key_hash(b"a"), 0xE40C_292C);
    assert_eq!(key_hash(b"foobar"), 0xBF9C_F968);
    assert_ne!(key_hash(b"user-1"), key_hash(b"user-2"));
}

/// Given a payload over the frame size limit,
/// when it is published,
/// then publish fails with InvalidInput and writes nothing.
#[test]
fn oversized_payload_is_rejected_without_writing() {
    let mut client = Client::new(Vec::new());

    let err = client
        .publish(1, b"k", &vec![0; MAX_PAYLOAD_LEN + 1])
        .unwrap_err();

    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(client.into_inner().is_empty());
}

/// Given a client connected to a TCP listener,
/// when it publishes a message and disconnects,
/// then the listener reads exactly that frame.
#[test]
fn publish_over_tcp() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let mut buf = Vec::new();
        listener.accept().unwrap().0.read_to_end(&mut buf).unwrap();
        buf
    });

    let mut client = Client::connect(addr).unwrap();
    client.publish(5, b"key", b"over tcp").unwrap();
    drop(client);

    let bytes = server.join().unwrap();
    let (header, payload) = decode(&bytes).unwrap().unwrap();
    assert_eq!(header.route_id, 5);
    assert_eq!(payload, b"over tcp");
}

/// Given a TCP address with no listener,
/// when the client connects,
/// then connect fails with an I/O error.
#[test]
fn connect_to_closed_port_fails() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();

    assert!(Client::connect(addr).is_err());
}
