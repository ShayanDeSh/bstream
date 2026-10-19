// Each test crate uses a different subset of these helpers.
#![allow(dead_code)]

pub mod kafka;
pub mod redis_streams;

use std::{
    io::ErrorKind,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use bstream::{
    Router, config::Config, ebpf::KernelFilter, ingest::Stats, sinks::local::SUBSCRIBE_OK,
};
use bstream_client::Client;
use bstream_common::frame::{FrameHeader, HEADER_LEN};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    time::{sleep, timeout},
};

pub const READ_TIMEOUT: Duration = Duration::from_secs(10);

pub struct TestRouter {
    pub ingest: SocketAddr,
    pub subscribe: SocketAddr,
    pub stats: Arc<Stats>,
    pub filter: Option<Arc<KernelFilter>>,
}

pub async fn start(extra_config: &str) -> TestRouter {
    let config = Config::from_toml(&format!(
        "ingest = \"127.0.0.1:0\"\nsubscribe = \"127.0.0.1:0\"\n{extra_config}"
    ))
    .unwrap();
    let router = Router::bind(&config).await.unwrap();
    let test_router = TestRouter {
        ingest: router.ingest_addr().unwrap(),
        subscribe: router.subscribe_addr().unwrap(),
        stats: router.stats(),
        filter: router.filter(),
    };
    tokio::spawn(router.run());
    test_router
}

pub async fn handshake(mut stream: TcpStream, route: u32) -> (TcpStream, u8) {
    stream.write_u32_le(route).await.unwrap();
    let status = timeout(READ_TIMEOUT, stream.read_u8())
        .await
        .unwrap()
        .unwrap();
    (stream, status)
}

pub async fn subscribe(addr: SocketAddr, route: u32) -> TcpStream {
    let (stream, status) = handshake(TcpStream::connect(addr).await.unwrap(), route).await;
    assert_eq!(status, SUBSCRIBE_OK);
    stream
}

/// Returns `None` once the router closes the subscription.
pub async fn read_frame(stream: &mut TcpStream) -> Option<(FrameHeader, Vec<u8>)> {
    let mut header = [0; HEADER_LEN];
    match timeout(READ_TIMEOUT, stream.read_exact(&mut header))
        .await
        .unwrap()
    {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return None,
        Err(e) => panic!("reading frame: {e}"),
    }
    let header = FrameHeader::from_bytes(&header).unwrap();
    let mut payload = vec![0; header.payload_len as usize];
    timeout(READ_TIMEOUT, stream.read_exact(&mut payload))
        .await
        .unwrap()
        .unwrap();
    Some((header, payload))
}

pub async fn expect_frame(stream: &mut TcpStream, route: u32, payload: &[u8]) {
    let (header, got) = read_frame(stream).await.expect("subscription closed");
    assert_eq!((header.route_id, &got[..]), (route, payload));
}

pub async fn expect_closed(stream: &mut TcpStream) {
    let n = timeout(READ_TIMEOUT, stream.read(&mut [0; 1]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 0, "expected the router to close the connection");
}

pub fn encode(frames: &[(u32, &[u8])]) -> Vec<u8> {
    let mut client = Client::new(Vec::new());
    for (route, payload) in frames {
        client.publish(*route, b"key", payload).unwrap();
    }
    client.into_inner()
}

/// Polls until `done` returns true, failing the test after `READ_TIMEOUT`.
pub async fn wait_until<F: Future<Output = bool>>(what: &str, mut done: impl FnMut() -> F) {
    let deadline = Instant::now() + READ_TIMEOUT;
    while !done().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(20)).await;
    }
}

pub async fn send(addr: SocketAddr, bytes: &[u8]) {
    TcpStream::connect(addr)
        .await
        .unwrap()
        .write_all(bytes)
        .await
        .unwrap();
}
