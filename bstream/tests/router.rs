use std::{io::ErrorKind, net::SocketAddr, sync::Arc, time::Duration};

use bstream::{
    Router,
    config::Config,
    ingest::Stats,
    sinks::local::{SUBSCRIBE_OK, SUBSCRIBE_UNKNOWN_ROUTE},
};
use bstream_client::{Client, key_hash};
use bstream_common::frame::{FrameHeader, HEADER_LEN, MAX_PAYLOAD_LEN};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpSocket, TcpStream},
    time::{sleep, timeout},
};

const READ_TIMEOUT: Duration = Duration::from_secs(10);

struct TestRouter {
    ingest: SocketAddr,
    subscribe: SocketAddr,
    stats: Arc<Stats>,
}

async fn start(extra_config: &str) -> TestRouter {
    let config = Config::from_toml(&format!(
        "ingest = \"127.0.0.1:0\"\nsubscribe = \"127.0.0.1:0\"\n{extra_config}"
    ))
    .unwrap();
    let router = Router::bind(&config).await.unwrap();
    let test_router = TestRouter {
        ingest: router.ingest_addr().unwrap(),
        subscribe: router.subscribe_addr().unwrap(),
        stats: router.stats(),
    };
    tokio::spawn(router.run());
    test_router
}

async fn handshake(mut stream: TcpStream, route: u32) -> (TcpStream, u8) {
    stream.write_u32_le(route).await.unwrap();
    let status = timeout(READ_TIMEOUT, stream.read_u8())
        .await
        .unwrap()
        .unwrap();
    (stream, status)
}

async fn subscribe(addr: SocketAddr, route: u32) -> TcpStream {
    let (stream, status) = handshake(TcpStream::connect(addr).await.unwrap(), route).await;
    assert_eq!(status, SUBSCRIBE_OK);
    stream
}

/// Returns `None` once the router closes the subscription.
async fn read_frame(stream: &mut TcpStream) -> Option<(FrameHeader, Vec<u8>)> {
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

async fn expect_frame(stream: &mut TcpStream, route: u32, payload: &[u8]) {
    let (header, got) = read_frame(stream).await.expect("subscription closed");
    assert_eq!((header.route_id, &got[..]), (route, payload));
}

async fn expect_closed(stream: &mut TcpStream) {
    let n = timeout(READ_TIMEOUT, stream.read(&mut [0; 1]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 0, "expected the router to close the connection");
}

fn encode(frames: &[(u32, &[u8])]) -> Vec<u8> {
    let mut client = Client::new(Vec::new());
    for (route, payload) in frames {
        client.publish(*route, b"key", payload).unwrap();
    }
    client.into_inner()
}

async fn send(addr: SocketAddr, bytes: &[u8]) {
    TcpStream::connect(addr)
        .await
        .unwrap()
        .write_all(bytes)
        .await
        .unwrap();
}

/// Given two subscribers on route 1,
/// when one producer publishes 1000 frames with distinct keys to route 1,
/// then each subscriber receives all 1000 frames in publish order with keys and payloads intact.
#[tokio::test]
async fn two_subscribers_receive_every_frame_in_order() {
    const N: u32 = 1000;
    let router = start("routes = [{ id = 1 }]").await;
    let mut subscribers = [
        subscribe(router.subscribe, 1).await,
        subscribe(router.subscribe, 1).await,
    ];

    let mut client = Client::new(Vec::new());
    for i in 0..N {
        client
            .publish(1, format!("key-{i}").as_bytes(), &i.to_le_bytes())
            .unwrap();
    }
    send(router.ingest, &client.into_inner()).await;

    for subscriber in &mut subscribers {
        for i in 0..N {
            let (header, payload) = read_frame(subscriber).await.expect("subscription closed");
            assert_eq!(header.route_id, 1);
            assert_eq!(header.key_hash, key_hash(format!("key-{i}").as_bytes()));
            assert_eq!(payload, i.to_le_bytes());
        }
    }
    assert_eq!(router.stats.unknown_route(), 0);
}

/// Given one subscriber on route 1 and one on route 2,
/// when a producer interleaves frames for both routes,
/// then each subscriber receives only its route's frames, in order.
#[tokio::test]
async fn subscribers_only_receive_their_route() {
    let router = start("routes = [{ id = 1 }, { id = 2 }]").await;
    let mut first = subscribe(router.subscribe, 1).await;
    let mut second = subscribe(router.subscribe, 2).await;

    send(
        router.ingest,
        &encode(&[(1, b"a0"), (2, b"b0"), (1, b"a1"), (2, b"b1")]),
    )
    .await;

    expect_frame(&mut first, 1, b"a0").await;
    expect_frame(&mut first, 1, b"a1").await;
    expect_frame(&mut second, 2, b"b0").await;
    expect_frame(&mut second, 2, b"b1").await;
}

/// Given a router with only route 1 and a subscriber on it,
/// when a producer interleaves frames for unconfigured route 99 with frames for route 1,
/// then the subscriber receives only the route 1 frames and both unknown frames are counted.
#[tokio::test]
async fn unknown_route_frames_are_counted_and_dropped() {
    let router = start("routes = [{ id = 1 }]").await;
    let mut subscriber = subscribe(router.subscribe, 1).await;

    send(
        router.ingest,
        &encode(&[(99, b"x"), (1, b"a"), (99, b"y"), (1, b"b")]),
    )
    .await;

    expect_frame(&mut subscriber, 1, b"a").await;
    expect_frame(&mut subscriber, 1, b"b").await;
    assert_eq!(router.stats.unknown_route(), 2);
}

/// Given a subscriber on route 1,
/// when a producer sends one frame as three separate TCP writes with pauses between them,
/// then the router reassembles it and delivers the whole frame once.
#[tokio::test]
async fn frame_split_across_writes_is_reassembled() {
    let router = start("routes = [{ id = 1 }]").await;
    let mut subscriber = subscribe(router.subscribe, 1).await;
    let bytes = encode(&[(1, b"split payload")]);
    let mut producer = TcpStream::connect(router.ingest).await.unwrap();
    producer.set_nodelay(true).unwrap();

    for chunk in [
        &bytes[..10],
        &bytes[10..HEADER_LEN + 5],
        &bytes[HEADER_LEN + 5..],
    ] {
        producer.write_all(chunk).await.unwrap();
        sleep(Duration::from_millis(50)).await;
    }

    expect_frame(&mut subscriber, 1, b"split payload").await;
}

/// Given a subscriber on route 1,
/// when a producer sends half a frame and disconnects, then another producer sends a full frame,
/// then the partial frame is discarded and the subscriber's next frame is the full one.
#[tokio::test]
async fn partial_frame_at_disconnect_is_discarded() {
    let router = start("routes = [{ id = 1 }]").await;
    let mut subscriber = subscribe(router.subscribe, 1).await;
    let partial = encode(&[(1, b"never delivered")]);
    let mut producer = TcpStream::connect(router.ingest).await.unwrap();

    producer
        .write_all(&partial[..partial.len() - 3])
        .await
        .unwrap();
    producer.shutdown().await.unwrap();
    expect_closed(&mut producer).await;
    send(router.ingest, &encode(&[(1, b"complete")])).await;

    expect_frame(&mut subscriber, 1, b"complete").await;
}

/// Given a subscriber on route 1,
/// when a producer sends a header with bad magic,
/// then the router closes that producer and keeps delivering frames from other producers.
#[tokio::test]
async fn bad_magic_closes_producer_connection() {
    let router = start("routes = [{ id = 1 }]").await;
    let mut subscriber = subscribe(router.subscribe, 1).await;
    let mut producer = TcpStream::connect(router.ingest).await.unwrap();

    producer.write_all(&[0xFF; HEADER_LEN]).await.unwrap();

    expect_closed(&mut producer).await;
    send(router.ingest, &encode(&[(1, b"still routing")])).await;
    expect_frame(&mut subscriber, 1, b"still routing").await;
}

/// Given a router with only route 1,
/// when a client subscribes to route 7,
/// then the router replies with SUBSCRIBE_UNKNOWN_ROUTE and closes the connection.
#[tokio::test]
async fn subscribe_to_unknown_route_is_rejected() {
    let router = start("routes = [{ id = 1 }]").await;

    let (mut stream, status) =
        handshake(TcpStream::connect(router.subscribe).await.unwrap(), 7).await;

    assert_eq!(status, SUBSCRIBE_UNKNOWN_ROUTE);
    expect_closed(&mut stream).await;
}

/// Given subscriber_buffer = 1, a route 1 subscriber with a tiny receive buffer that is not
/// reading, and a route 2 subscriber,
/// when a producer sends 16 maximum-size frames to route 1 followed by one frame to route 2,
/// then the route 1 subscriber receives a gap-free prefix of the frames and is then disconnected
/// instead of silently skipping frames.
#[tokio::test]
async fn lagging_subscriber_is_disconnected() {
    const N: u32 = 16;
    let router = start("subscriber_buffer = 1\nroutes = [{ id = 1 }, { id = 2 }]").await;
    let socket = TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let (mut slow, status) = handshake(socket.connect(router.subscribe).await.unwrap(), 1).await;
    assert_eq!(status, SUBSCRIBE_OK);
    let mut marker = subscribe(router.subscribe, 2).await;

    let mut client = Client::new(Vec::new());
    let mut payload = vec![0; MAX_PAYLOAD_LEN];
    for i in 0..N {
        payload[..4].copy_from_slice(&i.to_le_bytes());
        client.publish(1, b"key", &payload).unwrap();
    }
    client.publish(2, b"key", b"done").unwrap();
    send(router.ingest, &client.into_inner()).await;
    // The route 2 frame follows every route 1 frame on the same producer connection, so once it
    // arrives the router has published all of route 1.
    expect_frame(&mut marker, 2, b"done").await;

    let mut received = 0u32;
    while let Some((_, payload)) = read_frame(&mut slow).await {
        assert_eq!(payload[..4], received.to_le_bytes());
        received += 1;
    }
    assert!(received < N, "slow subscriber received all {N} frames");
}
