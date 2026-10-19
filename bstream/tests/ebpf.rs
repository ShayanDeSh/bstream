//! Kernel-side route filtering. Loading eBPF needs `CAP_BPF`, so these are ignored by default;
//! run them as root, e.g. `sudo -E cargo test -p bstream --test ebpf -- --include-ignored`.

mod common;

use std::time::Duration;

use bstream_client::Client;
use common::{expect_frame, start, subscribe, wait_until};
use tokio::{io::AsyncWriteExt as _, net::TcpStream, time::sleep};

/// A producer's socket is filtered only once the router has accepted it and inserted it into
/// `SOCKHASH`. Waiting after connecting, before sending, keeps early frames from bypassing the
/// kernel programs through the socket's normal receive path.
const REGISTER_DELAY: Duration = Duration::from_millis(250);

async fn connected_producer(addr: std::net::SocketAddr) -> TcpStream {
    let producer = TcpStream::connect(addr).await.unwrap();
    sleep(REGISTER_DELAY).await;
    producer
}

/// Given the kernel filter enabled with only route 1 configured and a subscriber on route 1,
/// when a producer interleaves frames for route 1 and unknown route 99,
/// then the subscriber receives only the route 1 frames, the kernel counts both route 99 frames
/// as dropped, and user space never sees them, so its unknown-route counter stays 0.
#[tokio::test]
#[ignore = "needs root to load eBPF"]
async fn kernel_drops_unknown_routes() {
    let router = start("ebpf = true\nroutes = [{ id = 1 }]").await;
    let filter = router.filter.clone().expect("ebpf enabled");
    let mut subscriber = subscribe(router.subscribe, 1).await;
    let mut producer = connected_producer(router.ingest).await;

    let mut client = Client::new(Vec::new());
    for (route, payload) in [(1, b"a"), (99, b"x"), (99, b"y"), (1, b"b")] {
        client.publish(route, b"key", payload).unwrap();
    }
    producer.write_all(&client.into_inner()).await.unwrap();

    expect_frame(&mut subscriber, 1, b"a").await;
    expect_frame(&mut subscriber, 1, b"b").await;
    wait_until("the kernel to drop both unknown frames", || async {
        filter.dropped_unknown().unwrap() == 2
    })
    .await;
    assert_eq!(router.stats.unknown_route(), 0);
}

/// Given the kernel filter enabled with routes 1 and 2 configured and a subscriber on each,
/// when a producer sends frames for both routes,
/// then every frame is delivered and the kernel drops nothing.
#[tokio::test]
#[ignore = "needs root to load eBPF"]
async fn kernel_passes_known_routes() {
    let router = start("ebpf = true\nroutes = [{ id = 1 }, { id = 2 }]").await;
    let filter = router.filter.clone().expect("ebpf enabled");
    let mut first = subscribe(router.subscribe, 1).await;
    let mut second = subscribe(router.subscribe, 2).await;
    let mut producer = connected_producer(router.ingest).await;

    let mut client = Client::new(Vec::new());
    for (route, payload) in [(1, b"a0"), (2, b"b0"), (1, b"a1"), (2, b"b1")] {
        client.publish(route, b"key", payload).unwrap();
    }
    producer.write_all(&client.into_inner()).await.unwrap();

    expect_frame(&mut first, 1, b"a0").await;
    expect_frame(&mut first, 1, b"a1").await;
    expect_frame(&mut second, 2, b"b0").await;
    expect_frame(&mut second, 2, b"b1").await;
    assert_eq!(filter.dropped_unknown().unwrap(), 0);
    assert_eq!(router.stats.unknown_route(), 0);
}
