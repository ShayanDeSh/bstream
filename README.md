# bstream

## Prerequisites

1. stable rust toolchains: `rustup toolchain install stable`
1. nightly rust toolchains: `rustup toolchain install nightly --component rust-src`
1. (if cross-compiling) rustup target: `rustup target add ${ARCH}-unknown-linux-musl`
1. (if cross-compiling) LLVM: (e.g.) `brew install llvm` (on macOS)
1. bpf-linker: `cargo install bpf-linker` (`--no-default-features` on macOS)

## Build & Run

Use `cargo build`, `cargo check`, etc. as normal. Run the router with a config file (default
`bstream.toml`):

```shell
cargo run --release -- bstream.toml
```

```toml
ingest = "0.0.0.0:7400"     # producers connect here
subscribe = "0.0.0.0:7401"  # local subscribers connect here
subscriber_buffer = 1024    # optional; frames a subscriber may lag before it is disconnected
sink_buffer = 1024          # optional; frames queued per broker sink of each route
ebpf = false                # optional; filter unknown routes in the kernel (needs CAP_BPF)

[[routes]]
id = 1
kafka = { topic = "orders" }  # optional; also produce this route's payloads to Kafka
redis = { stream = "orders" } # optional; also XADD this route's frames to a Redis stream
rabbitmq = { exchange = "events", routing_key = "orders" } # optional; exchange defaults to ""

[kafka]                       # required if any route has a Kafka topic
brokers = "localhost:9092"

[redis]                       # required if any route has a Redis stream
url = "redis://localhost:6379"

[rabbitmq]                    # required if any route has a RabbitMQ target
url = "amqp://guest:guest@localhost:5672/%2f"
```

Kafka records are keyed by the frame's `key_hash` (4 little-endian bytes), so each key stays on
one partition, in order. Redis stream entries have two fields, `key_hash` (decimal) and
`payload`, appended in ingest order. RabbitMQ messages are persistent and mandatory, carry the
payload as the body and `key_hash` as a header, and count as delivered only once the broker
confirms them.

Each broker sink of a route has its own queue of `sink_buffer` frames. Failed writes are retried
with exponential backoff (50 ms up to 1 s) until they succeed, so delivery is at-least-once; only
frames the broker rejects for good (a Redis error reply, a RabbitMQ nack or return, an oversized
Kafka record) are dropped. While a queue is full the router stops reading from producers on that
route, so TCP slows them down instead of the router buffering without limit. Local subscribers
never block ingest.

A subscriber sends a `u32` little-endian route id and gets one status byte back (`0` ok,
`1` unknown route), followed by every frame for that route.

With `ebpf = true`, an eBPF stream parser and verdict attach to a `SOCKHASH` holding every accepted
producer socket. The parser splits the TCP stream into frames by the header length, and the verdict
drops frames whose `route_id` is not configured, counting them in a kernel map, so unknown routes
never reach user space. Frames for configured routes pass up unchanged and are fanned out as usual.
Loading eBPF needs `CAP_BPF` (in practice, root); when `ebpf = false` (the default) the router runs
unprivileged and filters unknown routes in user space instead.

Cargo build scripts are used to automatically build the eBPF correctly and include it in the
program.

## Tests

Broker integration tests are ignored by default. Start the brokers, then include them:

```shell
docker compose up -d --wait
cargo test -- --include-ignored
```

`tests/outage.rs` stops and restarts the Redis container with `docker compose`.

The eBPF tests in `tests/ebpf.rs` load kernel programs, so they are ignored unless run as root:

```shell
sudo -E cargo test -p bstream --test ebpf -- --include-ignored
```

## License

With the exception of eBPF code, bstream is distributed under the terms
of either the [MIT license] or the [Apache License] (version 2.0), at your
option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.

### eBPF

All eBPF code is distributed under either the terms of the
[GNU General Public License, Version 2] or the [MIT license], at your
option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the GPL-2 license, shall be
dual licensed as above, without any additional terms or conditions.

[Apache license]: LICENSE-APACHE
[MIT license]: LICENSE-MIT
[GNU General Public License, Version 2]: LICENSE-GPL2
