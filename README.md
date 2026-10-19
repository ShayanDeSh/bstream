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

A subscriber sends a `u32` little-endian route id and gets one status byte back (`0` ok,
`1` unknown route), followed by every frame for that route.

Cargo build scripts are used to automatically build the eBPF correctly and include it in the
program.

## Tests

Broker integration tests are ignored by default. Start the brokers, then include them:

```shell
docker compose up -d --wait
cargo test -- --include-ignored
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
