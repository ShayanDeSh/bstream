use std::time::{SystemTime, UNIX_EPOCH};

use redis::aio::MultiplexedConnection;

use super::wait_until;

/// Matches the port in `docker-compose.yml`.
pub const URL: &str = "redis://127.0.0.1:6379";

pub fn unique_stream(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("bstream-{name}-{nanos}")
}

pub async fn connect() -> MultiplexedConnection {
    redis::Client::open(URL)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

pub async fn wait_for_len(conn: &MultiplexedConnection, stream: &str, len: usize) {
    wait_until(&format!("{len} entries in {stream}"), || async {
        let actual: usize = redis::cmd("XLEN")
            .arg(stream)
            .query_async(&mut conn.clone())
            .await
            .unwrap();
        actual >= len
    })
    .await;
}

/// An XRANGE entry: its id and its field/value pairs.
type StreamEntry = (String, Vec<(String, Vec<u8>)>);

/// Returns `(key_hash, payload)` for every entry, oldest first.
pub async fn xrange(conn: &mut MultiplexedConnection, stream: &str) -> Vec<(u32, Vec<u8>)> {
    let entries: Vec<StreamEntry> = redis::cmd("XRANGE")
        .arg(stream)
        .arg("-")
        .arg("+")
        .query_async(conn)
        .await
        .unwrap();
    entries
        .into_iter()
        .map(|(_, fields)| {
            let [(k, key_hash), (p, payload)] = <[_; 2]>::try_from(fields).unwrap();
            assert_eq!((k.as_str(), p.as_str()), ("key_hash", "payload"));
            let key_hash = String::from_utf8(key_hash).unwrap().parse().unwrap();
            (key_hash, payload)
        })
        .collect()
}
