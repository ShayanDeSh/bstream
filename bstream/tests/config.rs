use std::fs;

use bstream::config::Config;

/// Given a config with addresses and two routes and no subscriber_buffer,
/// when it is parsed,
/// then the addresses and route ids are read and the buffer defaults to 1024.
#[test]
fn parses_routes_and_defaults_buffer() {
    let config = Config::from_toml(
        r#"
        ingest = "127.0.0.1:7400"
        subscribe = "0.0.0.0:7401"

        [[routes]]
        id = 1

        [[routes]]
        id = 2
        "#,
    )
    .unwrap();

    assert_eq!(config.ingest, "127.0.0.1:7400".parse().unwrap());
    assert_eq!(config.subscribe, "0.0.0.0:7401".parse().unwrap());
    let ids: Vec<_> = config.routes.iter().map(|r| r.id).collect();
    assert_eq!(ids, [1, 2]);
    assert_eq!(config.subscriber_buffer, 1024);
}

/// Given a config that sets subscriber_buffer and has no routes,
/// when it is parsed,
/// then the explicit buffer is used and the route list is empty.
#[test]
fn explicit_buffer_and_empty_routes() {
    let config = Config::from_toml(
        r#"
        ingest = "127.0.0.1:0"
        subscribe = "127.0.0.1:0"
        subscriber_buffer = 8
        routes = []
        "#,
    )
    .unwrap();

    assert_eq!(config.subscriber_buffer, 8);
    assert!(config.routes.is_empty());
}

/// Given a config listing the same route id twice,
/// when it is parsed,
/// then parsing fails naming the duplicate id.
#[test]
fn duplicate_route_ids_are_rejected() {
    let err = Config::from_toml(
        r#"
        ingest = "127.0.0.1:0"
        subscribe = "127.0.0.1:0"
        routes = [{ id = 3 }, { id = 3 }]
        "#,
    )
    .unwrap_err();

    assert!(err.to_string().contains("duplicate route id 3"), "{err}");
}

/// Given a config with subscriber_buffer = 0,
/// when it is parsed,
/// then parsing fails, since a broadcast channel cannot have zero capacity.
#[test]
fn zero_subscriber_buffer_is_rejected() {
    let err = Config::from_toml(
        r#"
        ingest = "127.0.0.1:0"
        subscribe = "127.0.0.1:0"
        subscriber_buffer = 0
        routes = []
        "#,
    )
    .unwrap_err();

    assert!(err.to_string().contains("subscriber_buffer"), "{err}");
}

/// Given a config missing the ingest address,
/// when it is parsed,
/// then parsing fails.
#[test]
fn missing_ingest_address_is_rejected() {
    assert!(Config::from_toml("subscribe = \"127.0.0.1:0\"\nroutes = []").is_err());
}

/// Given a config whose ingest address is not a socket address,
/// when it is parsed,
/// then parsing fails.
#[test]
fn malformed_address_is_rejected() {
    let text = "ingest = \"localhost\"\nsubscribe = \"127.0.0.1:0\"\nroutes = []";

    assert!(Config::from_toml(text).is_err());
}

/// Given a config file on disk,
/// when it is loaded,
/// then its contents are parsed.
#[test]
fn load_reads_file() {
    let path = format!("{}/load_reads_file.toml", env!("CARGO_TARGET_TMPDIR"));
    fs::write(
        &path,
        "ingest = \"127.0.0.1:0\"\nsubscribe = \"127.0.0.1:0\"\nroutes = [{ id = 9 }]",
    )
    .unwrap();

    let config = Config::load(&path).unwrap();

    assert_eq!(config.routes[0].id, 9);
}

/// Given a path with no file,
/// when it is loaded,
/// then loading fails with an error naming the path.
#[test]
fn load_missing_file_fails() {
    let path = format!("{}/does-not-exist.toml", env!("CARGO_TARGET_TMPDIR"));

    let err = Config::load(&path).unwrap_err();

    assert!(err.to_string().contains("does-not-exist.toml"), "{err}");
}
