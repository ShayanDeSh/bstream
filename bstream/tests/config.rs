use std::fs;

use bstream::config::Config;

/// Given a config with addresses and two routes and no buffer sizes,
/// when it is parsed,
/// then the addresses and route ids are read and both buffers default to 1024.
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
    assert_eq!(config.sink_buffer, 1024);
}

/// Given a config that sets subscriber_buffer and sink_buffer and has no routes,
/// when it is parsed,
/// then the explicit buffers are used and the route list is empty.
#[test]
fn explicit_buffers_and_empty_routes() {
    let config = Config::from_toml(
        r#"
        ingest = "127.0.0.1:0"
        subscribe = "127.0.0.1:0"
        subscriber_buffer = 8
        sink_buffer = 16
        routes = []
        "#,
    )
    .unwrap();

    assert_eq!(config.subscriber_buffer, 8);
    assert_eq!(config.sink_buffer, 16);
    assert!(config.routes.is_empty());
}

/// Given a config with a [kafka] section and one of two routes naming a topic,
/// when it is parsed,
/// then the brokers are read and only that route has a Kafka topic.
#[test]
fn parses_kafka_brokers_and_route_topic() {
    let config = Config::from_toml(
        r#"
        ingest = "127.0.0.1:0"
        subscribe = "127.0.0.1:0"
        routes = [{ id = 1, kafka = { topic = "orders" } }, { id = 2 }]

        [kafka]
        brokers = "a:9092,b:9092"
        "#,
    )
    .unwrap();

    assert_eq!(config.kafka.unwrap().brokers, "a:9092,b:9092");
    assert_eq!(config.routes[0].kafka.as_ref().unwrap().topic, "orders");
    assert!(config.routes[1].kafka.is_none());
}

/// Given a route with a Kafka topic and no [kafka] section,
/// when it is parsed,
/// then parsing fails naming the route.
#[test]
fn kafka_route_without_kafka_section_is_rejected() {
    let err = Config::from_toml(
        r#"
        ingest = "127.0.0.1:0"
        subscribe = "127.0.0.1:0"
        routes = [{ id = 4, kafka = { topic = "orders" } }]
        "#,
    )
    .unwrap_err();

    assert!(err.to_string().contains("route 4"), "{err}");
}

/// Given a [kafka] section without brokers,
/// when it is parsed,
/// then parsing fails.
#[test]
fn kafka_section_without_brokers_is_rejected() {
    let text = "ingest = \"127.0.0.1:0\"\nsubscribe = \"127.0.0.1:0\"\nroutes = []\n[kafka]";

    assert!(Config::from_toml(text).is_err());
}

/// Given a config with a [redis] section and one of two routes naming a stream,
/// when it is parsed,
/// then the URL is read and only that route has a Redis stream.
#[test]
fn parses_redis_url_and_route_stream() {
    let config = Config::from_toml(
        r#"
        ingest = "127.0.0.1:0"
        subscribe = "127.0.0.1:0"
        routes = [{ id = 1, redis = { stream = "orders" } }, { id = 2 }]

        [redis]
        url = "redis://cache:6379"
        "#,
    )
    .unwrap();

    assert_eq!(config.redis.unwrap().url, "redis://cache:6379");
    assert_eq!(config.routes[0].redis.as_ref().unwrap().stream, "orders");
    assert!(config.routes[1].redis.is_none());
}

/// Given a route with a Redis stream and no [redis] section,
/// when it is parsed,
/// then parsing fails naming the route.
#[test]
fn redis_route_without_redis_section_is_rejected() {
    let err = Config::from_toml(
        r#"
        ingest = "127.0.0.1:0"
        subscribe = "127.0.0.1:0"
        routes = [{ id = 5, redis = { stream = "orders" } }]
        "#,
    )
    .unwrap_err();

    assert!(err.to_string().contains("route 5"), "{err}");
}

/// Given a [redis] section without a URL,
/// when it is parsed,
/// then parsing fails.
#[test]
fn redis_section_without_url_is_rejected() {
    let text = "ingest = \"127.0.0.1:0\"\nsubscribe = \"127.0.0.1:0\"\nroutes = []\n[redis]";

    assert!(Config::from_toml(text).is_err());
}

/// Given a config with a [rabbitmq] section, one route naming an exchange and routing key, and
/// one naming only a routing key,
/// when it is parsed,
/// then the URL is read and the route without an exchange uses the default exchange "".
#[test]
fn parses_rabbitmq_url_and_route_target() {
    let config = Config::from_toml(
        r#"
        ingest = "127.0.0.1:0"
        subscribe = "127.0.0.1:0"
        routes = [
            { id = 1, rabbitmq = { exchange = "events", routing_key = "orders" } },
            { id = 2, rabbitmq = { routing_key = "audit" } },
        ]

        [rabbitmq]
        url = "amqp://mq:5672/%2f"
        "#,
    )
    .unwrap();

    assert_eq!(config.rabbitmq.unwrap().url, "amqp://mq:5672/%2f");
    let targets: Vec<_> = config
        .routes
        .iter()
        .map(|route| {
            let target = route.rabbitmq.as_ref().unwrap();
            (target.exchange.as_str(), target.routing_key.as_str())
        })
        .collect();
    assert_eq!(targets, [("events", "orders"), ("", "audit")]);
}

/// Given a route with a RabbitMQ target and no [rabbitmq] section,
/// when it is parsed,
/// then parsing fails naming the route.
#[test]
fn rabbitmq_route_without_rabbitmq_section_is_rejected() {
    let err = Config::from_toml(
        r#"
        ingest = "127.0.0.1:0"
        subscribe = "127.0.0.1:0"
        routes = [{ id = 6, rabbitmq = { routing_key = "orders" } }]
        "#,
    )
    .unwrap_err();

    assert!(err.to_string().contains("route 6"), "{err}");
}

/// Given a route with a RabbitMQ target but no routing key,
/// when it is parsed,
/// then parsing fails.
#[test]
fn rabbitmq_route_without_routing_key_is_rejected() {
    let text = "ingest = \"127.0.0.1:0\"\nsubscribe = \"127.0.0.1:0\"\n\
                routes = [{ id = 1, rabbitmq = { exchange = \"events\" } }]\n\
                [rabbitmq]\nurl = \"amqp://mq\"";

    assert!(Config::from_toml(text).is_err());
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

/// Given a config with sink_buffer = 0,
/// when it is parsed,
/// then parsing fails, since an mpsc channel cannot have zero capacity.
#[test]
fn zero_sink_buffer_is_rejected() {
    let err = Config::from_toml(
        r#"
        ingest = "127.0.0.1:0"
        subscribe = "127.0.0.1:0"
        sink_buffer = 0
        routes = []
        "#,
    )
    .unwrap_err();

    assert!(err.to_string().contains("sink_buffer"), "{err}");
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
