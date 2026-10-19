use std::{collections::HashSet, fs, net::SocketAddr, path::Path};

use anyhow::{Context as _, bail, ensure};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub ingest: SocketAddr,
    pub subscribe: SocketAddr,
    /// Frames buffered per route for local subscribers. A subscriber that falls further behind
    /// is disconnected.
    #[serde(default = "default_subscriber_buffer")]
    pub subscriber_buffer: usize,
    /// Frames queued per broker sink of each route. While a queue is full, producers sending to
    /// its route are not read from.
    #[serde(default = "default_sink_buffer")]
    pub sink_buffer: usize,
    /// Required if any route has a Kafka topic.
    pub kafka: Option<Kafka>,
    /// Required if any route has a Redis stream.
    pub redis: Option<Redis>,
    /// Required if any route has a RabbitMQ target.
    pub rabbitmq: Option<RabbitMq>,
    pub routes: Vec<Route>,
}

#[derive(Debug, Deserialize)]
pub struct Kafka {
    /// Comma-separated `host:port` list, passed to librdkafka as `bootstrap.servers`.
    pub brokers: String,
}

#[derive(Debug, Deserialize)]
pub struct Redis {
    /// For example `redis://127.0.0.1:6379`.
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct RabbitMq {
    /// For example `amqp://guest:guest@127.0.0.1:5672/%2f`.
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct Route {
    pub id: u32,
    pub kafka: Option<KafkaRoute>,
    pub redis: Option<RedisRoute>,
    pub rabbitmq: Option<RabbitMqRoute>,
}

#[derive(Debug, Deserialize)]
pub struct KafkaRoute {
    pub topic: String,
}

#[derive(Debug, Deserialize)]
pub struct RedisRoute {
    pub stream: String,
}

#[derive(Debug, Deserialize)]
pub struct RabbitMqRoute {
    /// Defaults to the default exchange, which routes to the queue named by `routing_key`.
    #[serde(default)]
    pub exchange: String,
    pub routing_key: String,
}

fn default_subscriber_buffer() -> usize {
    1024
}

fn default_sink_buffer() -> usize {
    1024
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let text =
            fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        Self::from_toml(&text).with_context(|| format!("parsing {}", path.display()))
    }

    pub fn from_toml(text: &str) -> anyhow::Result<Self> {
        let config: Self = toml::from_str(text)?;
        ensure!(
            config.subscriber_buffer > 0,
            "subscriber_buffer must be positive"
        );
        ensure!(config.sink_buffer > 0, "sink_buffer must be positive");
        let mut seen = HashSet::new();
        for route in &config.routes {
            if !seen.insert(route.id) {
                bail!("duplicate route id {}", route.id);
            }
            ensure!(
                route.kafka.is_none() || config.kafka.is_some(),
                "route {} has a kafka topic but there is no [kafka] section",
                route.id
            );
            ensure!(
                route.redis.is_none() || config.redis.is_some(),
                "route {} has a redis stream but there is no [redis] section",
                route.id
            );
            ensure!(
                route.rabbitmq.is_none() || config.rabbitmq.is_some(),
                "route {} has a rabbitmq target but there is no [rabbitmq] section",
                route.id
            );
        }
        Ok(config)
    }
}
