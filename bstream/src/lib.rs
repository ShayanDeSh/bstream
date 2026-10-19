pub mod config;
pub mod ingest;
pub mod sinks;

use std::{io, net::SocketAddr, sync::Arc};

use anyhow::anyhow;
use lapin::uri::AMQPUri;
use tokio::net::TcpListener;

use crate::{
    config::Config,
    ingest::{Routes, Stats},
    sinks::{
        Sink,
        kafka::{self, KafkaSink},
        local::LocalSink,
        rabbitmq::RabbitMqSink,
        redis::RedisSink,
    },
};

pub struct Router {
    ingest: TcpListener,
    subscribe: TcpListener,
    local: Arc<LocalSink>,
    routes: Arc<Routes>,
    stats: Arc<Stats>,
}

impl Router {
    pub async fn bind(config: &Config) -> anyhow::Result<Self> {
        let local = Arc::new(LocalSink::new(
            config.routes.iter().map(|route| route.id),
            config.subscriber_buffer,
        ));
        let producer = config
            .kafka
            .as_ref()
            .map(|kafka| kafka::producer(&kafka.brokers))
            .transpose()?;
        let redis = config
            .redis
            .as_ref()
            .map(|redis| redis::Client::open(redis.url.as_str()))
            .transpose()?;
        let amqp_uri = config
            .rabbitmq
            .as_ref()
            .map(|rabbitmq| rabbitmq.url.parse::<AMQPUri>())
            .transpose()
            .map_err(|e| anyhow!("rabbitmq url: {e}"))?;
        let routes = config
            .routes
            .iter()
            .map(|route| {
                let mut sinks: Vec<Arc<dyn Sink>> = vec![local.clone()];
                if let (Some(kafka), Some(producer)) = (&route.kafka, &producer) {
                    sinks.push(Arc::new(KafkaSink::new(
                        Arc::clone(producer),
                        kafka.topic.clone(),
                    )));
                }
                if let (Some(redis_route), Some(client)) = (&route.redis, &redis) {
                    sinks.push(Arc::new(RedisSink::new(
                        client.clone(),
                        redis_route.stream.clone(),
                    )));
                }
                if let (Some(rabbitmq_route), Some(uri)) = (&route.rabbitmq, &amqp_uri) {
                    sinks.push(Arc::new(RabbitMqSink::new(
                        uri.clone(),
                        rabbitmq_route.exchange.clone(),
                        rabbitmq_route.routing_key.clone(),
                    )));
                }
                (route.id, sinks)
            })
            .collect();
        Ok(Self {
            ingest: TcpListener::bind(config.ingest).await?,
            subscribe: TcpListener::bind(config.subscribe).await?,
            local,
            routes: Arc::new(routes),
            stats: Arc::default(),
        })
    }

    pub fn ingest_addr(&self) -> io::Result<SocketAddr> {
        self.ingest.local_addr()
    }

    pub fn subscribe_addr(&self) -> io::Result<SocketAddr> {
        self.subscribe.local_addr()
    }

    pub fn stats(&self) -> Arc<Stats> {
        Arc::clone(&self.stats)
    }

    pub async fn run(self) -> anyhow::Result<()> {
        tokio::try_join!(
            ingest::serve(self.ingest, self.routes, self.stats),
            self.local.serve(self.subscribe),
        )?;
        Ok(())
    }
}
