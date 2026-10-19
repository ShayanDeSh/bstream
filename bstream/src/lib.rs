pub mod config;
pub mod ebpf;
pub mod ingest;
pub mod sinks;

use std::{collections::HashMap, io, net::SocketAddr, sync::Arc};

use anyhow::anyhow;
use lapin::uri::AMQPUri;
use tokio::net::TcpListener;

use crate::{
    config::Config,
    ebpf::KernelFilter,
    ingest::{Routes, Stats},
    sinks::{
        Queue, SinkKind,
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
    filter: Option<Arc<KernelFilter>>,
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
        let mut metrics = HashMap::new();
        let routes = config
            .routes
            .iter()
            .map(|route| {
                let name = |target: String| format!("route {} {target}", route.id);
                let mut queues = Vec::new();
                if let (Some(kafka), Some(producer)) = (&route.kafka, &producer) {
                    let sink = KafkaSink::new(producer.clone(), kafka.topic.clone());
                    let name = name(format!("kafka topic {:?}", kafka.topic));
                    queues.push((
                        SinkKind::Kafka,
                        Queue::spawn(name, sink, config.sink_buffer),
                    ));
                }
                if let (Some(redis_route), Some(client)) = (&route.redis, &redis) {
                    let sink = RedisSink::new(client.clone(), redis_route.stream.clone());
                    let name = name(format!("redis stream {:?}", redis_route.stream));
                    queues.push((
                        SinkKind::Redis,
                        Queue::spawn(name, sink, config.sink_buffer),
                    ));
                }
                if let (Some(rabbitmq_route), Some(uri)) = (&route.rabbitmq, &amqp_uri) {
                    let sink = RabbitMqSink::new(
                        uri.clone(),
                        rabbitmq_route.exchange.clone(),
                        rabbitmq_route.routing_key.clone(),
                    );
                    let name = name(format!(
                        "rabbitmq exchange {:?} key {:?}",
                        rabbitmq_route.exchange, rabbitmq_route.routing_key
                    ));
                    queues.push((
                        SinkKind::RabbitMq,
                        Queue::spawn(name, sink, config.sink_buffer),
                    ));
                }
                for (kind, queue) in &queues {
                    metrics.insert((route.id, *kind), queue.metrics());
                }
                (
                    route.id,
                    queues.into_iter().map(|(_, queue)| queue).collect(),
                )
            })
            .collect();
        let filter = config
            .ebpf
            .then(|| KernelFilter::load(config.routes.iter().map(|route| route.id)).map(Arc::new))
            .transpose()?;
        Ok(Self {
            ingest: TcpListener::bind(config.ingest).await?,
            subscribe: TcpListener::bind(config.subscribe).await?,
            local,
            routes: Arc::new(routes),
            stats: Arc::new(Stats::new(metrics)),
            filter,
        })
    }

    pub fn filter(&self) -> Option<Arc<KernelFilter>> {
        self.filter.clone()
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
            ingest::serve(
                self.ingest,
                Arc::clone(&self.local),
                self.routes,
                self.stats,
                self.filter,
            ),
            self.local.serve(self.subscribe),
        )?;
        Ok(())
    }
}
