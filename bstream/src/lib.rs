pub mod config;
pub mod ingest;
pub mod sinks;

use std::{io, net::SocketAddr, sync::Arc};

use tokio::net::TcpListener;

use crate::{
    config::Config,
    ingest::{Routes, Stats},
    sinks::{
        Sink,
        kafka::{self, KafkaSink},
        local::LocalSink,
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
