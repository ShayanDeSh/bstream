pub mod config;
pub mod ingest;
pub mod sinks;

use std::{io, net::SocketAddr, sync::Arc};

use tokio::net::TcpListener;

use crate::{config::Config, ingest::Stats, sinks::local::LocalSink};

pub struct Router {
    ingest: TcpListener,
    subscribe: TcpListener,
    local: Arc<LocalSink>,
    stats: Arc<Stats>,
}

impl Router {
    pub async fn bind(config: &Config) -> io::Result<Self> {
        Ok(Self {
            ingest: TcpListener::bind(config.ingest).await?,
            subscribe: TcpListener::bind(config.subscribe).await?,
            local: Arc::new(LocalSink::new(
                config.routes.iter().map(|route| route.id),
                config.subscriber_buffer,
            )),
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
            ingest::serve(self.ingest, Arc::clone(&self.local), self.stats),
            self.local.serve(self.subscribe),
        )?;
        Ok(())
    }
}
