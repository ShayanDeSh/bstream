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
    pub routes: Vec<Route>,
}

#[derive(Debug, Deserialize)]
pub struct Route {
    pub id: u32,
}

fn default_subscriber_buffer() -> usize {
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
        let mut seen = HashSet::new();
        for route in &config.routes {
            if !seen.insert(route.id) {
                bail!("duplicate route id {}", route.id);
            }
        }
        Ok(config)
    }
}
