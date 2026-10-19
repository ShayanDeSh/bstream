use std::env;

use bstream::{Router, config::Config};
use log::info;
use tokio::signal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let path = env::args().nth(1).unwrap_or_else(|| "bstream.toml".into());
    let config = Config::load(&path)?;
    let router = Router::bind(&config).await?;
    info!(
        "producers on {}, subscribers on {}",
        router.ingest_addr()?,
        router.subscribe_addr()?
    );

    tokio::select! {
        result = router.run() => result,
        result = signal::ctrl_c() => Ok(result?),
    }
}
