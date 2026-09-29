use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use color_eyre::{Result, eyre::Context};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    prometheus()?;

    let database = nike_database::initialize_database().await?;
    tracing::info!("Initialized a database connection");

    let cursor = database.get_last_inserted_score().await?.map(|v| v as u64);
    tracing::info!(cursor = cursor, "Latest inserted score id found");

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    tokio::select! {
        _ = nike_socket::init_socket_thread(sender, cursor) => {},
        _ = nike_database::database_loop(database, receiver) => {},

    }
    Ok(())
}

fn prometheus() -> Result<()> {
    let sock = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 10101);
    let builder = PrometheusBuilder::new().with_http_listener(sock);
    builder.install().wrap_err("Failed to install Prometheus")?;
    tracing::info!(ip = ?sock, "Installed Prometheus");
    Ok(())
}
