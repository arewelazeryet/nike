mod config;
mod context;
mod event;
mod osu;

use std::{net::SocketAddr, sync::Arc};

use crate::{config::Config, context::Context, osu::Osu};
use color_eyre::{Result, eyre::Context as _};
use tokio::{net::TcpListener, sync::mpsc::UnboundedSender};
use tokio_tungstenite::tungstenite::Message;
use tracing::info;

pub async fn init_socket_thread(
    channel: UnboundedSender<Message>,
    resume_id: Option<u64>,
) -> Result<()> {
    let Config { setup, osu } = Config::parse();

    let osu = Osu::new(osu).wrap_err("Failed to create osu! client")?;
    let ctx = Arc::new(Context::new(&setup, channel));

    let addr = SocketAddr::new(setup.ip_addr, setup.port);
    let listener = TcpListener::bind(addr).await.unwrap();
    info!("Listening on {addr}...");

    tokio::spawn(Context::fetch_scores(
        Arc::clone(&ctx),
        osu,
        setup.interval,
        resume_id.into(),
    ));

    while let Ok(conn) = listener.accept().await {
        tokio::spawn(Context::handle_connection(Arc::clone(&ctx), conn));
    }

    Ok(())
}
