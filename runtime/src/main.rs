use color_eyre::Result;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let database = nike_database::initialize_database().await?;
    tracing::info!("Initialized a database connection");

    let cursor = database.get_last_inserted_score().await? as u64;
    tracing::info!(cursor = cursor, "Latest inserted score id found");

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    tokio::select! {
        _ = nike_socket::init_socket_thread(sender, cursor) => {},
        _ = nike_database::database_loop(database, receiver) => {},

    }
    Ok(())
}
