use std::env;

use crate::{database::Database, types::DatabaseScore};
use color_eyre::eyre::Result;
use rosu_v2::model::score::Score;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_tungstenite::tungstenite::Message;

mod database;
pub mod types;

pub async fn initialize_database() -> Result<Database> {
    let database_url = env::var("DATABASE_URL")?;

    Database::new(&database_url).await
}

pub async fn database_loop(database: Database, mut channel: UnboundedReceiver<Message>) {
    let mut scores_list: Vec<DatabaseScore> = Vec::with_capacity(2000);

    while let Some(res) = channel.recv().await {
        match res {
            Message::Binary(data) => {
                let score: Score = serde_json::from_slice(&data).unwrap();

                scores_list.push(Into::<DatabaseScore>::into(score));
            }
            Message::Text(text) => match text.as_str() {
                "start-batch" => {
                    scores_list.clear();
                    tracing::info!("batch start");
                    continue;
                }
                "end-batch" => {
                    tracing::info!("batch end, sizeof: {}", scores_list.len());
                    database.insert_score(scores_list.iter()).await.unwrap();
                    continue;
                }
                _ => todo!(),
            },
            _ => todo!(),
        }
    }
}
