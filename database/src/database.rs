use std::time::SystemTime;

use color_eyre::{Result, eyre::Context};
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use metrics::{counter, gauge};
use sqlx::postgres::{PgPoolOptions, PgQueryResult, PgRow, PgStatement, PgTypeInfo};
use sqlx::{Describe, Error as SqlxError, Execute, PgPool, SqlStr, query, query_as};
use sqlx::{Either, Executor, Postgres, Transaction, pool::PoolConnection};

use crate::types::DatabaseScore;

/// This database is effectively write-only
#[derive(Debug)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn new(uri: &str) -> Result<Self> {
        tracing::debug!("Opening Postgres connection pool");
        let pool = PgPoolOptions::new().connect(uri).await?;
        tracing::info!("Postgres connection pool ready");

        Ok(Self { pool })
    }

    pub async fn acquire(&self) -> Result<PoolConnection<Postgres>> {
        tracing::trace!("Acquiring Postgres connection from pool");
        self.pool
            .acquire()
            .await
            .wrap_err("Failed to acquire a Postgres connection")
    }

    pub async fn begin(&self) -> Result<Transaction<'static, Postgres>> {
        tracing::trace!("Starting Postgres transaction");
        self.pool
            .begin()
            .await
            .wrap_err("Failed to start a Postgres transaction")
    }

    pub async fn insert_score(
        &self,
        scores: impl ExactSizeIterator<Item = &DatabaseScore>,
    ) -> Result<()> {
        let batch_length = scores.len();
        let span = tracing::info_span!(target: "insert_score_batch", "Score insertion", batch_size = batch_length);
        tracing::info!(parent: &span, "Inserting a new score batch");

        let mut trans = self.begin().await?;

        for score in scores {
            // no useful data on insertion
            let _ = query!(
                r#"
    INSERT INTO scores
    (id, user_id, ruleset_id, beatmap_id, has_replay, grade, accuracy, max_combo, total_score, classic_total_score, total_score_without_mods, is_perfect_combo, legacy_perfect, pp, legacy_total_score, ended_at, build_id, lazer, data)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                "#,
                score.id as i64,
                score.user_id as i64,
                score.mode as i16,
                score.beatmap_id as i64,
                score.replay,
                score.grade.to_string(),
                score.accuracy,
                score.max_combo as i32,
                score.total_score as i64,
                score.classic_total_score as i64,
                score.total_score_without_mods.map(|s| s as i64),
                score.is_perfect_combo,
                score.legacy_perfect,
                score.pp as f64,
                score.legacy_total_score as i64,
                score.ended_at,
                score.build_id as i16,
                score.lazer,
                serde_json::to_value(&score.data)?,
            ).execute(&mut *trans).await.wrap_err("Failed to insert a result")?;
        }

        trans.commit().await?;
        counter!(description: "Number of scores inserted",
            unit: metrics::Unit::Count,
            "nike.scores_inserted_since_startup")
        .increment(batch_length as u64);

        gauge!(description: "Timestamp of the last score insertion",
            unit: metrics::Unit::Seconds,
            "nike.last_insertion_success_seconds")
        .set(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64(),
        );
        tracing::info!(parent: &span, "Commit: inserted {batch_length} scores");

        Ok(())
    }

    pub async fn get_last_inserted_score(&self) -> Result<Option<i64>> {
        #[derive(sqlx::Type)]
        #[sqlx(transparent)]
        struct ScoreId {
            id: i64,
        }

        let result = query_as!(
            ScoreId,
            r#"SELECT id FROM scores
            WHERE ended_at >= NOW() - INTERVAL '72 hours'
            ORDER BY id DESC LIMIT 1"#
        )
        .fetch_optional(self)
        .await
        .wrap_err("Failed to fetch last score");
        result.map(|r| r.map(|r| r.id))
    }
}

impl<'p> Executor<'p> for &Database {
    type Database = Postgres;

    #[inline]
    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> BoxStream<'e, std::result::Result<Either<PgQueryResult, PgRow>, SqlxError>>
    where
        'p: 'e,
        E: Execute<'q, Self::Database>,
    {
        <&PgPool as Executor<'p>>::fetch_many(&self.pool, query)
    }

    #[inline]
    fn fetch_optional<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<PgRow>, SqlxError>>
    where
        'p: 'e,
        E: Execute<'q, Self::Database>,
    {
        <&PgPool as Executor<'p>>::fetch_optional(&self.pool, query)
    }

    #[inline]
    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [PgTypeInfo],
    ) -> BoxFuture<'e, Result<PgStatement, SqlxError>>
    where
        'p: 'e,
    {
        <&PgPool as Executor<'p>>::prepare_with(&self.pool, sql, parameters)
    }

    #[inline]
    fn describe<'e>(self, sql: SqlStr) -> BoxFuture<'e, Result<Describe<Self::Database>, SqlxError>>
    where
        'p: 'e,
    {
        <&PgPool as Executor<'p>>::describe(&self.pool, sql)
    }
}
