use crate::{
    errors::DbError,
    revision::{Asset, Revision},
};
use async_sqlite::{Client, ClientBuilder, JournalMode};
use rusqlite::{Connection, OptionalExtension, params};
use rusqlite_migration::{M, Migrations};
use std::{path::Path, sync::LazyLock};
use tracing::info;

static MIGRATIONS: LazyLock<Migrations<'static>> = LazyLock::new(|| {
    Migrations::new(vec![
        M::up(
            "
            CREATE TABLE revisions (
                revision_name TEXT NOT NULL PRIMARY KEY,
                number INTEGER NOT NULL
            );

            CREATE TABLE assets (
                revision TEXT NOT NULL REFERENCES revisions(revision_name),
                file_name TEXT NOT NULL,
                tar_file_name TEXT,
                file_type INTEGER NOT NULL,
                size INTEGER NOT NULL,
                crc INTEGER NOT NULL,
                header_crc INTEGER NOT NULL,
                header_size INTEGER NOT NULL,
                compressed_header_size INTEGER NOT NULL,

                origin_revision TEXT NOT NULL REFERENCES revisions(revision_name),

                PRIMARY KEY (revision, file_name)
            );

            CREATE INDEX idx_assets_lookup ON assets (file_name, crc, size);
        ",
        ),
        M::up(
            "
            ALTER TABLE assets ADD COLUMN downloaded INTEGER NOT NULL DEFAULT 0;
            ALTER TABLE revisions ADD COLUMN complete INTEGER NOT NULL DEFAULT 0;
        ",
        ),
    ])
});

const ASSET_COLUMNS: &str = "a.file_name, a.tar_file_name, a.file_type, a.size, a.crc, \
    a.header_crc, a.header_size, a.compressed_header_size, a.origin_revision";

const ORIGIN_JOIN: &str =
    "JOIN assets o ON o.revision = a.origin_revision AND o.file_name = a.file_name";

/// An asset to download, with the revision whose directory holds its file.
pub struct QueuedAsset {
    pub asset: Asset,
    pub origin_revision: String,
}

fn queued_asset_from_row(row: &rusqlite::Row) -> rusqlite::Result<QueuedAsset> {
    Ok(QueuedAsset {
        asset: Asset {
            file_name: row.get(0)?,
            tar_file_name: row.get(1)?,
            file_type: row.get(2)?,
            size: row.get(3)?,
            crc: row.get(4)?,
            header_crc: row.get(5)?,
            header_size: row.get(6)?,
            compressed_header_size: row.get(7)?,
        },
        origin_revision: row.get(8)?,
    })
}

#[derive(Clone)]
pub struct Database {
    client: Client,
}

impl Database {
    pub async fn init(path: &str) -> miette::Result<Self> {
        let client = ClientBuilder::new()
            .path(path)
            .journal_mode(JournalMode::Wal)
            .open()
            .await
            .map_err(DbError::AsyncSqlite)?;

        client
            .conn_mut_and_then(|conn: &mut Connection| -> Result<(), DbError> {
                conn.pragma_update(None, "foreign_keys", true)?;
                MIGRATIONS.to_latest(conn).map_err(DbError::Migration)?;
                Ok(())
            })
            .await?;

        info!("Database {} initialized!", path);

        Ok(Self { client })
    }

    pub async fn list_revisions(&self) -> Result<Vec<String>, DbError> {
        let revisions = self
            .client
            .conn_and_then(|conn| -> Result<Vec<String>, DbError> {
                let mut stmt =
                    conn.prepare("SELECT revision_name FROM revisions ORDER BY number DESC")?;
                let names = stmt
                    .query_map([], |row| row.get(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(names)
            })
            .await?;

        Ok(revisions)
    }

    pub async fn latest_revision(&self) -> Result<Option<Revision>, DbError> {
        let latest_revision = self
            .client
            .conn_and_then(|conn| -> Result<Option<Revision>, DbError> {
                let mut stmt = conn.prepare(
                    "SELECT revision_name, number FROM revisions WHERE complete = 1 ORDER BY number DESC LIMIT 1",
                )?;
                let revision = stmt
                    .query_row([], |row| {
                        Ok(Revision {
                            name: row.get(0)?,
                            number: row.get(1)?,
                        })
                    })
                    .optional()?;
                Ok(revision)
            })
            .await?;

        Ok(latest_revision)
    }

    pub async fn insert_new_revision(
        &self,
        revision: Revision,
        fetched_assets: Vec<Asset>,
    ) -> miette::Result<Vec<QueuedAsset>> {
        let assets_to_download = self
        .client
        .conn_mut_and_then(move |conn: &mut Connection| -> Result<Vec<QueuedAsset>, DbError> {
            let tx = conn.transaction().map_err(DbError::Transaction)?;

            tx.execute(
                "INSERT OR IGNORE INTO revisions (revision_name, number) VALUES (?1, ?2)",
                params![revision.name, revision.number],
            )?;

            let mut stmt_check = tx.prepare(
                "SELECT origin_revision FROM assets WHERE file_name = ?1 AND crc = ?2 AND size = ?3 LIMIT 1"
            )?;
            let mut stmt_insert = tx.prepare(
                "INSERT OR IGNORE INTO assets (
                    revision, file_name, tar_file_name, file_type, size, crc,
                    header_crc, header_size, compressed_header_size, origin_revision
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            )?;

            for asset in fetched_assets {
                let existing_origin: Option<String> = stmt_check
                    .query_row(params![asset.file_name, asset.crc, asset.size], |row| row.get(0))
                    .optional()?;

                let origin_revision = existing_origin.unwrap_or_else(|| revision.name.clone());

                stmt_insert.execute(params![
                    revision.name,
                    asset.file_name,
                    asset.tar_file_name,
                    asset.file_type,
                    asset.size,
                    asset.crc,
                    asset.header_crc,
                    asset.header_size,
                    asset.compressed_header_size,
                    origin_revision
                ])?;
            }

            drop(stmt_check);
            drop(stmt_insert);

            let mut stmt_queue = tx.prepare(&format!(
                "SELECT {ASSET_COLUMNS} FROM assets a {ORIGIN_JOIN} WHERE a.revision = ?1 AND o.downloaded = 0"
            ))?;
            let assets_to_download = stmt_queue
                .query_map(params![revision.name], queued_asset_from_row)?
                .collect::<Result<Vec<_>, _>>()?;

            drop(stmt_queue);
            tx.commit().map_err(DbError::Transaction)?;

            Ok(assets_to_download)
        })
        .await?;

        Ok(assets_to_download)
    }

    /// Resets `downloaded` for every asset of `revision` whose origin file is missing
    /// under `save_dir/<origin_revision>/`, clears `complete` on every revision that
    /// uses one of those files, and returns those assets.
    pub async fn requeue_missing_files(
        &self,
        revision: &str,
        save_dir: &Path,
    ) -> Result<Vec<QueuedAsset>, DbError> {
        let revision = revision.to_owned();
        let save_dir = save_dir.to_owned();
        self.client
            .conn_mut_and_then(move |conn: &mut Connection| -> Result<Vec<QueuedAsset>, DbError> {
                let tx = conn.transaction().map_err(DbError::Transaction)?;

                let mut stmt_downloaded = tx.prepare(&format!(
                    "SELECT {ASSET_COLUMNS} FROM assets a {ORIGIN_JOIN} WHERE a.revision = ?1 AND o.downloaded = 1"
                ))?;
                let missing = stmt_downloaded
                    .query_map(params![revision], queued_asset_from_row)?
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .filter(|queued| {
                        !save_dir
                            .join(&queued.origin_revision)
                            .join(&queued.asset.file_name)
                            .exists()
                    })
                    .collect::<Vec<_>>();

                let mut stmt_reset = tx.prepare(
                    "UPDATE assets SET downloaded = 0 WHERE revision = ?1 AND file_name = ?2",
                )?;
                let mut stmt_incomplete = tx.prepare(
                    "UPDATE revisions SET complete = 0 WHERE revision_name IN (
                        SELECT revision FROM assets WHERE origin_revision = ?1 AND file_name = ?2
                    )",
                )?;
                for queued in &missing {
                    stmt_reset.execute(params![queued.origin_revision, queued.asset.file_name])?;
                    stmt_incomplete
                        .execute(params![queued.origin_revision, queued.asset.file_name])?;
                }

                drop(stmt_downloaded);
                drop(stmt_reset);
                drop(stmt_incomplete);
                tx.commit().map_err(DbError::Transaction)?;

                Ok(missing)
            })
            .await
    }

    pub async fn mark_downloaded(
        &self,
        origin_revision: &str,
        file_name: &str,
    ) -> Result<(), DbError> {
        let origin_revision = origin_revision.to_owned();
        let file_name = file_name.to_owned();
        self.client
            .conn_and_then(move |conn| -> Result<(), DbError> {
                conn.execute(
                    "UPDATE assets SET downloaded = 1 WHERE revision = ?1 AND file_name = ?2",
                    params![origin_revision, file_name],
                )?;
                Ok(())
            })
            .await
    }

    /// Sets `complete` on `revision` only if every one of its assets is downloaded;
    /// returns whether it did.
    pub async fn mark_revision_complete(&self, revision: &str) -> Result<bool, DbError> {
        let revision = revision.to_owned();
        self.client
            .conn_and_then(move |conn| -> Result<bool, DbError> {
                let updated = conn.execute(
                    &format!(
                        "UPDATE revisions SET complete = 1 WHERE revision_name = ?1 AND NOT EXISTS (
                            SELECT 1 FROM assets a {ORIGIN_JOIN} WHERE a.revision = ?1 AND o.downloaded = 0
                        )"
                    ),
                    params![revision],
                )?;
                Ok(updated == 1)
            })
            .await
    }

    pub async fn get_revision_for_asset(
        &self,
        revision_name: String,
        file_name: String,
    ) -> Result<Option<String>, DbError> {
        let result = self
            .client
            .conn_and_then(move |conn| -> Result<Option<String>, DbError> {
                let mut stmt = conn.prepare(
                    "SELECT origin_revision FROM assets WHERE revision = ?1 AND file_name = ?2 LIMIT 1",
                )?;

                let asset_info = stmt
                    .query_row(params![revision_name, file_name], |row| {
                        let origin_revision: String = row.get(0)?;
                        Ok(origin_revision)
                    })
                    .optional()?;

                Ok(asset_info)
            })
            .await?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::{Database, QueuedAsset};
    use crate::revision::{Asset, Revision};
    use rusqlite::Connection;

    const MIGRATION_V1: &str = "
        CREATE TABLE revisions (
            revision_name TEXT NOT NULL PRIMARY KEY,
            number INTEGER NOT NULL
        );

        CREATE TABLE assets (
            revision TEXT NOT NULL REFERENCES revisions(revision_name),
            file_name TEXT NOT NULL,
            tar_file_name TEXT,
            file_type INTEGER NOT NULL,
            size INTEGER NOT NULL,
            crc INTEGER NOT NULL,
            header_crc INTEGER NOT NULL,
            header_size INTEGER NOT NULL,
            compressed_header_size INTEGER NOT NULL,

            origin_revision TEXT NOT NULL REFERENCES revisions(revision_name),

            PRIMARY KEY (revision, file_name)
        );

        CREATE INDEX idx_assets_lookup ON assets (file_name, crc, size);

        PRAGMA user_version = 1;
    ";

    #[tokio::test]
    async fn migration_v1_to_v2_keeps_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("v1.db");
        let conn = Connection::open(&path).expect("open v1");
        conn.execute_batch(MIGRATION_V1).expect("v1 schema");
        conn.execute_batch(
            "INSERT INTO revisions VALUES ('R1', 1);
             INSERT INTO assets VALUES ('R1', 'a.wad', NULL, 1, 10, 20, 30, 40, 50, 'R1');",
        )
        .expect("v1 rows");
        drop(conn);

        Database::init(path.to_str().expect("utf-8 path"))
            .await
            .expect("migrate");

        let conn = Connection::open(&path).expect("open v2");
        let (number, complete): (i64, i64) = conn
            .query_row(
                "SELECT number, complete FROM revisions WHERE revision_name = 'R1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("revision row");
        assert_eq!((number, complete), (1, 0));
        let (crc, downloaded): (i64, i64) = conn
            .query_row(
                "SELECT crc, downloaded FROM assets WHERE file_name = 'a.wad'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("asset row");
        assert_eq!((crc, downloaded), (20, 0));
    }

    fn revision(name: &str, number: i64) -> Revision {
        Revision {
            name: name.to_string(),
            number,
        }
    }

    fn asset(file_name: &str) -> Asset {
        Asset {
            file_name: file_name.to_string(),
            size: 10,
            crc: 20,
            ..Asset::default()
        }
    }

    fn names(queued: &[QueuedAsset]) -> Vec<&str> {
        let mut names: Vec<&str> = queued.iter().map(|q| q.asset.file_name.as_str()).collect();
        names.sort();
        names
    }

    #[tokio::test]
    async fn requeue_missing_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::init(dir.path().join("t.db").to_str().expect("utf-8 path"))
            .await
            .expect("db");
        let manifest = || vec![asset("a.wad"), asset("b.wad")];

        let queued = db
            .insert_new_revision(revision("R1", 1), manifest())
            .await
            .expect("insert R1");
        assert_eq!(names(&queued), ["a.wad", "b.wad"]);
        db.mark_downloaded("R1", "a.wad").await.expect("mark a");
        std::fs::create_dir_all(dir.path().join("R1")).expect("R1 dir");
        std::fs::write(dir.path().join("R1").join("b.wad"), "unverified").expect("b file");

        let queued = db
            .insert_new_revision(revision("R2", 2), manifest())
            .await
            .expect("insert R2");
        assert_eq!(names(&queued), ["b.wad"]);

        let requeued = db
            .requeue_missing_files("R2", dir.path())
            .await
            .expect("requeue");
        assert_eq!(names(&requeued), ["a.wad"]);
        assert!(!db.mark_revision_complete("R2").await.expect("complete"));
    }

    #[tokio::test]
    async fn missing_file_clears_complete() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::init(dir.path().join("t.db").to_str().expect("utf-8 path"))
            .await
            .expect("db");
        db.insert_new_revision(revision("R1", 1), vec![asset("a.wad")])
            .await
            .expect("insert R1");
        db.mark_downloaded("R1", "a.wad").await.expect("mark a");
        assert!(db.mark_revision_complete("R1").await.expect("complete R1"));
        db.insert_new_revision(revision("R2", 2), vec![asset("a.wad")])
            .await
            .expect("insert R2");
        assert!(db.mark_revision_complete("R2").await.expect("complete R2"));

        db.requeue_missing_files("R2", dir.path())
            .await
            .expect("requeue");
        assert!(db.latest_revision().await.expect("latest").is_none());
    }
}
