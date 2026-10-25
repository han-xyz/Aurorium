use crate::{
    crc::Crc32,
    db::{Database, QueuedAsset},
    errors::AssetFetcherError,
    fetcher::fetcher::Fetcher,
    revision::Asset,
    wizard_patcher::WizardPatcher,
};
use futures_util::{StreamExt, stream};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::Client;
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::LazyLock,
    time::Duration,
};
use tokio::io::AsyncReadExt;
use tracing::{debug, info, instrument, trace, warn};

static MAIN_PROGRESS_STYLE: LazyLock<ProgressStyle> = LazyLock::new(|| {
    ProgressStyle::with_template(
        "{spinner:.yellow} [{pos}/{len}] [{wide_bar:.cyan/blue}] ({eta_precise}/{elapsed_precise})",
    )
    .expect("Failed to create progress bar style")
});

static FILE_PROGRESS_STYLE: LazyLock<ProgressStyle> = LazyLock::new(|| {
    ProgressStyle::with_template("{msg:.cyan} [{wide_bar:.cyan/blue}] {bytes}/{total_bytes}")
        .expect("Failed to create progress bar style")
});

pub struct AssetFetcher<'a> {
    client: Client,
    concurrent_downloads: &'a NonZeroUsize,
    wizard_patcher: WizardPatcher,
    save_directory: PathBuf,
    assets: Vec<QueuedAsset>,
    db: Database,
    verify_crc: bool,
}

impl<'a> AssetFetcher<'a> {
    pub fn new<P>(
        wizard_patcher: WizardPatcher,
        concurrent_downloads: &'a NonZeroUsize,
        save_directory: P,
        assets: Vec<QueuedAsset>,
        db: Database,
        verify_crc: bool,
    ) -> miette::Result<Self>
    where
        P: AsRef<Path>,
    {
        let client = Client::builder()
            .user_agent("KingsIsle Patcher")
            .pool_max_idle_per_host(concurrent_downloads.get())
            .tcp_keepalive(Duration::from_mins(1))
            .timeout(Duration::from_mins(2))
            .build()
            .map_err(AssetFetcherError::ClientBuild)?;

        Ok(AssetFetcher {
            client,
            save_directory: save_directory.as_ref().to_path_buf(),
            wizard_patcher,
            concurrent_downloads,
            assets,
            db,
            verify_crc,
        })
    }

    #[instrument(skip(self))]
    pub async fn fetch_assets(&self) -> miette::Result<()> {
        if self.assets.is_empty() {
            info!(
                "No assets to fetch for revision {}",
                self.wizard_patcher.revision
            );
            return Ok(());
        }

        debug!(
            "Starting download of {} assets with {} concurrent downloads",
            self.assets.len(),
            self.concurrent_downloads
        );

        let multi_progress = MultiProgress::new();
        let main_progress = multi_progress.add(ProgressBar::new(self.assets.len() as u64));
        main_progress.set_style(MAIN_PROGRESS_STYLE.clone());
        main_progress.enable_steady_tick(Duration::from_millis(200));

        let downloads = self.assets.iter().map(|queued| {
            let client = self.client.clone();
            let url_prefix = self.wizard_patcher.url_prefix.clone();
            let save_dir = self.save_directory.clone();
            let db = self.db.clone();
            let verify_crc = self.verify_crc;

            let multi_progress = multi_progress.clone();
            let main_progress = main_progress.clone();

            async move {
                let file = &queued.asset;
                let url = format!("{url_prefix}/{}", file.file_name);
                let save_path = save_dir.join(&queued.origin_revision).join(&file.file_name);

                if Self::is_accepted_on_disk(&save_path, file, verify_crc).await {
                    trace!(file = %file.file_name, "already on disk, skipping download");
                    Self::record_downloaded(&db, queued).await;
                    main_progress.inc(1);
                    return;
                }

                trace!(url = %url, file = %file.file_name, "starting download");

                // Download the file and write it to disk
                match client.get(&url).send().await {
                    Ok(res) => {
                        if !res.status().is_success() {
                            warn!(response=res.status().as_u16(), file = %file.file_name, "failed to download asset");
                            main_progress.inc(1);
                            return;
                        }

                        let file_progress = multi_progress.add(ProgressBar::new_spinner());
                        let short_filename = file.file_name.rsplit('/').next().unwrap_or(&file.file_name);
                        file_progress.set_style(FILE_PROGRESS_STYLE.clone());
                        file_progress.set_message(short_filename.to_string());
                        file_progress.set_length(res.content_length().unwrap_or(file.size.into()));

                        let expected_crc = verify_crc.then_some(file.crc);
                        match Self::write_to_file_streamed(&save_path, res, Some(&file_progress), Some(file.size.into()), expected_crc).await {
                            Ok(crc) => {
                                file_progress.finish_with_message("Done");
                                if crc != file.crc {
                                    Self::warn_crc_mismatch(file, crc);
                                }
                                Self::record_downloaded(&db, queued).await;
                            }
                            Err(e) => {
                                warn!(error = %e, file = %file.file_name, "failed to write file to disk");
                            }
                        }

                        multi_progress.remove(&file_progress);
                    },
                    Err(e) => {
                        // TODO: Handle retries (or log failures in a separate list)
                        warn!(error = %e, file = %file.file_name, "failed to download file");
                    }
                }

                main_progress.inc(1);
            }
        });

        stream::iter(downloads)
            .buffer_unordered(self.concurrent_downloads.get())
            .collect::<Vec<()>>()
            .await;

        multi_progress.clear().unwrap();
        info!("All downloads completed");

        Ok(())
    }

    /// A file left on disk with the manifest size counts as downloaded; a CRC mismatch
    /// rejects it only when `verify_crc` is set.
    async fn is_accepted_on_disk(path: &Path, file: &Asset, verify_crc: bool) -> bool {
        let Ok(metadata) = tokio::fs::metadata(path).await else {
            return false;
        };
        if metadata.len() != u64::from(file.size) {
            return false;
        }
        let crc = match Self::file_crc(path).await {
            Ok(crc) => crc,
            Err(e) => {
                warn!(error = %e, file = %file.file_name, "failed to read file on disk");
                return false;
            }
        };
        if crc == file.crc {
            return true;
        }
        Self::warn_crc_mismatch(file, crc);
        !verify_crc
    }

    async fn file_crc(path: &Path) -> std::io::Result<u32> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut buffer = vec![0u8; 128 * 1024];
        let mut crc = Crc32::default();
        loop {
            let read = file.read(&mut buffer).await?;
            if read == 0 {
                return Ok(crc.value());
            }
            crc.update(&buffer[..read]);
        }
    }

    fn warn_crc_mismatch(file: &Asset, actual: u32) {
        warn!(
            file = %file.file_name,
            expected = file.crc,
            actual,
            "CRC does not match the manifest"
        );
    }

    async fn record_downloaded(db: &Database, queued: &QueuedAsset) {
        if let Err(e) = db
            .mark_downloaded(&queued.origin_revision, &queued.asset.file_name)
            .await
        {
            warn!(error = %e, file = %queued.asset.file_name, "failed to mark asset as downloaded");
        }
    }
}

impl Fetcher for AssetFetcher<'_> {}

#[cfg(test)]
mod tests {
    use super::AssetFetcher;
    use crate::{
        db::Database,
        revision::{Asset, Revision},
        wizard_patcher::WizardPatcher,
    };
    use std::{
        num::NonZeroUsize,
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    const REVISION: &str = "V_r1.Wizard_1_0";
    const BODY: &[u8] = b"123456789";
    const BODY_CRC: u32 = 0x2dfd_2d88;

    async fn serve_body(requests: Arc<AtomicUsize>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.expect("accept");
                requests.fetch_add(1, Ordering::SeqCst);
                let mut request = [0u8; 1024];
                let _ = socket.read(&mut request).await;
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    BODY.len()
                );
                socket.write_all(head.as_bytes()).await.expect("write head");
                socket.write_all(BODY).await.expect("write body");
            }
        });
        format!("http://{addr}")
    }

    async fn fetch(root: &Path, crc: u32, verify_crc: bool) -> (Database, usize) {
        let requests = Arc::new(AtomicUsize::new(0));
        let url_prefix = serve_body(requests.clone()).await;
        let db = Database::init(root.join("test.db").to_str().expect("utf-8 path"))
            .await
            .expect("db");
        let revision = Revision {
            name: REVISION.to_string(),
            number: 1,
        };
        let asset = Asset {
            file_name: "a.wad".to_string(),
            size: BODY.len() as u32,
            crc,
            ..Asset::default()
        };
        let queued = db
            .insert_new_revision(revision.clone(), vec![asset])
            .await
            .expect("insert");
        let wizard_patcher = WizardPatcher {
            list_file_url: String::new(),
            url_prefix,
            revision,
        };

        AssetFetcher::new(
            wizard_patcher,
            &NonZeroUsize::MIN,
            root,
            queued,
            db.clone(),
            verify_crc,
        )
        .expect("asset fetcher")
        .fetch_assets()
        .await
        .expect("fetch");

        (db, requests.load(Ordering::SeqCst))
    }

    #[tokio::test]
    async fn crc_mismatch_kept_unless_verify_crc() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (db, _) = fetch(dir.path(), BODY_CRC ^ 1, false).await;
        assert!(dir.path().join(REVISION).join("a.wad").exists());
        assert!(db.mark_revision_complete(REVISION).await.expect("complete"));

        let dir = tempfile::tempdir().expect("tempdir");
        let (db, _) = fetch(dir.path(), BODY_CRC ^ 1, true).await;
        assert!(!dir.path().join(REVISION).join("a.wad").exists());
        assert!(!db.mark_revision_complete(REVISION).await.expect("complete"));
    }

    #[tokio::test]
    async fn existing_file_with_right_size_marked_without_fetch() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(dir.path().join(REVISION)).expect("revision dir");
        std::fs::write(dir.path().join(REVISION).join("a.wad"), BODY).expect("file");

        let (db, requests) = fetch(dir.path(), BODY_CRC, false).await;

        assert_eq!(requests, 0);
        assert!(db.mark_revision_complete(REVISION).await.expect("complete"));
    }
}
