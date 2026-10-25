use crate::{
    errors::ManifestFetcherError, fetcher::fetcher::Fetcher, revision::Asset,
    wizard_patcher::WizardPatcher, xml_parser::parse_file_list,
};
use reqwest::Client;
use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::fs::try_exists;
use tracing::{debug, info, warn};

/// This struct is responsible for fetching the `LatestFileList.bin` and `LatestFileList.xml` from their servers.
pub struct ManifestFetcher {
    client: Client,
    wizard_patcher: WizardPatcher,
    save_directory: PathBuf,
}

impl ManifestFetcher {
    pub fn new<P>(wizard_patcher: WizardPatcher, save_directory: P) -> miette::Result<Self>
    where
        P: AsRef<Path>,
    {
        let client = Client::builder()
            .user_agent("KingsIsle Patcher")
            .tcp_keepalive(Duration::from_mins(1))
            .timeout(Duration::from_mins(2))
            .build()
            .map_err(ManifestFetcherError::ClientBuild)?;

        Ok(Self {
            client,
            wizard_patcher: wizard_patcher.clone(),
            save_directory: save_directory.as_ref().join(wizard_patcher.revision.name),
        })
    }

    pub async fn fetch_bin_manifest(&self) -> miette::Result<()> {
        let path = self.save_directory.join("LatestFileList.bin");
        let file_exists = try_exists(&path).await.map_err(ManifestFetcherError::Io)?;

        if !file_exists {
            info!("Fetching LatestFileList.bin...");
            let response = Self::fetch(&self.client, &self.wizard_patcher.list_file_url).await?;
            Self::write_to_file_streamed(&path, response, None, None, None).await?;
            return Ok(());
        }

        info!(path = %path.display(), "BIN manifest already cached, skipping download");
        Ok(())
    }

    pub async fn fetch_xml_manifest(&self) -> miette::Result<Vec<Asset>> {
        let path = self.save_directory.join("LatestFileList.xml");
        let file_exists = try_exists(&path).await.map_err(ManifestFetcherError::Io)?;
        let list_file_url = self.wizard_patcher.list_file_url.replace(".bin", ".xml");

        if !file_exists {
            info!("Fetching LatestFileList.xml...");
            let response = Self::fetch(&self.client, &list_file_url).await?;
            Self::write_to_file_streamed(&path, response, None, None, None).await?;
        } else {
            info!(path = %path.display(), "XML manifest already cached, skipping download");
        }

        let assets = match parse_file_list(&path) {
            Ok(assets) => assets,
            Err(e) => {
                Self::remove_cached(&path).await;
                return Err(e);
            }
        };
        debug!("Parsed {} entries from LatestFileList.xml", assets.len());

        if assets.is_empty() {
            Self::remove_cached(&path).await;
            return Err(ManifestFetcherError::EmptyAssetList.into());
        }

        Ok(assets)
    }

    async fn remove_cached(path: &Path) {
        match tokio::fs::remove_file(path).await {
            Err(e) if e.kind() != ErrorKind::NotFound => {
                warn!(error = %e, path = %path.display(), "failed to delete cached manifest");
            }
            _ => {}
        }
    }
}

impl Fetcher for ManifestFetcher {}

#[cfg(test)]
mod tests {
    use super::ManifestFetcher;
    use crate::{
        errors::ManifestFetcherError, fetcher::test_support::respond, revision::Revision,
        wizard_patcher::WizardPatcher,
    };
    use std::path::Path;

    const REVISION: &str = "V_r1.Wizard_1_0";

    fn manifest_fetcher(list_file_url: String, save_directory: &Path) -> ManifestFetcher {
        let wizard_patcher = WizardPatcher {
            list_file_url,
            url_prefix: String::new(),
            revision: Revision {
                name: REVISION.to_string(),
                number: 1,
            },
        };
        ManifestFetcher::new(wizard_patcher, save_directory).expect("manifest fetcher")
    }

    async fn serve_404_once() -> String {
        let server = respond("404 Not Found", b"not found").await;
        format!("{}/{REVISION}/Windows/LatestFileList.bin", server.url)
    }

    #[tokio::test]
    async fn fetch_404_is_error_no_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let fetcher = manifest_fetcher(serve_404_once().await, dir.path());

        assert!(fetcher.fetch_bin_manifest().await.is_err());

        let revision_dir = dir.path().join(REVISION);
        assert!(!revision_dir.join("LatestFileList.bin").exists());
        assert!(!revision_dir.join("LatestFileList.bin.part").exists());
    }

    #[tokio::test]
    async fn bad_manifest_deleted_and_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let revision_dir = dir.path().join(REVISION);
        std::fs::create_dir_all(&revision_dir).expect("revision dir");
        let manifest = revision_dir.join("LatestFileList.xml");
        std::fs::write(
            &manifest,
            "<LatestFileList><RECORD><SrcFileName>a.wad</SrcFileName></LatestFileList>",
        )
        .expect("write manifest");
        let fetcher = manifest_fetcher(
            format!("http://127.0.0.1:9/{REVISION}/Windows/LatestFileList.bin"),
            dir.path(),
        );

        assert!(fetcher.fetch_xml_manifest().await.is_err());
        assert!(!manifest.exists());
    }

    #[tokio::test]
    async fn empty_manifest_deleted_and_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let revision_dir = dir.path().join(REVISION);
        std::fs::create_dir_all(&revision_dir).expect("revision dir");
        let manifest = revision_dir.join("LatestFileList.xml");
        std::fs::write(&manifest, "<LatestFileList></LatestFileList>").expect("write manifest");
        let fetcher = manifest_fetcher(
            format!("http://127.0.0.1:9/{REVISION}/Windows/LatestFileList.bin"),
            dir.path(),
        );

        let err = fetcher
            .fetch_xml_manifest()
            .await
            .expect_err("empty manifest");
        assert!(matches!(
            err.downcast_ref::<ManifestFetcherError>(),
            Some(ManifestFetcherError::EmptyAssetList)
        ));
        assert!(!manifest.exists());
    }
}
