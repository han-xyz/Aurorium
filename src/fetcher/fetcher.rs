use crate::{crc::Crc32, errors::FetcherTraitError, wizard_patcher::WizardPatcher};
use indicatif::ProgressBar;
use reqwest::{Client, Response, Url, redirect};
use std::path::{Path, PathBuf};
use tokio::{
    fs::create_dir_all,
    io::{AsyncWriteExt, BufWriter},
};
use tracing::warn;

const MAX_REDIRECTS: usize = 3;

/// Outbound allow-list: the configured patch host plus the hosts the patch server named,
/// all under the scheme of the manifest URL.
#[derive(Debug, Clone)]
pub struct UrlPolicy {
    scheme: String,
    hosts: Vec<String>,
}

impl UrlPolicy {
    pub fn new(wizard_patcher: &WizardPatcher) -> Self {
        let list_url = Url::parse(&wizard_patcher.list_file_url).ok();
        let prefix_url = Url::parse(&wizard_patcher.url_prefix).ok();
        let hosts = [list_url.as_ref(), prefix_url.as_ref()]
            .into_iter()
            .flatten()
            .filter_map(|url| url.host_str())
            .chain([wizard_patcher.patch_host.as_str()])
            .map(str::to_ascii_lowercase)
            .collect();

        Self {
            scheme: list_url
                .map(|url| url.scheme().to_string())
                .unwrap_or_default(),
            hosts,
        }
    }

    pub fn check(&self, url: &str) -> Result<(), FetcherTraitError> {
        match Url::parse(url) {
            Ok(parsed) if self.allows(&parsed) => Ok(()),
            _ => {
                warn!(
                    url,
                    "refusing to fetch from a host or scheme outside the allow-list"
                );
                Err(FetcherTraitError::UrlRefused(url.to_string()))
            }
        }
    }

    pub fn redirect(&self) -> redirect::Policy {
        let policy = self.clone();
        redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() > MAX_REDIRECTS {
                warn!(url = %attempt.url(), "refusing redirect: too many redirects");
                return attempt.error("too many redirects");
            }
            if !policy.allows(attempt.url()) {
                warn!(url = %attempt.url(), "refusing redirect outside the allow-list");
                return attempt.error("redirect target is not allowed");
            }
            attempt.follow()
        })
    }

    fn allows(&self, url: &Url) -> bool {
        url.scheme() == self.scheme
            && url.host_str().is_some_and(|host| {
                self.hosts
                    .iter()
                    .any(|allowed| allowed.eq_ignore_ascii_case(host))
            })
    }
}

pub trait Fetcher {
    async fn fetch(client: &Client, url: &str) -> miette::Result<Response> {
        Ok(client
            .get(url)
            .send()
            .await
            .and_then(Response::error_for_status)
            .map_err(|e| FetcherTraitError::Fetch(e, url.to_string()))?)
    }

    /// Streams an HTTP response to disk, optionally driving a progress bar.
    /// When `expected_size` or `expected_crc` (KingsIsle CRC-32, see [`Crc32`]) is given
    /// and the body does not match, the partial file is deleted and an error is returned.
    /// Returns the CRC of the written body.
    ///
    /// # Panics
    /// This function will panic if any of the following conditions are met:
    /// - The file path is invalid.
    /// - The response body cannot be read.
    /// - The file cannot be created or written to.
    /// - The file cannot be renamed to its final name after writing.
    ///
    /// # TODO
    /// Implement resuming downloads by checking for a .part file and continuing from where it left off. (Their server supports range requests, so this should be possible.)
    async fn write_to_file_streamed<P>(
        path: P,
        mut response: reqwest::Response,
        progress: Option<&ProgressBar>,
        expected_size: Option<u64>,
        expected_crc: Option<u32>,
    ) -> miette::Result<u32>
    where
        P: AsRef<Path>,
    {
        let final_path = path.as_ref();
        let part_path = Self::part_path(final_path);

        // Check if parent dir exists, else create it
        if let Some(parent) = final_path.parent() {
            create_dir_all(parent)
                .await
                .map_err(FetcherTraitError::CreateDir)?;
        }

        let file = tokio::fs::File::create(&part_path)
            .await
            .map_err(FetcherTraitError::Io)?;
        let mut writer = BufWriter::with_capacity(128 * 1024, file); // TODO: Let the user configure this buffer size(?)

        // Stream response to file in chunks (to avoid loading the entire file into memory)
        let result: miette::Result<u32> = async {
            let mut size = 0u64;
            let mut crc = Crc32::default();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|e| miette::miette!("Failed to read chunk: {e}"))?
            {
                writer
                    .write_all(&chunk)
                    .await
                    .map_err(FetcherTraitError::Io)?;
                size += chunk.len() as u64;
                crc.update(&chunk);

                if let Some(pb) = progress {
                    pb.inc(chunk.len() as u64);
                }
            }

            writer.flush().await.map_err(FetcherTraitError::Io)?;

            if let Some(expected) = expected_size.filter(|&expected| expected != size) {
                return Err(FetcherTraitError::SizeMismatch {
                    expected,
                    actual: size,
                }
                .into());
            }
            let crc = crc.value();
            if let Some(expected) = expected_crc.filter(|&expected| expected != crc) {
                return Err(FetcherTraitError::CrcMismatch {
                    expected,
                    actual: crc,
                }
                .into());
            }
            Ok(crc)
        }
        .await;

        // If there was an error during the download, remove the partial file and return the error
        let crc = match result {
            Ok(crc) => crc,
            Err(e) => {
                let _ = tokio::fs::remove_file(&part_path).await;
                return Err(e);
            }
        };

        // Rename the .part file to the final filename
        tokio::fs::rename(&part_path, final_path)
            .await
            .map_err(FetcherTraitError::Rename)?;

        Ok(crc)
    }

    fn part_path(path: &Path) -> PathBuf {
        let mut part_os = path.as_os_str().to_owned();
        part_os.push(".part");
        PathBuf::from(part_os)
    }
}

#[cfg(test)]
mod tests {
    use super::{Fetcher, UrlPolicy};
    use crate::{
        errors::FetcherTraitError,
        fetcher::test_support::{BODY, BODY_CRC, respond},
        revision::Revision,
        wizard_patcher::WizardPatcher,
    };
    use reqwest::{Client, Response};
    use std::path::Path;

    fn policy() -> UrlPolicy {
        UrlPolicy::new(&WizardPatcher {
            list_file_url:
                "http://versions.example/WizPatcher/V_r1.Wizard_1_0/Windows/LatestFileList.bin"
                    .to_string(),
            url_prefix: "http://assets.example/WizPatcher/V_r1.Wizard_1_0".to_string(),
            revision: Revision {
                name: "V_r1.Wizard_1_0".to_string(),
                number: 1,
            },
            patch_host: "patch.example".to_string(),
        })
    }

    #[test]
    fn url_policy_allows_named_hosts_only() {
        let policy = policy();

        assert!(policy.check("http://patch.example/a.wad").is_ok());
        assert!(policy.check("http://versions.example/a.bin").is_ok());
        assert!(policy.check("http://assets.example/a.wad").is_ok());
        assert!(policy.check("http://evil.example/a.wad").is_err());
        assert!(policy.check("https://assets.example/a.wad").is_err());
    }

    struct TestFetcher;
    impl Fetcher for TestFetcher {}

    async fn serve_body_once() -> Response {
        let server = respond("200 OK", BODY).await;
        Client::new()
            .get(format!("{}/a.wad", server.url))
            .send()
            .await
            .expect("response")
    }

    fn assert_nothing_written(path: &Path) {
        assert!(!path.exists());
        assert!(!TestFetcher::part_path(path).exists());
    }

    #[tokio::test]
    async fn size_mismatch_deletes_part() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("a.wad");

        let err = TestFetcher::write_to_file_streamed(
            &path,
            serve_body_once().await,
            None,
            Some(BODY.len() as u64 + 1),
            Some(BODY_CRC),
        )
        .await
        .expect_err("size mismatch");

        assert!(matches!(
            err.downcast_ref::<FetcherTraitError>(),
            Some(FetcherTraitError::SizeMismatch { .. })
        ));
        assert_nothing_written(&path);
    }

    #[tokio::test]
    async fn crc_mismatch_deletes_part() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("a.wad");

        let err = TestFetcher::write_to_file_streamed(
            &path,
            serve_body_once().await,
            None,
            Some(BODY.len() as u64),
            Some(BODY_CRC ^ 1),
        )
        .await
        .expect_err("crc mismatch");

        assert!(matches!(
            err.downcast_ref::<FetcherTraitError>(),
            Some(FetcherTraitError::CrcMismatch { .. })
        ));
        assert_nothing_written(&path);
    }

    #[tokio::test]
    async fn matching_download_is_kept() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("a.wad");

        let crc = TestFetcher::write_to_file_streamed(
            &path,
            serve_body_once().await,
            None,
            Some(BODY.len() as u64),
            Some(BODY_CRC),
        )
        .await
        .expect("verified write");

        assert_eq!(crc, BODY_CRC);
        assert_eq!(std::fs::read(&path).expect("final file"), BODY);
        assert!(!TestFetcher::part_path(&path).exists());
    }
}
