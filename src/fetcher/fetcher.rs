use crate::{crc::Crc32, errors::FetcherTraitError};
use indicatif::ProgressBar;
use reqwest::{Client, Response};
use std::path::{Path, PathBuf};
use tokio::{
    fs::create_dir_all,
    io::{AsyncWriteExt, BufWriter},
};

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
    use super::Fetcher;
    use crate::errors::FetcherTraitError;
    use reqwest::{Client, Response};
    use std::path::Path;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    const BODY: &[u8] = b"123456789";
    const BODY_CRC: u32 = 0x2dfd_2d88;

    struct TestFetcher;
    impl Fetcher for TestFetcher {}

    async fn serve_body_once() -> Response {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request).await;
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                BODY.len()
            );
            socket.write_all(head.as_bytes()).await.expect("write head");
            socket.write_all(BODY).await.expect("write body");
        });
        Client::new()
            .get(format!("http://{addr}/a.wad"))
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
