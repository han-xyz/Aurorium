use crate::{
    AppState,
    errors::RouteError,
    utils::{ConnectionAddr, is_safe_relative},
};
use axum::{
    extract::{Path, Request, State},
    response::IntoResponse,
};
use reqwest::StatusCode;
use std::path::{Component, Path as StdPath};
use tower::ServiceExt;
use tower_http::services::ServeFile;
use tracing::{debug, warn};

impl IntoResponse for RouteError {
    fn into_response(self) -> axum::response::Response {
        match self {
            RouteError::NotFound(file) => {
                (StatusCode::NOT_FOUND, format!("File not found: {file}")).into_response()
            }
            RouteError::Database(err) => {
                warn!(error = %err, "Database error occurred");
                (StatusCode::INTERNAL_SERVER_ERROR).into_response()
            }
            RouteError::BadRequest(file_path) => (
                StatusCode::BAD_REQUEST,
                format!("Invalid file path: {file_path}"),
            )
                .into_response(),
        }
    }
}

pub async fn file(
    State(state): State<AppState>,
    Path((revision, file_path)): Path<(String, String)>,
    ConnectionAddr(addr): ConnectionAddr,
    req: Request,
) -> Result<impl IntoResponse, RouteError> {
    debug!("GET /{revision}/{file_path} from {addr}");

    if !is_safe_relative(&revision) || revision.contains('/') {
        return Err(RouteError::BadRequest(revision));
    }

    // Prevent directory traversal. Better to be safe than sorry
    if StdPath::new(&file_path).components().any(|c| {
        matches!(
            c,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(RouteError::BadRequest(file_path));
    }

    // If the file is LatestFileList.xml or LatestFileList.bin, we know that it belongs to the current revision
    let revision_for_asset =
        if file_path == "LatestFileList.xml" || file_path == "LatestFileList.bin" {
            revision
        } else {
            state
                .db
                .get_revision_for_asset(revision, file_path.clone())
                .await?
                .ok_or_else(|| RouteError::NotFound(file_path.clone()))?
        };

    let path = state
        .save_directory
        .join(revision_for_asset)
        .join(file_path);

    match ServeFile::new(path).oneshot(req).await {
        Ok(res) => Ok(res.into_response()),
        Err(err) => match err {},
    }
}

#[cfg(test)]
mod tests {
    use crate::{AppState, config::AppConfig, db::Database, router};
    use axum::{Router, body::Body, http::Request};
    use reqwest::StatusCode;
    use std::fs;
    use tempfile::TempDir;
    use tower::ServiceExt;

    const REVISION: &str = "V_r1.Wizard_1_0";

    async fn app() -> (TempDir, Router) {
        let root = tempfile::tempdir().expect("tempdir");
        let save_directory = root.path().join("srv").join("data");
        let revision_dir = save_directory.join(REVISION);
        fs::create_dir_all(revision_dir.join("LatestFileList")).expect("revision dir");
        fs::write(revision_dir.join("LatestFileList.xml"), "inside").expect("write");
        fs::write(
            revision_dir.join("LatestFileList").join("other.xml"),
            "nested",
        )
        .expect("write");
        fs::write(revision_dir.join("fooLatestFileList.xml"), "prefixed").expect("write");
        fs::create_dir_all(root.path().join("outside")).expect("outside dir");
        fs::write(
            root.path().join("outside").join("LatestFileList.xml"),
            "outside",
        )
        .expect("write");

        let db_path = root.path().join("test.db");
        let db = Database::init(db_path.to_str().expect("utf-8 path"))
            .await
            .expect("db");
        let state = AppState::new(AppConfig::default(), db, save_directory);
        (root, router(state))
    }

    async fn status_of(app: &Router, uri: &str) -> StatusCode {
        let req = Request::builder()
            .uri(uri)
            .header("X-Forwarded-For", "127.0.0.1")
            .body(Body::empty())
            .expect("request");
        app.clone().oneshot(req).await.expect("response").status()
    }

    #[tokio::test]
    async fn route_revision_traversal_400() {
        let (_root, app) = app().await;
        for uri in [
            "/..%2F..%2Foutside/LatestFileList.xml",
            "/%2Fabs/LatestFileList.xml",
            "/../LatestFileList.xml",
            "/x%00y/LatestFileList.xml",
            "/a%5Cb/LatestFileList.xml",
        ] {
            assert_eq!(status_of(&app, uri).await, StatusCode::BAD_REQUEST, "{uri}");
        }
    }

    #[tokio::test]
    async fn route_exact_latestfilelist_only() {
        let (_root, app) = app().await;
        assert_eq!(
            status_of(&app, "/V_r1.Wizard_1_0/LatestFileList.xml").await,
            StatusCode::OK
        );
        for uri in [
            "/V_r1.Wizard_1_0/LatestFileList/other.xml",
            "/V_r1.Wizard_1_0/fooLatestFileList.xml",
        ] {
            assert_eq!(status_of(&app, uri).await, StatusCode::NOT_FOUND, "{uri}");
        }
    }
}
