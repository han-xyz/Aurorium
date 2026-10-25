use crate::{AppState, errors::RouteError, utils::ConnectionAddr};
use axum::{
    extract::State,
    response::{AppendHeaders, IntoResponse, Response},
};
use reqwest::{StatusCode, header};
use tracing::debug;

pub async fn get_latest_revision(
    State(state): State<AppState>,
    ConnectionAddr(addr): ConnectionAddr,
) -> Result<Response, RouteError> {
    debug!("GET /latest from {}", addr);

    let Some(revision) = state.db.latest_revision().await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    let headers = AppendHeaders([(header::CONTENT_TYPE, "text/plain; charset=utf-8")]);

    Ok((headers, revision.name).into_response())
}

#[cfg(test)]
mod tests {
    use crate::{AppState, config::AppConfig, db::Database, revision::Revision, router};
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{Request, Response, header},
    };
    use reqwest::StatusCode;
    use std::path::Path;
    use tempfile::TempDir;
    use tower::ServiceExt;

    const REVISION: &str = "V_r1.Wizard_1_0";

    async fn app() -> (TempDir, Database, Router) {
        let root = tempfile::tempdir().expect("tempdir");
        let db = Database::init(db_path(root.path()).to_str().expect("utf-8 path"))
            .await
            .expect("db");
        let state = AppState::new(AppConfig::default(), db.clone(), root.path().join("data"));
        (root, db, router(state))
    }

    fn db_path(root: &Path) -> std::path::PathBuf {
        root.join("test.db")
    }

    fn revision() -> Revision {
        Revision {
            name: REVISION.to_string(),
            number: 1,
        }
    }

    async fn get(app: &Router, uri: &str) -> Response<Body> {
        let req = Request::builder()
            .uri(uri)
            .header("X-Forwarded-For", "127.0.0.1")
            .body(Body::empty())
            .expect("request");
        app.clone().oneshot(req).await.expect("response")
    }

    #[tokio::test]
    async fn db_error_is_500() {
        let (root, _db, app) = app().await;
        rusqlite::Connection::open(db_path(root.path()))
            .expect("open db")
            .execute_batch("DROP TABLE assets; DROP TABLE revisions;")
            .expect("drop tables");

        for uri in ["/latest", "/revisions"] {
            assert_eq!(
                get(&app, uri).await.status(),
                StatusCode::INTERNAL_SERVER_ERROR,
                "{uri}"
            );
        }
    }

    #[tokio::test]
    async fn latest_404_until_complete() {
        let (_root, db, app) = app().await;
        db.insert_new_revision(revision(), vec![])
            .await
            .expect("insert");
        assert_eq!(get(&app, "/latest").await.status(), StatusCode::NOT_FOUND);

        assert!(db.mark_revision_complete(REVISION).await.expect("mark"));
        let res = get(&app, "/latest").await;
        assert_eq!(res.status(), StatusCode::OK);
        let body = to_bytes(res.into_body(), usize::MAX).await.expect("body");
        assert_eq!(body, REVISION.as_bytes());
    }

    #[tokio::test]
    async fn latest_text_plain() {
        let (_root, db, app) = app().await;
        db.insert_new_revision(revision(), vec![])
            .await
            .expect("insert");
        db.mark_revision_complete(REVISION).await.expect("mark");

        let res = get(&app, "/latest").await;
        assert_eq!(
            res.headers()[header::CONTENT_TYPE],
            "text/plain; charset=utf-8"
        );
    }
}
