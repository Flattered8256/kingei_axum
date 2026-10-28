use std::sync::Arc;

use axum::{
    Router,
    extract::DefaultBodyLimit,
    middleware,
    routing::{get, post, put},
};

use crate::handler::file;
use crate::middleware::auth;
use crate::state::AppState;

pub fn protected_routes() -> Router<Arc<AppState>> {
    let bearer = Router::new()
        .route("/", post(file::create))
        .route("/{id}/complete", post(file::complete))
        .route("/{id}/download", get(file::download))
        .layer(middleware::from_fn(auth::auth_middleware));

    let content = Router::new()
        .route("/{id}/content", get(file::content))
        .layer(middleware::from_fn(auth::auth_bearer_or_cookie));

    // 本地存储的上传/下载入口靠 URL 里的签名授权，不走 JWT，也不限制请求体大小。
    let signed = Router::new()
        .route(
            "/{id}/blob",
            put(file::upload_blob).get(file::download_blob),
        )
        .layer(DefaultBodyLimit::disable());

    bearer.merge(content).merge(signed)
}
