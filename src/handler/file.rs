use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::Deserialize;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::error::AppError;
use crate::error::file::FileError;
use crate::serializer::file::{
    CreateFileRequest, FileResponse, PresignDownloadResponse, PresignUploadResponse,
};
use crate::service::file::FileContent;
use crate::state::AppState;
use crate::storage::{StorageError, content_disposition};

pub async fn create(
    State(state): State<Arc<AppState>>,
    Extension(user_id): Extension<Uuid>,
    Json(req): Json<CreateFileRequest>,
) -> Result<Json<PresignUploadResponse>, AppError> {
    let result = state.file_service.create(user_id, req).await?;
    Ok(Json(result))
}

pub async fn complete(
    State(state): State<Arc<AppState>>,
    Extension(user_id): Extension<Uuid>,
    Path(id): Path<i64>,
) -> Result<Json<FileResponse>, AppError> {
    let result = state.file_service.complete(user_id, id).await?;
    Ok(Json(result))
}

pub async fn download(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<PresignDownloadResponse>, AppError> {
    let result = state.file_service.download(id).await?;
    Ok(Json(result))
}

pub async fn content(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let result = match state.file_service.content(id, range_header(&headers)).await {
        Ok(result) => result,
        Err(AppError::Storage(StorageError::RangeNotSatisfiable { total })) => {
            return Ok(range_not_satisfiable(total));
        }
        Err(e) => return Err(e),
    };
    file_response(result, true)
}

#[derive(Debug, Deserialize)]
pub struct BlobQuery {
    token: Option<String>,
}

pub async fn upload_blob(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(query): Query<BlobQuery>,
    body: Body,
) -> Result<StatusCode, AppError> {
    let token = query
        .token
        .as_deref()
        .ok_or(AppError::File(FileError::InvalidStorageToken))?;
    state
        .file_service
        .upload_blob(id, token, body.into_data_stream())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn download_blob(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(query): Query<BlobQuery>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let token = query
        .token
        .as_deref()
        .ok_or(AppError::File(FileError::InvalidStorageToken))?;
    let result = match state
        .file_service
        .download_blob(id, token, range_header(&headers))
        .await
    {
        Ok(result) => result,
        Err(AppError::Storage(StorageError::RangeNotSatisfiable { total })) => {
            return Ok(range_not_satisfiable(total));
        }
        Err(e) => return Err(e),
    };
    file_response(result, false)
}

fn range_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
}

fn file_response(result: FileContent, inline: bool) -> Result<Response, AppError> {
    let status = if result.ranged && result.object.content_range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    let mut response = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, result.file.content_type)
        .header(
            header::CONTENT_DISPOSITION,
            content_disposition(&result.file.name, inline),
        )
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CACHE_CONTROL, "private, max-age=86400");

    if result.object.content_length > 0 {
        response = response.header(
            header::CONTENT_LENGTH,
            result.object.content_length.to_string(),
        );
    }
    if let Some(content_range) = result.object.content_range {
        response = response.header(header::CONTENT_RANGE, content_range);
    }

    let body = Body::from_stream(ReaderStream::new(result.object.body.into_reader()));
    response
        .body(body)
        .map_err(|e| AppError::Storage(StorageError::Io(e.to_string())))
}

fn range_not_satisfiable(total: u64) -> Response {
    (
        StatusCode::RANGE_NOT_SATISFIABLE,
        [(header::CONTENT_RANGE, format!("bytes */{total}"))],
    )
        .into_response()
}
