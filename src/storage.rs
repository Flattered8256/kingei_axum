pub mod local;
pub mod s3;
pub mod sign;

use std::env;
use std::pin::Pin;
use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use tokio::io::{AsyncRead, Take};

pub const PRESIGN_TTL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("{0}")]
    S3(String),
    #[error("{0}")]
    Io(String),
    #[error("{0}")]
    Token(String),
    #[error("invalid storage token")]
    InvalidToken,
    #[error("invalid object key")]
    InvalidKey,
    #[error("size mismatch")]
    SizeMismatch,
    #[error("range not satisfiable")]
    RangeNotSatisfiable { total: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBackend {
    S3,
    Local,
}

pub fn parse_backend(value: &str) -> Option<StorageBackend> {
    match value.trim().to_ascii_lowercase().as_str() {
        "s3" => Some(StorageBackend::S3),
        "local" => Some(StorageBackend::Local),
        _ => None,
    }
}

pub fn backend_from_env() -> StorageBackend {
    match env::var("STORAGE_BACKEND") {
        Ok(value) => parse_backend(&value).unwrap_or_else(|| {
            panic!("STORAGE_BACKEND必须是s3或local,请检查项目根目录下的.env文件")
        }),
        Err(_) => StorageBackend::S3,
    }
}

pub enum Storage {
    S3(s3::S3Storage),
    Local(local::LocalStorage),
}

impl Storage {
    pub async fn connect() -> Self {
        match backend_from_env() {
            StorageBackend::S3 => Storage::S3(s3::S3Storage::connect().await),
            StorageBackend::Local => Storage::Local(local::LocalStorage::connect()),
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(self, Storage::Local(_))
    }

    pub async fn upload_url(
        &self,
        file_id: i64,
        key: &str,
        content_type: &str,
        size: i64,
    ) -> Result<String, StorageError> {
        match self {
            Storage::S3(s3) => s3.upload_url(key, content_type, size).await,
            Storage::Local(local) => local.upload_url(file_id, key),
        }
    }

    pub async fn download_url(
        &self,
        file_id: i64,
        key: &str,
        content_type: &str,
        filename: &str,
    ) -> Result<String, StorageError> {
        match self {
            Storage::S3(s3) => s3.download_url(key, content_type, filename).await,
            Storage::Local(local) => local.download_url(file_id, key),
        }
    }

    pub fn verify_token(
        &self,
        token: &str,
        file_id: i64,
        key: &str,
        op: sign::StorageOp,
    ) -> Result<(), StorageError> {
        match self {
            Storage::S3(_) => Err(StorageError::InvalidToken),
            Storage::Local(local) => local.verify_token(token, file_id, key, op),
        }
    }

    pub async fn head_object(&self, key: &str) -> Result<Option<ObjectMeta>, StorageError> {
        match self {
            Storage::S3(s3) => s3.head_object(key).await,
            Storage::Local(local) => local.head_object(key).await,
        }
    }

    pub async fn get_object(
        &self,
        key: &str,
        range: Option<&str>,
    ) -> Result<ObjectStream, StorageError> {
        match self {
            Storage::S3(s3) => s3.get_object(key, range).await,
            Storage::Local(local) => local.get_object(key, range).await,
        }
    }

    pub async fn get_object_to_path(
        &self,
        key: &str,
        path: &std::path::Path,
    ) -> Result<(), StorageError> {
        match self {
            Storage::S3(s3) => s3.get_object_to_path(key, path).await,
            Storage::Local(local) => local.get_object_to_path(key, path).await,
        }
    }

    pub async fn put_object(
        &self,
        key: &str,
        content_type: &str,
        path: &std::path::Path,
    ) -> Result<i64, StorageError> {
        match self {
            Storage::S3(s3) => s3.put_object(key, content_type, path).await,
            Storage::Local(local) => local.put_object(key, content_type, path).await,
        }
    }

    pub async fn write_stream<S, B, E>(
        &self,
        key: &str,
        size: i64,
        stream: S,
    ) -> Result<i64, StorageError>
    where
        S: futures::Stream<Item = Result<B, E>> + Send,
        B: AsRef<[u8]>,
        E: std::fmt::Display,
    {
        match self {
            Storage::S3(_) => {
                drop(stream);
                Err(StorageError::S3("当前存储后端不支持服务端写入".to_string()))
            }
            Storage::Local(local) => local.write_stream(key, size, stream).await,
        }
    }
}

pub struct ObjectMeta {
    pub content_length: i64,
}

pub struct ObjectStream {
    pub content_length: i64,
    pub content_range: Option<String>,
    pub body: ObjectBody,
}

pub enum ObjectBody {
    S3(ByteStream),
    Local(Take<tokio::fs::File>),
}

impl ObjectBody {
    pub fn into_reader(self) -> Pin<Box<dyn AsyncRead + Send>> {
        match self {
            ObjectBody::S3(stream) => Box::pin(stream.into_async_read()),
            ObjectBody::Local(file) => Box::pin(file),
        }
    }
}

pub fn parse_bytes_range(header: &str) -> Option<String> {
    let spec = header.trim().strip_prefix("bytes=")?.trim();
    if spec.is_empty() || spec.contains(',') {
        return None;
    }
    let (start, end) = spec.split_once('-')?;
    if start.is_empty() && end.is_empty() {
        return None;
    }
    if !start.chars().all(|c| c.is_ascii_digit()) || !end.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(format!("bytes={spec}"))
}

pub fn content_disposition(filename: &str, inline: bool) -> String {
    let sanitized: String = filename
        .chars()
        .filter(|c| *c != '\r' && *c != '\n')
        .collect();
    let fallback: String = sanitized
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let fallback = if fallback.is_empty() {
        "download".to_string()
    } else {
        fallback
    };
    let kind = if inline { "inline" } else { "attachment" };
    format!(
        "{kind}; filename=\"{fallback}\"; filename*=UTF-8''{}",
        percent_encode(sanitized.as_bytes())
    )
}

fn percent_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        if is_unreserved(b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn is_unreserved(b: u8) -> bool {
    matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attachment_ascii_filename() {
        assert_eq!(
            content_disposition("hello.txt", false),
            "attachment; filename=\"hello.txt\"; filename*=UTF-8''hello.txt"
        );
    }

    #[test]
    fn inline_ascii_filename() {
        assert_eq!(
            content_disposition("hello.txt", true),
            "inline; filename=\"hello.txt\"; filename*=UTF-8''hello.txt"
        );
    }

    #[test]
    fn chinese_filename() {
        assert_eq!(
            content_disposition("照片.png", false),
            "attachment; filename=\"__.png\"; filename*=UTF-8''%E7%85%A7%E7%89%87.png"
        );
    }

    #[test]
    fn parse_open_and_closed_range() {
        assert_eq!(
            parse_bytes_range("bytes=0-1023"),
            Some("bytes=0-1023".to_string())
        );
        assert_eq!(
            parse_bytes_range("bytes=500-"),
            Some("bytes=500-".to_string())
        );
        assert_eq!(
            parse_bytes_range("bytes=-500"),
            Some("bytes=-500".to_string())
        );
        assert_eq!(parse_bytes_range("bytes=0-1,2-3"), None);
        assert_eq!(parse_bytes_range("bytes="), None);
        assert_eq!(parse_bytes_range("items=0-1"), None);
    }

    #[test]
    fn parse_backend_values() {
        assert_eq!(parse_backend("s3"), Some(StorageBackend::S3));
        assert_eq!(parse_backend(" LOCAL "), Some(StorageBackend::Local));
        assert_eq!(parse_backend("oss"), None);
    }
}
