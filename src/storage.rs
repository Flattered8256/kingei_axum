pub mod local;
pub mod s3;

use std::path::Path;
use std::time::Duration;

use tokio::io::AsyncRead;

pub const PRESIGN_TTL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug)]
pub struct StorageError(pub String);

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for StorageError {}

pub struct ObjectMeta {
    pub content_length: i64,
}

pub struct ObjectStream {
    pub content_length: i64,
    pub content_range: Option<String>,
    pub body: Box<dyn AsyncRead + Send + Unpin>,
}

pub enum Storage {
    S3(s3::S3Storage),
    Local(local::LocalStorage),
}

impl Storage {
    pub async fn connect() -> Self {
        let backend = std::env::var("STORAGE_BACKEND").unwrap_or_default();
        let backend = backend.trim();
        if backend.eq_ignore_ascii_case("local") {
            return Self::Local(local::LocalStorage::connect().await);
        }
        if backend.is_empty() || backend.eq_ignore_ascii_case("s3") {
            return Self::S3(s3::S3Storage::connect().await);
        }
        panic!("STORAGE_BACKEND必须是s3或local,请检查项目根目录下的.env文件");
    }

    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    pub async fn presign_put(
        &self,
        key: &str,
        content_type: &str,
        size: i64,
    ) -> Result<String, StorageError> {
        match self {
            Self::S3(storage) => storage.presign_put(key, content_type, size).await,
            Self::Local(_) => Err(StorageError("本地存储不使用预签名上传".to_string())),
        }
    }

    pub async fn presign_get(
        &self,
        key: &str,
        content_type: &str,
        filename: &str,
    ) -> Result<String, StorageError> {
        match self {
            Self::S3(storage) => storage.presign_get(key, content_type, filename).await,
            Self::Local(_) => Err(StorageError("本地存储不使用预签名下载".to_string())),
        }
    }

    pub async fn head_object(&self, key: &str) -> Result<Option<ObjectMeta>, StorageError> {
        match self {
            Self::S3(storage) => storage.head_object(key).await,
            Self::Local(storage) => storage.head_object(key).await,
        }
    }

    pub async fn get_object(
        &self,
        key: &str,
        range: Option<&str>,
    ) -> Result<ObjectStream, StorageError> {
        match self {
            Self::S3(storage) => storage.get_object(key, range).await,
            Self::Local(storage) => storage.get_object(key, range).await,
        }
    }

    pub async fn get_object_to_path(&self, key: &str, path: &Path) -> Result<(), StorageError> {
        match self {
            Self::S3(storage) => storage.get_object_to_path(key, path).await,
            Self::Local(storage) => storage.get_object_to_path(key, path).await,
        }
    }

    pub async fn put_object(
        &self,
        key: &str,
        content_type: &str,
        path: &Path,
    ) -> Result<i64, StorageError> {
        match self {
            Self::S3(storage) => storage.put_object(key, content_type, path).await,
            Self::Local(storage) => storage.put_object(key, path).await,
        }
    }

    pub async fn put_stream(
        &self,
        key: &str,
        reader: impl AsyncRead + Send + Unpin,
        expected: u64,
    ) -> Result<u64, StorageError> {
        match self {
            Self::Local(storage) => storage.put_stream(key, reader, expected).await,
            Self::S3(_) => Err(StorageError("S3 存储由客户端直传，服务端不接收文件流".to_string())),
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
}
