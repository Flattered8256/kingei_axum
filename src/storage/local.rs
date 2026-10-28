use std::env;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use futures::StreamExt;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

use crate::storage::sign::{self, StorageOp};
use crate::storage::{ObjectBody, ObjectMeta, ObjectStream, StorageError};

pub struct LocalStorage {
    root: PathBuf,
    tmp: PathBuf,
    base: String,
    secret: String,
}

impl LocalStorage {
    pub fn connect() -> Self {
        let dir = env::var("STORAGE_DIR").expect("缺少STORAGE_DIR,请检查项目根目录下的.env文件");
        let root = PathBuf::from(dir.trim());
        std::fs::create_dir_all(&root).expect("创建STORAGE_DIR失败");
        let root = std::fs::canonicalize(&root).expect("解析STORAGE_DIR失败");
        let tmp = root.join(".tmp");
        std::fs::create_dir_all(&tmp).expect("创建STORAGE_DIR/.tmp失败");
        let base = public_base();
        let secret = storage_secret();

        tracing::info!(dir = %root.display(), base = %base, "local storage connected");
        Self {
            root,
            tmp,
            base,
            secret,
        }
    }

    pub fn upload_url(&self, file_id: i64, key: &str) -> Result<String, StorageError> {
        let token = sign::sign(&self.secret, file_id, key, StorageOp::Put)?;
        Ok(format!("{}/file/{file_id}/blob?token={token}", self.base))
    }

    pub fn download_url(&self, file_id: i64, key: &str) -> Result<String, StorageError> {
        let token = sign::sign(&self.secret, file_id, key, StorageOp::Get)?;
        Ok(format!("{}/file/{file_id}/blob?token={token}", self.base))
    }

    pub fn verify_token(
        &self,
        token: &str,
        file_id: i64,
        key: &str,
        op: StorageOp,
    ) -> Result<(), StorageError> {
        sign::verify(&self.secret, token, file_id, key, op)
    }

    pub async fn head_object(&self, key: &str) -> Result<Option<ObjectMeta>, StorageError> {
        let path = self.object_path(key)?;
        match tokio::fs::metadata(&path).await {
            Ok(meta) => Ok(Some(ObjectMeta {
                content_length: meta.len() as i64,
            })),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_error(e)),
        }
    }

    pub async fn get_object(
        &self,
        key: &str,
        range: Option<&str>,
    ) -> Result<ObjectStream, StorageError> {
        let path = self.object_path(key)?;
        let mut file = tokio::fs::File::open(&path).await.map_err(io_error)?;
        let total = file.metadata().await.map_err(io_error)?.len();

        let Some(spec) = range else {
            return Ok(ObjectStream {
                content_length: total as i64,
                content_range: None,
                body: ObjectBody::Local(file.take(u64::MAX)),
            });
        };

        let (start, end) = parse_range(spec, total)?;
        file.seek(SeekFrom::Start(start)).await.map_err(io_error)?;
        let length = end - start + 1;
        Ok(ObjectStream {
            content_length: length as i64,
            content_range: Some(format!("bytes {start}-{end}/{total}")),
            body: ObjectBody::Local(file.take(length)),
        })
    }

    pub async fn get_object_to_path(&self, key: &str, path: &Path) -> Result<(), StorageError> {
        let source = self.object_path(key)?;
        tokio::fs::copy(&source, path).await.map_err(io_error)?;
        Ok(())
    }

    pub async fn put_object(
        &self,
        key: &str,
        _content_type: &str,
        path: &Path,
    ) -> Result<i64, StorageError> {
        let size = tokio::fs::metadata(path).await.map_err(io_error)?.len() as i64;
        let destination = self.object_path(key)?;
        let parent = destination.parent().ok_or(StorageError::InvalidKey)?;
        tokio::fs::create_dir_all(parent).await.map_err(io_error)?;

        let temp = self.temp_path();
        tokio::fs::copy(path, &temp).await.map_err(io_error)?;
        if let Err(e) = tokio::fs::rename(&temp, &destination).await {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(io_error(e));
        }
        Ok(size)
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
        if size < 0 {
            return Err(StorageError::SizeMismatch);
        }

        let destination = self.object_path(key)?;
        let parent = destination.parent().ok_or(StorageError::InvalidKey)?;
        tokio::fs::create_dir_all(parent).await.map_err(io_error)?;
        tokio::fs::create_dir_all(&self.tmp)
            .await
            .map_err(io_error)?;

        let temp = self.temp_path();
        let mut file = tokio::fs::File::create(&temp).await.map_err(io_error)?;
        let mut written: i64 = 0;
        let mut stream = std::pin::pin!(stream);

        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(e) => {
                    remove_temp(&temp).await;
                    return Err(StorageError::Io(e.to_string()));
                }
            };
            let bytes = chunk.as_ref();
            written += bytes.len() as i64;
            if written > size {
                remove_temp(&temp).await;
                return Err(StorageError::SizeMismatch);
            }
            if let Err(e) = file.write_all(bytes).await {
                remove_temp(&temp).await;
                return Err(io_error(e));
            }
        }

        if let Err(e) = file.flush().await {
            remove_temp(&temp).await;
            return Err(io_error(e));
        }
        drop(file);

        if written != size {
            remove_temp(&temp).await;
            return Err(StorageError::SizeMismatch);
        }

        if let Err(e) = tokio::fs::rename(&temp, &destination).await {
            remove_temp(&temp).await;
            return Err(io_error(e));
        }
        Ok(written)
    }

    fn object_path(&self, key: &str) -> Result<PathBuf, StorageError> {
        // object_key 只允许 `{owner_id}/{uuid}`，两段都必须是 UUID，借此拒绝路径穿越。
        let mut parts = key.split('/');
        let owner = parts.next().ok_or(StorageError::InvalidKey)?;
        let name = parts.next().ok_or(StorageError::InvalidKey)?;
        if parts.next().is_some() {
            return Err(StorageError::InvalidKey);
        }
        Uuid::parse_str(owner).map_err(|_| StorageError::InvalidKey)?;
        Uuid::parse_str(name).map_err(|_| StorageError::InvalidKey)?;
        Ok(self.root.join(owner).join(name))
    }

    fn temp_path(&self) -> PathBuf {
        self.tmp.join(Uuid::new_v4().to_string())
    }
}

fn storage_secret() -> String {
    env::var("STORAGE_SECRET")
        .or_else(|_| env::var("JWT_SECRET"))
        .expect("缺少STORAGE_SECRET或JWT_SECRET,请检查项目根目录下的.env文件")
}

fn public_base() -> String {
    let raw = env::var("STORAGE_PUBLIC_BASE_URL")
        .expect("缺少STORAGE_PUBLIC_BASE_URL,请检查项目根目录下的.env文件");
    let base = raw.trim().trim_end_matches('/');
    if !(base.starts_with("http://") || base.starts_with("https://"))
        || base.contains(char::is_whitespace)
        || base.contains('?')
        || base.contains('#')
    {
        panic!(
            "STORAGE_PUBLIC_BASE_URL必须是http(s)://主机[:端口][/前缀],请检查项目根目录下的.env文件"
        );
    }
    base.to_string()
}

fn io_error(error: std::io::Error) -> StorageError {
    StorageError::Io(error.to_string())
}

async fn remove_temp(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}

fn parse_range(spec: &str, total: u64) -> Result<(u64, u64), StorageError> {
    parse_range_inner(spec, total).ok_or(StorageError::RangeNotSatisfiable { total })
}

fn parse_range_inner(spec: &str, total: u64) -> Option<(u64, u64)> {
    if total == 0 {
        return None;
    }
    let body = spec.strip_prefix("bytes=")?;
    let (start, end) = body.split_once('-')?;

    if start.is_empty() {
        let suffix: u64 = end.parse().ok()?;
        if suffix == 0 {
            return None;
        }
        let length = suffix.min(total);
        return Some((total - length, total - 1));
    }

    let start: u64 = start.parse().ok()?;
    if start >= total {
        return None;
    }
    let end = if end.is_empty() {
        total - 1
    } else {
        end.parse::<u64>().ok()?.min(total - 1)
    };
    if end < start {
        return None;
    }
    Some((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage_at(dir: &Path) -> LocalStorage {
        LocalStorage {
            root: dir.to_path_buf(),
            tmp: dir.join(".tmp"),
            base: "http://127.0.0.1:8000".to_string(),
            secret: "test-secret".to_string(),
        }
    }

    const KEY: &str = "11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222";

    fn chunks() -> impl futures::Stream<Item = Result<Vec<u8>, std::io::Error>> + Send {
        futures::stream::iter(vec![Ok(b"hello".to_vec()), Ok(b" world".to_vec())])
    }

    #[tokio::test]
    async fn roundtrip_and_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let storage = storage_at(dir.path());
        std::fs::create_dir_all(&storage.tmp).unwrap();

        storage.write_stream(KEY, 11, chunks()).await.unwrap();
        assert_eq!(
            storage
                .head_object(KEY)
                .await
                .unwrap()
                .map(|meta| meta.content_length),
            Some(11)
        );

        let whole = storage.get_object(KEY, None).await.unwrap();
        assert_eq!(whole.content_length, 11);
        let mut buf = Vec::new();
        whole
            .body
            .into_reader()
            .read_to_end(&mut buf)
            .await
            .unwrap();
        assert_eq!(buf, b"hello world");

        let ranged = storage.get_object(KEY, Some("bytes=1-3")).await.unwrap();
        assert_eq!(ranged.content_length, 3);
        assert_eq!(ranged.content_range.as_deref(), Some("bytes 1-3/11"));
        let mut buf = Vec::new();
        ranged
            .body
            .into_reader()
            .read_to_end(&mut buf)
            .await
            .unwrap();
        assert_eq!(buf, b"ell");

        let suffix = storage.get_object(KEY, Some("bytes=-5")).await.unwrap();
        assert_eq!(suffix.content_range.as_deref(), Some("bytes 6-10/11"));
        let mut buf = Vec::new();
        suffix
            .body
            .into_reader()
            .read_to_end(&mut buf)
            .await
            .unwrap();
        assert_eq!(buf, b"world");
    }

    #[tokio::test]
    async fn size_mismatch_removes_temp() {
        let dir = tempfile::tempdir().unwrap();
        let storage = storage_at(dir.path());
        std::fs::create_dir_all(&storage.tmp).unwrap();

        let result = storage.write_stream(KEY, 3, chunks()).await;
        assert!(matches!(result, Err(StorageError::SizeMismatch)));
        assert!(storage.head_object(KEY).await.unwrap().is_none());
        assert_eq!(std::fs::read_dir(&storage.tmp).unwrap().count(), 0);
    }

    #[test]
    fn reject_bad_keys() {
        let dir = tempfile::tempdir().unwrap();
        let storage = storage_at(dir.path());
        assert!(matches!(
            storage.object_path("../escape"),
            Err(StorageError::InvalidKey)
        ));
        assert!(matches!(
            storage.object_path("/etc/passwd"),
            Err(StorageError::InvalidKey)
        ));
        assert!(storage.object_path(KEY).is_ok());
        assert!(matches!(
            storage.object_path("11111111-1111-1111-1111-111111111111"),
            Err(StorageError::InvalidKey)
        ));
    }

    #[test]
    fn parse_range_cases() {
        assert_eq!(parse_range_inner("bytes=0-0", 10), Some((0, 0)));
        assert_eq!(parse_range_inner("bytes=5-", 10), Some((5, 9)));
        assert_eq!(parse_range_inner("bytes=-3", 10), Some((7, 9)));
        assert_eq!(parse_range_inner("bytes=0-100", 10), Some((0, 9)));
        assert_eq!(parse_range_inner("bytes=10-", 10), None);
        assert_eq!(parse_range_inner("bytes=-0", 10), None);
        assert_eq!(parse_range_inner("bytes=0-", 0), None);
        assert_eq!(parse_range_inner("items=0-1", 10), None);
    }

    #[test]
    fn signed_urls() {
        let dir = tempfile::tempdir().unwrap();
        let storage = storage_at(dir.path());
        let url = storage.upload_url(7, KEY).unwrap();
        assert!(url.starts_with("http://127.0.0.1:8000/file/7/blob?token="));
        let token = url.rsplit("token=").next().unwrap();
        assert!(storage.verify_token(token, 7, KEY, StorageOp::Put).is_ok());
    }
}
