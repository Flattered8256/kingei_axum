use std::env;
use std::path::Path;

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{
    Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CorsConfiguration, CorsRule};
use tokio::io::AsyncWriteExt;

use crate::storage::{ObjectBody, ObjectMeta, ObjectStream, PRESIGN_TTL, StorageError};

pub struct S3Storage {
    client: Client,
    presign: Client,
    bucket: String,
}

struct S3Config {
    endpoint: String,
    public_endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: String,
    region: String,
    path_style: bool,
    manage_bucket: bool,
}

impl S3Config {
    fn from_env() -> Self {
        let endpoint = require_env("S3_ENDPOINT");
        let public_endpoint = env::var("S3_PUBLIC_ENDPOINT").unwrap_or_else(|_| endpoint.clone());
        Self {
            endpoint,
            public_endpoint,
            access_key: require_env("S3_ACCESS_KEY"),
            secret_key: require_env("S3_SECRET_KEY"),
            bucket: require_env("S3_BUCKET"),
            region: env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            path_style: env_flag("S3_PATH_STYLE", true),
            manage_bucket: env_flag("S3_MANAGE_BUCKET", true),
        }
    }
}

impl S3Storage {
    pub async fn connect() -> Self {
        let config = S3Config::from_env();
        let client = s3_client(
            &config.endpoint,
            &config.access_key,
            &config.secret_key,
            &config.region,
            config.path_style,
        )
        .await;
        let presign = if config.public_endpoint == config.endpoint {
            client.clone()
        } else {
            s3_client(
                &config.public_endpoint,
                &config.access_key,
                &config.secret_key,
                &config.region,
                config.path_style,
            )
            .await
        };

        if config.manage_bucket {
            ensure_bucket(&client, &config.bucket).await;
        } else {
            client
                .head_bucket()
                .bucket(&config.bucket)
                .send()
                .await
                .expect("S3 bucket不存在或无权访问");
        }

        tracing::info!(
            bucket = %config.bucket,
            endpoint = %config.endpoint,
            public_endpoint = %config.public_endpoint,
            path_style = config.path_style,
            manage_bucket = config.manage_bucket,
            "s3 connected"
        );
        Self {
            client,
            presign,
            bucket: config.bucket,
        }
    }

    pub async fn upload_url(
        &self,
        key: &str,
        content_type: &str,
        size: i64,
    ) -> Result<String, StorageError> {
        let presigning = PresigningConfig::expires_in(PRESIGN_TTL)
            .map_err(|e| StorageError::S3(e.to_string()))?;
        let presigned = self
            .presign
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .content_length(size)
            .presigned(presigning)
            .await
            .map_err(|e| StorageError::S3(format!("{e:?}")))?;
        Ok(presigned.uri().to_string())
    }

    pub async fn download_url(
        &self,
        key: &str,
        content_type: &str,
        filename: &str,
    ) -> Result<String, StorageError> {
        let presigning = PresigningConfig::expires_in(PRESIGN_TTL)
            .map_err(|e| StorageError::S3(e.to_string()))?;
        let presigned = self
            .presign
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .response_content_type(content_type)
            .response_content_disposition(crate::storage::content_disposition(filename, false))
            .presigned(presigning)
            .await
            .map_err(|e| StorageError::S3(format!("{e:?}")))?;
        Ok(presigned.uri().to_string())
    }

    pub async fn head_object(&self, key: &str) -> Result<Option<ObjectMeta>, StorageError> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => Ok(Some(ObjectMeta {
                content_length: output.content_length().unwrap_or(0),
            })),
            Err(e) => {
                if is_not_found(&e) {
                    Ok(None)
                } else {
                    Err(StorageError::S3(format!("{e:?}")))
                }
            }
        }
    }

    pub async fn get_object(
        &self,
        key: &str,
        range: Option<&str>,
    ) -> Result<ObjectStream, StorageError> {
        let mut request = self.client.get_object().bucket(&self.bucket).key(key);
        if let Some(range) = range {
            request = request.range(range);
        }
        let output = request
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("{e:?}")))?;
        Ok(ObjectStream {
            content_length: output.content_length().unwrap_or(0),
            content_range: output.content_range().map(str::to_string),
            body: ObjectBody::S3(output.body),
        })
    }

    pub async fn get_object_to_path(&self, key: &str, path: &Path) -> Result<(), StorageError> {
        let object = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("{e:?}")))?;
        let mut reader = object.body.into_async_read();
        let mut writer = tokio::fs::File::create(path)
            .await
            .map_err(|e| StorageError::Io(e.to_string()))?;
        tokio::io::copy(&mut reader, &mut writer)
            .await
            .map_err(|e| StorageError::Io(e.to_string()))?;
        writer
            .flush()
            .await
            .map_err(|e| StorageError::Io(e.to_string()))?;
        Ok(())
    }

    pub async fn put_object(
        &self,
        key: &str,
        content_type: &str,
        path: &Path,
    ) -> Result<i64, StorageError> {
        let size = tokio::fs::metadata(path)
            .await
            .map_err(|e| StorageError::Io(e.to_string()))?
            .len() as i64;
        let body = ByteStream::from_path(path)
            .await
            .map_err(|e| StorageError::Io(e.to_string()))?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .content_length(size)
            .body(body)
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("{e:?}")))?;
        Ok(size)
    }
}

fn require_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("缺少{name},请检查项目根目录下的.env文件"))
}

fn env_flag(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(value) => parse_bool(&value)
            .unwrap_or_else(|| panic!("{name}必须是true或false,请检查项目根目录下的.env文件")),
        Err(_) => default,
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

async fn ensure_bucket(client: &Client, bucket: &str) {
    let listed = client.list_buckets().send().await.expect("连接S3失败");
    let exists = listed
        .buckets()
        .iter()
        .any(|item| item.name() == Some(bucket));
    if !exists {
        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("创建S3 bucket失败");
    }

    let cors = CorsConfiguration::builder()
        .cors_rules(
            CorsRule::builder()
                .allowed_methods("PUT")
                .allowed_methods("GET")
                .allowed_methods("HEAD")
                .allowed_origins("*")
                .allowed_headers("*")
                .expose_headers("ETag")
                .expose_headers("Content-Length")
                .expose_headers("Content-Range")
                .build()
                .expect("构造S3 CORS规则失败"),
        )
        .build()
        .expect("构造S3 CORS配置失败");
    client
        .put_bucket_cors()
        .bucket(bucket)
        .cors_configuration(cors)
        .send()
        .await
        .expect("设置S3 bucket CORS失败");
}

async fn s3_client(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    path_style: bool,
) -> Client {
    let credentials = Credentials::new(access_key, secret_key, None, None, "s3");
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new(region.to_string()))
        .credentials_provider(credentials)
        .load()
        .await;
    let config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(path_style)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .build();
    Client::from_conf(config)
}

fn is_not_found(error: &SdkError<HeadObjectError>) -> bool {
    if error
        .as_service_error()
        .is_some_and(HeadObjectError::is_not_found)
    {
        return true;
    }
    error
        .raw_response()
        .is_some_and(|response| response.status().as_u16() == 404)
}

#[cfg(test)]
mod tests {
    use super::parse_bool;

    #[test]
    fn parse_bool_flags() {
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("TRUE"), Some(true));
        assert_eq!(parse_bool("1"), Some(true));
        assert_eq!(parse_bool("yes"), Some(true));
        assert_eq!(parse_bool("false"), Some(false));
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("off"), Some(false));
        assert_eq!(parse_bool("maybe"), None);
    }
}
