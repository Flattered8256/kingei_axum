use chrono::{Duration, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};

use crate::storage::{PRESIGN_TTL, StorageError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageOp {
    Put,
    Get,
}

#[derive(Debug, Serialize, Deserialize)]
struct StorageClaims {
    file_id: i64,
    key: String,
    op: StorageOp,
    iat: i64,
    exp: i64,
}

pub fn sign(secret: &str, file_id: i64, key: &str, op: StorageOp) -> Result<String, StorageError> {
    let now = Utc::now();
    let claims = StorageClaims {
        file_id,
        key: key.to_string(),
        op,
        iat: now.timestamp(),
        exp: (now + Duration::seconds(PRESIGN_TTL.as_secs() as i64)).timestamp(),
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| StorageError::Token(e.to_string()))
}

pub fn verify(
    secret: &str,
    token: &str,
    file_id: i64,
    key: &str,
    op: StorageOp,
) -> Result<(), StorageError> {
    let data = decode::<StorageClaims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &Validation::new(Algorithm::HS256),
    )
    .map_err(|_| StorageError::InvalidToken)?;

    if data.claims.file_id != file_id || data.claims.key != key || data.claims.op != op {
        return Err(StorageError::InvalidToken);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "test-secret";
    const KEY: &str = "11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222";

    #[test]
    fn roundtrip() {
        let token = sign(SECRET, 7, KEY, StorageOp::Put).unwrap();
        assert!(verify(SECRET, &token, 7, KEY, StorageOp::Put).is_ok());
    }

    #[test]
    fn reject_wrong_op() {
        let token = sign(SECRET, 7, KEY, StorageOp::Put).unwrap();
        assert!(matches!(
            verify(SECRET, &token, 7, KEY, StorageOp::Get),
            Err(StorageError::InvalidToken)
        ));
    }

    #[test]
    fn reject_wrong_file_or_key() {
        let token = sign(SECRET, 7, KEY, StorageOp::Get).unwrap();
        assert!(matches!(
            verify(SECRET, &token, 8, KEY, StorageOp::Get),
            Err(StorageError::InvalidToken)
        ));
        assert!(matches!(
            verify(SECRET, &token, 7, "other", StorageOp::Get),
            Err(StorageError::InvalidToken)
        ));
        assert!(matches!(
            verify("other-secret", &token, 7, KEY, StorageOp::Get),
            Err(StorageError::InvalidToken)
        ));
    }
}
