//! JWT 認証モジュール
//!
//! HS256 署名を検証し、accountId / sub を抽出する。
//! Kotlin Gateway と同じ環境変数・ロジックを使用。

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// 認証済みユーザー情報
#[derive(Debug, Clone)]
pub struct Principal {
    pub account_id: String,
    pub session_id: String,
}

/// 認証結果
#[derive(Debug)]
pub enum AuthResult {
    Ok(Principal),
    Err(AuthError),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AuthError {
    SecretNotConfigured,
    MissingBearerToken,
    InvalidJwtFormat,
    InvalidJwtHeader,
    InvalidJwtPayload,
    MissingAlg,
    UnsupportedAlg,
    InvalidJwtSignature,
    MissingExp,
    TokenExpired,
    TokenNotYetValid,
    InvalidIssuer,
    InvalidAudience,
    MissingAccountId,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// JWT 認証器
pub struct JwtAuth {
    secret: Vec<u8>,
    issuer: Option<String>,
    audience: Option<String>,
    clock_skew_sec: i64,
}

impl JwtAuth {
    /// 環境変数から設定を読み込んで初期化
    pub fn from_env() -> Self {
        Self {
            secret: env::var("JWT_HS256_SECRET")
                .unwrap_or_default()
                .into_bytes(),
            issuer: env::var("JWT_ISSUER").ok().filter(|s| !s.is_empty()),
            audience: env::var("JWT_AUDIENCE").ok().filter(|s| !s.is_empty()),
            clock_skew_sec: env::var("JWT_CLOCK_SKEW_SEC")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(secret: &str) -> Self {
        Self {
            secret: secret.as_bytes().to_vec(),
            issuer: None,
            audience: None,
            clock_skew_sec: 30,
        }
    }

    /// Authorization ヘッダーを検証
    pub fn authenticate(&self, authorization_header: Option<&str>) -> AuthResult {
        if self.secret.is_empty() {
            return AuthResult::Err(AuthError::SecretNotConfigured);
        }

        let token = match Self::parse_bearer_token(authorization_header) {
            Some(t) => t,
            None => return AuthResult::Err(AuthError::MissingBearerToken),
        };

        self.verify_hs256(token)
    }

    /// JWTトークン文字列（Bearer接頭辞なし）を直接検証
    pub fn authenticate_token(&self, token: &str) -> AuthResult {
        if self.secret.is_empty() {
            return AuthResult::Err(AuthError::SecretNotConfigured);
        }
        let t = token.trim();
        if t.is_empty() {
            return AuthResult::Err(AuthError::MissingBearerToken);
        }
        self.verify_hs256(t)
    }

    fn parse_bearer_token(header: Option<&str>) -> Option<&str> {
        let h = header?.trim();
        if !h.starts_with("Bearer ") {
            return None;
        }
        let token = h.strip_prefix("Bearer ")?.trim();
        if token.is_empty() {
            return None;
        }
        Some(token)
    }

    fn verify_hs256(&self, token: &str) -> AuthResult {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return AuthResult::Err(AuthError::InvalidJwtFormat);
        }

        let header_b64 = parts[0];
        let payload_b64 = parts[1];
        let sig_b64 = parts[2];

        // ヘッダー検証
        let header_json = match Self::decode_json(header_b64) {
            Some(j) => j,
            None => return AuthResult::Err(AuthError::InvalidJwtHeader),
        };

        let alg = match header_json.get("alg").and_then(|v| v.as_str()) {
            Some(a) => a,
            None => return AuthResult::Err(AuthError::MissingAlg),
        };

        if alg != "HS256" {
            return AuthResult::Err(AuthError::UnsupportedAlg);
        }

        // 署名検証
        let signing_input = format!("{}.{}", header_b64, payload_b64);
        let expected_sig = self.hmac_sha256(signing_input.as_bytes());
        let actual_sig = match Self::decode_b64_url(sig_b64) {
            Some(s) => s,
            None => return AuthResult::Err(AuthError::InvalidJwtSignature),
        };

        if !Self::constant_time_equals(&expected_sig, &actual_sig) {
            return AuthResult::Err(AuthError::InvalidJwtSignature);
        }

        // ペイロード検証
        let payload_json = match Self::decode_json(payload_b64) {
            Some(j) => j,
            None => return AuthResult::Err(AuthError::InvalidJwtPayload),
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // exp チェック
        let exp = match payload_json.get("exp").and_then(|v| v.as_i64()) {
            Some(e) => e,
            None => return AuthResult::Err(AuthError::MissingExp),
        };

        if now > exp + self.clock_skew_sec {
            return AuthResult::Err(AuthError::TokenExpired);
        }

        // nbf チェック
        if let Some(nbf) = payload_json.get("nbf").and_then(|v| v.as_i64()) {
            if now < nbf - self.clock_skew_sec {
                return AuthResult::Err(AuthError::TokenNotYetValid);
            }
        }

        // issuer チェック
        if let Some(ref expected_issuer) = self.issuer {
            let iss = payload_json.get("iss").and_then(|v| v.as_str());
            if iss != Some(expected_issuer.as_str()) {
                return AuthResult::Err(AuthError::InvalidIssuer);
            }
        }

        // audience チェック
        if let Some(ref expected_audience) = self.audience {
            if !Self::payload_has_audience(&payload_json, expected_audience) {
                return AuthResult::Err(AuthError::InvalidAudience);
            }
        }

        // accountId と sub(session) 抽出。
        let sub = payload_json
            .get("sub")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);
        let account_id = payload_json
            .get("accountId")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .or_else(|| sub.clone());

        match account_id {
            Some(id) => {
                let session_id = sub.unwrap_or_else(|| id.clone());
                AuthResult::Ok(Principal {
                    account_id: id,
                    session_id,
                })
            }
            None => AuthResult::Err(AuthError::MissingAccountId),
        }
    }

    fn payload_has_audience(payload: &serde_json::Value, expected: &str) -> bool {
        match payload.get("aud") {
            Some(serde_json::Value::String(s)) => s == expected,
            Some(serde_json::Value::Array(arr)) => arr.iter().any(|v| v.as_str() == Some(expected)),
            _ => false,
        }
    }

    fn decode_json(b64_url: &str) -> Option<serde_json::Value> {
        let bytes = Self::decode_b64_url(b64_url)?;
        serde_json::from_slice(&bytes).ok()
    }

    fn decode_b64_url(s: &str) -> Option<Vec<u8>> {
        URL_SAFE_NO_PAD.decode(s).ok()
    }

    fn hmac_sha256(&self, msg: &[u8]) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("HMAC key error");
        mac.update(msg);
        mac.finalize().into_bytes().to_vec()
    }

    fn constant_time_equals(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        let mut res = 0u8;
        for (x, y) in a.iter().zip(b.iter()) {
            res |= x ^ y;
        }
        res == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_token(secret: &str, payload: &str) -> String {
        let header = r#"{"alg":"HS256","typ":"JWT"}"#;
        let header_b64 = URL_SAFE_NO_PAD.encode(header.as_bytes());
        let payload_b64 = URL_SAFE_NO_PAD.encode(payload.as_bytes());

        let signing_input = format!("{}.{}", header_b64, payload_b64);
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC key error");
        mac.update(signing_input.as_bytes());
        let sig = mac.finalize().into_bytes();
        let sig_b64 = URL_SAFE_NO_PAD.encode(&sig);

        format!("{}.{}.{}", header_b64, payload_b64, sig_b64)
    }

    #[test]
    fn test_valid_token() {
        let secret = "test-secret";
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let payload = format!(r#"{{"accountId":"acc123","exp":{}}}"#, exp);
        let token = make_test_token(secret, &payload);

        let auth = JwtAuth {
            secret: secret.as_bytes().to_vec(),
            issuer: None,
            audience: None,
            clock_skew_sec: 30,
        };

        match auth.authenticate(Some(&format!("Bearer {}", token))) {
            AuthResult::Ok(principal) => {
                assert_eq!(principal.account_id, "acc123");
                assert_eq!(principal.session_id, "acc123");
            }
            AuthResult::Err(e) => panic!("Expected Ok, got {:?}", e),
        }
    }

    #[test]
    fn test_sub_is_used_as_session_id() {
        let secret = "test-secret";
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let payload = format!(r#"{{"accountId":"acc123","sub":"sess-a","exp":{}}}"#, exp);
        let token = make_test_token(secret, &payload);

        let auth = JwtAuth {
            secret: secret.as_bytes().to_vec(),
            issuer: None,
            audience: None,
            clock_skew_sec: 30,
        };

        match auth.authenticate(Some(&format!("Bearer {}", token))) {
            AuthResult::Ok(principal) => {
                assert_eq!(principal.account_id, "acc123");
                assert_eq!(principal.session_id, "sess-a");
            }
            AuthResult::Err(e) => panic!("Expected Ok, got {:?}", e),
        }
    }

    #[test]
    fn test_expired_token() {
        let secret = "test-secret";
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 3600; // 1時間前
        let payload = format!(r#"{{"accountId":"acc123","exp":{}}}"#, exp);
        let token = make_test_token(secret, &payload);

        let auth = JwtAuth {
            secret: secret.as_bytes().to_vec(),
            issuer: None,
            audience: None,
            clock_skew_sec: 30,
        };

        match auth.authenticate(Some(&format!("Bearer {}", token))) {
            AuthResult::Err(AuthError::TokenExpired) => {}
            other => panic!("Expected TokenExpired, got {:?}", other),
        }
    }

    #[test]
    fn test_invalid_signature() {
        let secret = "test-secret";
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let payload = format!(r#"{{"accountId":"acc123","exp":{}}}"#, exp);
        let token = make_test_token("wrong-secret", &payload);

        let auth = JwtAuth {
            secret: secret.as_bytes().to_vec(),
            issuer: None,
            audience: None,
            clock_skew_sec: 30,
        };

        match auth.authenticate(Some(&format!("Bearer {}", token))) {
            AuthResult::Err(AuthError::InvalidJwtSignature) => {}
            other => panic!("Expected InvalidJwtSignature, got {:?}", other),
        }
    }
}
