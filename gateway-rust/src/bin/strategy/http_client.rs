use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::intent::StrategyIntent;
use super::replay::StrategyExecutionCatchupInput;

pub const ACCEPT_JSON: &str = "application/json";
pub const ACCEPT_JSON_OR_TEXT: &str = "application/json, text/plain";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedHttpBaseUrl {
    pub host: String,
    pub port: u16,
    pub base_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleHttpResponse {
    pub status_code: u16,
    pub body: Vec<u8>,
}

pub fn parse_http_base_url(raw: &str) -> Result<ParsedHttpBaseUrl, String> {
    let trimmed = raw.trim();
    let rest = trimmed
        .strip_prefix("http://")
        .ok_or_else(|| "only http:// base URLs are supported".to_string())?;
    if rest.is_empty() {
        return Err("base URL host is required".to_string());
    }

    let (host_port, base_path) = match rest.split_once('/') {
        Some((left, right)) => (left, format!("/{}", right.trim_matches('/'))),
        None => (rest, String::new()),
    };
    if host_port.is_empty() {
        return Err("base URL host is required".to_string());
    }
    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() && !port.is_empty() => {
            let port = port
                .parse::<u16>()
                .map_err(|err| format!("invalid port: {err}"))?;
            (host.to_string(), port)
        }
        _ => (host_port.to_string(), 80),
    };

    Ok(ParsedHttpBaseUrl {
        host,
        port,
        base_path: if base_path == "/" {
            String::new()
        } else {
            base_path
        },
    })
}

pub async fn http_get_body(base: &ParsedHttpBaseUrl, path: &str) -> Result<Vec<u8>, String> {
    http_get_body_with_accept(base, path, ACCEPT_JSON).await
}

pub async fn http_get_body_with_accept(
    base: &ParsedHttpBaseUrl,
    path: &str,
    accept: &str,
) -> Result<Vec<u8>, String> {
    let response = http_request(base, "GET", path, None, accept).await?;
    if response.status_code != 200 {
        let message = String::from_utf8_lossy(&response.body);
        return Err(format!("HTTP {}: {}", response.status_code, message));
    }
    Ok(response.body)
}

pub async fn fetch_json<T>(
    base_url: &str,
    path: &str,
    accept: &str,
    decode_context: &str,
) -> Result<T, String>
where
    T: DeserializeOwned,
{
    let parsed = parse_http_base_url(base_url)?;
    let raw = http_get_body_with_accept(&parsed, path, accept).await?;
    serde_json::from_slice::<T>(&raw).map_err(|err| format!("{decode_context} failed: {err}"))
}

pub async fn fetch_catchup_page(
    base_url: &str,
    endpoint_path: &str,
    id: &str,
    after_cursor: u64,
    limit: usize,
) -> Result<StrategyExecutionCatchupInput, String> {
    let parsed = parse_http_base_url(base_url)?;
    let path = format!(
        "{}{endpoint_path}/{id}?afterCursor={after_cursor}&limit={limit}",
        parsed.base_path
    );
    let raw = http_get_body(&parsed, &path).await?;
    serde_json::from_slice::<StrategyExecutionCatchupInput>(&raw)
        .map_err(|err| format!("decode catch-up page failed: {err}"))
}

pub async fn post_strategy_intent_adapt(
    base: &ParsedHttpBaseUrl,
    intent: &StrategyIntent,
) -> Result<SimpleHttpResponse, String> {
    let path = format!("{}{}", base.base_path, "/strategy/intent/adapt");
    let body =
        serde_json::to_vec(intent).map_err(|err| format!("encode adapt request failed: {err}"))?;
    http_request(base, "POST", &path, Some(&body), ACCEPT_JSON).await
}

pub async fn post_strategy_intent_submit(
    base: &ParsedHttpBaseUrl,
    intent: &StrategyIntent,
) -> Result<SimpleHttpResponse, String> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct SubmitRequest<'a> {
        intent: &'a StrategyIntent,
    }

    let path = format!("{}{}", base.base_path, "/strategy/intent/submit");
    let body = serde_json::to_vec(&SubmitRequest { intent })
        .map_err(|err| format!("encode submit request failed: {err}"))?;
    http_request(base, "POST", &path, Some(&body), ACCEPT_JSON).await
}

pub fn decode_json_or_string(body: &[u8]) -> Value {
    if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(body)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(body).to_string()))
    }
}

pub async fn http_request(
    base: &ParsedHttpBaseUrl,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
    accept: &str,
) -> Result<SimpleHttpResponse, String> {
    let mut stream = TcpStream::connect((base.host.as_str(), base.port))
        .await
        .map_err(|err| format!("connect failed: {err}"))?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nAccept: {accept}\r\nConnection: close\r\n",
        host = base.host
    );
    if let Some(body) = body {
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|err| format!("write request failed: {err}"))?;
    if let Some(body) = body {
        stream
            .write_all(body)
            .await
            .map_err(|err| format!("write request body failed: {err}"))?;
    }
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .map_err(|err| format!("read response failed: {err}"))?;
    parse_http_response(&raw)
}

pub fn parse_http_response(raw: &[u8]) -> Result<SimpleHttpResponse, String> {
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| "invalid HTTP response".to_string())?;
    let (head, body) = raw.split_at(split + 4);
    let head = std::str::from_utf8(head).map_err(|err| format!("invalid HTTP headers: {err}"))?;
    let mut lines = head.split("\r\n").filter(|line| !line.is_empty());
    let status_line = lines
        .next()
        .ok_or_else(|| "missing HTTP status line".to_string())?;
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "missing HTTP status code".to_string())?
        .parse::<u16>()
        .map_err(|err| format!("invalid HTTP status code: {err}"))?;

    let mut headers = HashMap::<String, String>::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    let body = if headers
        .get("transfer-encoding")
        .map(|value| value.eq_ignore_ascii_case("chunked"))
        .unwrap_or(false)
    {
        decode_chunked_body(body)?
    } else {
        body.to_vec()
    };

    Ok(SimpleHttpResponse { status_code, body })
}

pub fn decode_chunked_body(raw: &[u8]) -> Result<Vec<u8>, String> {
    let mut cursor = 0usize;
    let mut out = Vec::new();
    loop {
        let line_end = raw[cursor..]
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|offset| cursor + offset)
            .ok_or_else(|| "invalid chunk header".to_string())?;
        let size_raw = std::str::from_utf8(&raw[cursor..line_end])
            .map_err(|err| format!("invalid chunk size: {err}"))?;
        let size_hex = size_raw
            .split(';')
            .next()
            .ok_or_else(|| "invalid chunk size".to_string())?;
        let size = usize::from_str_radix(size_hex.trim(), 16)
            .map_err(|err| format!("invalid chunk size: {err}"))?;
        cursor = line_end + 2;
        if size == 0 {
            return Ok(out);
        }
        let end = cursor
            .checked_add(size)
            .ok_or_else(|| "chunk body overflow".to_string())?;
        if end + 2 > raw.len() {
            return Err("truncated chunk body".to_string());
        }
        out.extend_from_slice(&raw[cursor..end]);
        if &raw[end..end + 2] != b"\r\n" {
            return Err("invalid chunk terminator".to_string());
        }
        cursor = end + 2;
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_chunked_body, parse_http_base_url, parse_http_response};

    #[test]
    fn parse_http_base_url_supports_port_and_prefix() {
        let parsed = parse_http_base_url("http://127.0.0.1:8081/api").expect("parse");
        assert_eq!(parsed.host, "127.0.0.1");
        assert_eq!(parsed.port, 8081);
        assert_eq!(parsed.base_path, "/api");
    }

    #[test]
    fn decode_chunked_body_round_trips() {
        let raw = b"4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n";
        let body = decode_chunked_body(raw).expect("decode chunked");
        assert_eq!(body, b"Wikipedia");
    }

    #[test]
    fn parse_http_response_handles_chunked_json() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n{\"ok\"\r\n3\r\n:1}\r\n0\r\n\r\n";
        let response = parse_http_response(raw).expect("parse response");
        assert_eq!(response.status_code, 200);
        assert_eq!(response.body, br#"{"ok":1}"#);
    }
}
