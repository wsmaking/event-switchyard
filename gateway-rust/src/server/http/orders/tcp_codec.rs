use axum::http::StatusCode;

use crate::auth::{AuthError, AuthResult};
use crate::order::OrderType;

use super::{
    AppState, V3_TCP_INTENT_LEN_OFFSET, V3_TCP_KIND_ACCEPT, V3_TCP_KIND_DECODE_ERROR,
    V3_TCP_KIND_KILLED, V3_TCP_KIND_REJECTED, V3_TCP_MODEL_LEN_OFFSET, V3_TCP_PRICE_OFFSET,
    V3_TCP_QTY_OFFSET, V3_TCP_REASON_AUTH_EXPIRED, V3_TCP_REASON_AUTH_INTERNAL,
    V3_TCP_REASON_AUTH_INVALID, V3_TCP_REASON_AUTH_NOT_YET_VALID, V3_TCP_REASON_AUTH_REQUIRED,
    V3_TCP_REASON_BAD_INTENT_LEN, V3_TCP_REASON_BAD_METADATA_UTF8, V3_TCP_REASON_BAD_MODEL_LEN,
    V3_TCP_REASON_BAD_SIDE, V3_TCP_REASON_BAD_SYMBOL, V3_TCP_REASON_BAD_TOKEN_LEN,
    V3_TCP_REASON_BAD_TOKEN_UTF8, V3_TCP_REASON_BAD_TYPE, V3_TCP_REASON_METADATA_WITH_INLINE_TOKEN,
    V3_TCP_REASON_NONE, V3_TCP_REQUEST_SIZE, V3_TCP_RESPONSE_SIZE, V3_TCP_SIDE_OFFSET,
    V3_TCP_SYMBOL_LEN, V3_TCP_SYMBOL_OFFSET, V3_TCP_TOKEN_MAX_LEN, V3_TCP_TOKEN_OFFSET,
    V3_TCP_TYPE_OFFSET, VolatileOrderResponse, support::parse_v3_symbol_key,
    v3_tcp_reason_code_from_reason,
};

#[derive(Debug)]
pub(in crate::server::http) struct V3TcpDecodedRequest<'a> {
    pub(in crate::server::http) jwt_token: Option<&'a str>,
    pub(in crate::server::http) intent_id: Option<&'a str>,
    pub(in crate::server::http) model_id: Option<&'a str>,
    pub(in crate::server::http) symbol_key: [u8; 8],
    pub(in crate::server::http) side: u8,
    pub(in crate::server::http) order_type: OrderType,
    pub(in crate::server::http) qty: u64,
    pub(in crate::server::http) price: u64,
}

pub(in crate::server::http) enum V3TcpDecodedFrame<'a> {
    AuthInit { jwt_token: &'a str },
    Order(V3TcpDecodedRequest<'a>),
}

pub(in crate::server::http) fn decode_v3_tcp_frame<'a>(
    frame: &'a [u8; V3_TCP_REQUEST_SIZE],
) -> Result<V3TcpDecodedFrame<'a>, u32> {
    let token_len =
        u16::from_le_bytes(frame[0..2].try_into().expect("token length bytes")) as usize;
    if token_len > V3_TCP_TOKEN_MAX_LEN {
        return Err(V3_TCP_REASON_BAD_TOKEN_LEN);
    }
    let jwt_token = if token_len == 0 {
        None
    } else {
        let token_end = V3_TCP_TOKEN_OFFSET + token_len;
        let token = std::str::from_utf8(&frame[V3_TCP_TOKEN_OFFSET..token_end])
            .map_err(|_| V3_TCP_REASON_BAD_TOKEN_UTF8)?;
        Some(token)
    };
    let intent_len = u16::from_le_bytes(
        frame[V3_TCP_INTENT_LEN_OFFSET..(V3_TCP_INTENT_LEN_OFFSET + 2)]
            .try_into()
            .expect("intent length bytes"),
    ) as usize;
    let model_len = u16::from_le_bytes(
        frame[V3_TCP_MODEL_LEN_OFFSET..(V3_TCP_MODEL_LEN_OFFSET + 2)]
            .try_into()
            .expect("model length bytes"),
    ) as usize;

    let side_raw = frame[V3_TCP_SIDE_OFFSET];
    let order_type_raw = frame[V3_TCP_TYPE_OFFSET];
    let qty = u64::from_le_bytes(
        frame[V3_TCP_QTY_OFFSET..(V3_TCP_QTY_OFFSET + 8)]
            .try_into()
            .expect("qty bytes"),
    );
    let price_or_reserved = u64::from_le_bytes(
        frame[V3_TCP_PRICE_OFFSET..(V3_TCP_PRICE_OFFSET + 8)]
            .try_into()
            .expect("price bytes"),
    );

    let is_auth_init = side_raw == 0 && order_type_raw == 0 && qty == 0 && price_or_reserved == 0;
    if is_auth_init {
        let jwt_token = jwt_token.ok_or(V3_TCP_REASON_AUTH_REQUIRED)?;
        return Ok(V3TcpDecodedFrame::AuthInit { jwt_token });
    }

    let (intent_id, model_id) = if token_len > 0 {
        if intent_len > 0 || model_len > 0 {
            return Err(V3_TCP_REASON_METADATA_WITH_INLINE_TOKEN);
        }
        (None, None)
    } else {
        if intent_len > V3_TCP_TOKEN_MAX_LEN {
            return Err(V3_TCP_REASON_BAD_INTENT_LEN);
        }
        let model_offset = V3_TCP_TOKEN_OFFSET + intent_len;
        if model_len > V3_TCP_TOKEN_MAX_LEN.saturating_sub(intent_len) {
            return Err(V3_TCP_REASON_BAD_MODEL_LEN);
        }
        let intent_id = if intent_len == 0 {
            None
        } else {
            Some(
                std::str::from_utf8(
                    &frame[V3_TCP_TOKEN_OFFSET..(V3_TCP_TOKEN_OFFSET + intent_len)],
                )
                .map_err(|_| V3_TCP_REASON_BAD_METADATA_UTF8)?,
            )
        };
        let model_id = if model_len == 0 {
            None
        } else {
            Some(
                std::str::from_utf8(&frame[model_offset..(model_offset + model_len)])
                    .map_err(|_| V3_TCP_REASON_BAD_METADATA_UTF8)?,
            )
        };
        (intent_id, model_id)
    };

    let symbol_raw = &frame[V3_TCP_SYMBOL_OFFSET..(V3_TCP_SYMBOL_OFFSET + V3_TCP_SYMBOL_LEN)];
    let symbol_len = symbol_raw
        .iter()
        .position(|b| *b == 0)
        .unwrap_or(symbol_raw.len());
    if symbol_len == 0 {
        return Err(V3_TCP_REASON_BAD_SYMBOL);
    }
    let symbol =
        std::str::from_utf8(&symbol_raw[..symbol_len]).map_err(|_| V3_TCP_REASON_BAD_SYMBOL)?;
    let side = match side_raw {
        1 | 2 => side_raw,
        _ => return Err(V3_TCP_REASON_BAD_SIDE),
    };
    let order_type = match order_type_raw {
        1 => OrderType::Limit,
        2 => OrderType::Market,
        _ => return Err(V3_TCP_REASON_BAD_TYPE),
    };
    let price = if order_type == OrderType::Market {
        0
    } else {
        price_or_reserved
    };
    let symbol_key = parse_v3_symbol_key(symbol).ok_or(V3_TCP_REASON_BAD_SYMBOL)?;

    Ok(V3TcpDecodedFrame::Order(V3TcpDecodedRequest {
        jwt_token,
        intent_id,
        model_id,
        symbol_key,
        side,
        order_type,
        qty,
        price,
    }))
}

pub(in crate::server::http) fn decode_v3_tcp_request<'a>(
    frame: &'a [u8; V3_TCP_REQUEST_SIZE],
) -> Result<V3TcpDecodedRequest<'a>, u32> {
    match decode_v3_tcp_frame(frame)? {
        V3TcpDecodedFrame::Order(decoded) => Ok(decoded),
        V3TcpDecodedFrame::AuthInit { .. } => Err(V3_TCP_REASON_BAD_TYPE),
    }
}

pub(in crate::server::http) fn authenticate_v3_tcp_token(
    state: &AppState,
    jwt_token: &str,
) -> Result<crate::auth::Principal, (StatusCode, u32)> {
    match state.jwt_auth.authenticate_token(jwt_token) {
        AuthResult::Ok(p) => Ok(p),
        AuthResult::Err(e) => {
            let (status, reason) = match e {
                AuthError::SecretNotConfigured => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    V3_TCP_REASON_AUTH_INTERNAL,
                ),
                AuthError::TokenExpired => (StatusCode::UNAUTHORIZED, V3_TCP_REASON_AUTH_EXPIRED),
                AuthError::TokenNotYetValid => {
                    (StatusCode::UNAUTHORIZED, V3_TCP_REASON_AUTH_NOT_YET_VALID)
                }
                _ => (StatusCode::UNAUTHORIZED, V3_TCP_REASON_AUTH_INVALID),
            };
            Err((status, reason))
        }
    }
}

pub(in crate::server::http) fn encode_v3_tcp_decode_error(
    status: StatusCode,
    reason_code: u32,
    received_at_ns: u64,
) -> [u8; V3_TCP_RESPONSE_SIZE] {
    encode_v3_tcp_response_raw(
        V3_TCP_KIND_DECODE_ERROR,
        status,
        reason_code,
        0,
        0,
        received_at_ns,
    )
}

pub(in crate::server::http) fn encode_v3_tcp_auth_ok(
    received_at_ns: u64,
) -> [u8; V3_TCP_RESPONSE_SIZE] {
    encode_v3_tcp_response_raw(
        V3_TCP_KIND_ACCEPT,
        StatusCode::ACCEPTED,
        V3_TCP_REASON_NONE,
        0,
        0,
        received_at_ns,
    )
}

pub(in crate::server::http) fn encode_v3_tcp_response(
    status: StatusCode,
    resp: &VolatileOrderResponse,
) -> [u8; V3_TCP_RESPONSE_SIZE] {
    encode_v3_tcp_response_raw(
        v3_tcp_kind(resp),
        status,
        v3_tcp_reason_code_from_reason(resp.reason_text()),
        resp.session_seq().unwrap_or(0),
        resp.session_seq().unwrap_or(0),
        resp.received_at_ns(),
    )
}

pub(in crate::server::http) fn encode_v3_tcp_response_raw(
    kind: u8,
    status: StatusCode,
    reason_code: u32,
    session_seq: u64,
    attempt_seq: u64,
    received_at_ns: u64,
) -> [u8; V3_TCP_RESPONSE_SIZE] {
    let mut out = [0u8; V3_TCP_RESPONSE_SIZE];
    out[0] = kind;
    out[1] = 0;
    out[2..4].copy_from_slice(&(status.as_u16()).to_le_bytes());
    out[4..8].copy_from_slice(&reason_code.to_le_bytes());
    out[8..16].copy_from_slice(&session_seq.to_le_bytes());
    out[16..24].copy_from_slice(&attempt_seq.to_le_bytes());
    out[24..32].copy_from_slice(&received_at_ns.to_le_bytes());
    out
}

fn v3_tcp_kind(resp: &VolatileOrderResponse) -> u8 {
    match resp.status_text() {
        "VOLATILE_ACCEPT" => V3_TCP_KIND_ACCEPT,
        "KILLED" => V3_TCP_KIND_KILLED,
        _ => V3_TCP_KIND_REJECTED,
    }
}
