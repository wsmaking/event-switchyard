use super::orders::{
    self, V3_TCP_REASON_AUTH_REQUIRED, V3_TCP_REASON_AUTH_UNEXPECTED_TOKEN, V3_TCP_REQUEST_SIZE,
    V3TcpDecodedFrame, authenticate_v3_tcp_token, decode_v3_tcp_frame, encode_v3_tcp_auth_ok,
    encode_v3_tcp_decode_error, process_order_v3_hot_path_tcp,
};
use super::{AppState, parse_bool_env};
use axum::http::StatusCode;
use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::info;

pub(super) async fn run_v3_tcp_server(port: u16, state: AppState) -> anyhow::Result<()> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    let tcp_busy_poll_us = std::env::var("V3_TCP_BUSY_POLL_US")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);
    #[cfg(target_os = "linux")]
    if tcp_busy_poll_us > 0 {
        info!(
            busy_poll_us = tcp_busy_poll_us,
            "v3 tcp busy-poll is enabled (SO_BUSY_POLL)"
        );
    }
    #[cfg(not(target_os = "linux"))]
    if tcp_busy_poll_us > 0 {
        info!(
            busy_poll_us = tcp_busy_poll_us,
            "V3_TCP_BUSY_POLL_US is set but ignored on non-Linux"
        );
    }
    info!("v3 TCP server listening on {}", addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        let conn_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_v3_tcp_connection(socket, conn_state, tcp_busy_poll_us).await {
                tracing::warn!(peer = %peer, error = %err, "v3 tcp connection error");
            }
        });
    }
}

#[cfg(target_os = "linux")]
static V3_TCP_BUSY_POLL_WARNED: AtomicBool = AtomicBool::new(false);

#[cfg(target_os = "linux")]
fn configure_v3_tcp_busy_poll(socket: &tokio::net::TcpStream, busy_poll_us: u32) {
    if busy_poll_us == 0 {
        return;
    }
    use std::os::fd::AsRawFd;
    let fd = socket.as_raw_fd();
    let value: libc::c_int = busy_poll_us.min(libc::c_int::MAX as u32) as libc::c_int;
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_BUSY_POLL,
            (&value as *const libc::c_int).cast::<libc::c_void>(),
            std::mem::size_of_val(&value) as libc::socklen_t,
        )
    };
    if rc != 0 && !V3_TCP_BUSY_POLL_WARNED.swap(true, Ordering::Relaxed) {
        let err = std::io::Error::last_os_error();
        tracing::warn!(error = %err, busy_poll_us = value, "failed to set SO_BUSY_POLL");
    }
}

#[cfg(not(target_os = "linux"))]
fn configure_v3_tcp_busy_poll(_socket: &tokio::net::TcpStream, _busy_poll_us: u32) {}

async fn handle_v3_tcp_connection(
    mut socket: tokio::net::TcpStream,
    state: AppState,
    tcp_busy_poll_us: u32,
) -> anyhow::Result<()> {
    socket.set_nodelay(true)?;
    configure_v3_tcp_busy_poll(&socket, tcp_busy_poll_us);
    let auth_cache_enabled = parse_bool_env("V3_TCP_AUTH_CACHE_ENABLE").unwrap_or(true);
    let sticky_auth_context = parse_bool_env("V3_TCP_AUTH_STICKY_CONTEXT").unwrap_or(true);
    let auth_cache_capacity = std::env::var("V3_TCP_AUTH_CACHE_CAPACITY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(256);
    let auth_init_required = parse_bool_env("V3_TCP_AUTH_INIT_REQUIRED").unwrap_or(false);
    let mut auth_cache: HashMap<String, crate::auth::Principal> = if auth_cache_enabled {
        HashMap::with_capacity(auth_cache_capacity.min(1024))
    } else {
        HashMap::new()
    };
    let mut sticky_token = String::new();
    let mut sticky_principal: Option<crate::auth::Principal> = None;
    let mut auth_init_principal: Option<crate::auth::Principal> = None;
    let mut req = [0u8; V3_TCP_REQUEST_SIZE];
    loop {
        match socket.read_exact(&mut req).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let t0 = gateway_core::now_nanos();
        let resp = match decode_v3_tcp_frame(&req) {
            Ok(V3TcpDecodedFrame::AuthInit { jwt_token }) => {
                let principal = if auth_cache_enabled {
                    if let Some(cached) = auth_cache.get(jwt_token) {
                        Ok(cached.clone())
                    } else {
                        match authenticate_v3_tcp_token(&state, jwt_token) {
                            Ok(principal) => {
                                if auth_cache.len() >= auth_cache_capacity {
                                    auth_cache.clear();
                                }
                                auth_cache.insert(jwt_token.to_string(), principal.clone());
                                Ok(principal)
                            }
                            Err(err) => Err(err),
                        }
                    }
                } else {
                    authenticate_v3_tcp_token(&state, jwt_token)
                };
                match principal {
                    Ok(principal) => {
                        auth_init_principal = Some(principal.clone());
                        if sticky_auth_context {
                            sticky_token.clear();
                            sticky_token.push_str(jwt_token);
                            sticky_principal = Some(principal);
                        }
                        encode_v3_tcp_auth_ok(t0)
                    }
                    Err((status, reason_code)) => {
                        auth_init_principal = None;
                        if sticky_auth_context {
                            sticky_principal = None;
                            sticky_token.clear();
                        }
                        encode_v3_tcp_decode_error(status, reason_code, t0)
                    }
                }
            }
            Ok(V3TcpDecodedFrame::Order(decoded)) => {
                if auth_init_required {
                    if decoded.jwt_token.is_some() {
                        encode_v3_tcp_decode_error(
                            StatusCode::UNPROCESSABLE_ENTITY,
                            V3_TCP_REASON_AUTH_UNEXPECTED_TOKEN,
                            t0,
                        )
                    } else {
                        match auth_init_principal.as_ref() {
                            Some(principal) => process_order_v3_hot_path_tcp(
                                &state,
                                &principal.account_id,
                                &principal.session_id,
                                &decoded,
                                t0,
                            ),
                            None => encode_v3_tcp_decode_error(
                                StatusCode::UNAUTHORIZED,
                                V3_TCP_REASON_AUTH_REQUIRED,
                                t0,
                            ),
                        }
                    }
                } else {
                    let principal = match decoded.jwt_token {
                        Some(jwt_token) => {
                            if sticky_auth_context
                                && sticky_principal.is_some()
                                && sticky_token == jwt_token
                            {
                                Ok(sticky_principal.as_ref().expect("checked is_some"))
                            } else if auth_cache_enabled {
                                let mut auth_error: Option<(StatusCode, u32)> = None;
                                if !auth_cache.contains_key(jwt_token) {
                                    match authenticate_v3_tcp_token(&state, jwt_token) {
                                        Ok(principal) => {
                                            if auth_cache.len() >= auth_cache_capacity {
                                                auth_cache.clear();
                                            }
                                            auth_cache.insert(jwt_token.to_string(), principal);
                                        }
                                        Err(err) => {
                                            if sticky_auth_context {
                                                sticky_principal = None;
                                                sticky_token.clear();
                                            }
                                            auth_error = Some(err);
                                        }
                                    }
                                }
                                if let Some(err) = auth_error {
                                    Err(err)
                                } else {
                                    match auth_cache.get(jwt_token) {
                                        Some(cached) => Ok(cached),
                                        None => Err((
                                            StatusCode::UNAUTHORIZED,
                                            orders::V3_TCP_REASON_AUTH_INVALID,
                                        )),
                                    }
                                }
                            } else {
                                match authenticate_v3_tcp_token(&state, jwt_token) {
                                    Ok(principal) => {
                                        sticky_principal = Some(principal);
                                        Ok(sticky_principal.as_ref().expect("set above"))
                                    }
                                    Err(err) => Err(err),
                                }
                            }
                        }
                        None => {
                            if sticky_auth_context {
                                match sticky_principal.as_ref() {
                                    Some(principal) => Ok(principal),
                                    None => Err((
                                        StatusCode::UNAUTHORIZED,
                                        orders::V3_TCP_REASON_AUTH_INVALID,
                                    )),
                                }
                            } else {
                                Err((StatusCode::UNAUTHORIZED, orders::V3_TCP_REASON_AUTH_INVALID))
                            }
                        }
                    };
                    match principal {
                        Ok(principal) => {
                            let resp = process_order_v3_hot_path_tcp(
                                &state,
                                &principal.account_id,
                                &principal.session_id,
                                &decoded,
                                t0,
                            );
                            if sticky_auth_context {
                                if let Some(jwt_token) = decoded.jwt_token {
                                    if sticky_principal.is_none() || sticky_token != jwt_token {
                                        sticky_token.clear();
                                        sticky_token.push_str(jwt_token);
                                        sticky_principal = Some(principal.clone());
                                    }
                                }
                            }
                            resp
                        }
                        Err((status, reason_code)) => {
                            encode_v3_tcp_decode_error(status, reason_code, t0)
                        }
                    }
                }
            }
            Err(code) => encode_v3_tcp_decode_error(StatusCode::UNPROCESSABLE_ENTITY, code, t0),
        };
        socket.write_all(&resp).await?;
    }
    Ok(())
}
