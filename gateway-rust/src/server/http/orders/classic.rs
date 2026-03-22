use super::*;

/// 注文受付（POST /orders）
/// - JWT検証 → Idempotency-Key → FastPath → 監査/Bus/Snapshot 保存
pub(crate) async fn handle_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<OrderRequest>,
) -> OrderResponseResult {
    handle_order_with_contract(state, headers, req, OrderIngressContract::Legacy).await
}

async fn handle_order_with_contract(
    state: AppState,
    headers: HeaderMap,
    req: OrderRequest,
    contract: OrderIngressContract,
) -> OrderResponseResult {
    if contract == OrderIngressContract::V2 {
        state.v2_requests_total.fetch_add(1, Ordering::Relaxed);
    }
    let t0 = now_nanos();
    let principal = authenticate_request(&state, &headers, t0)?;
    let client_order_id = req.client_order_id.clone();
    let idempotency_key = build_idempotency_key(&headers, &req);

    if idempotency_key.is_none() {
        record_ack(&state, t0);
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(OrderResponse::rejected("IDEMPOTENCY_REQUIRED")),
        ));
    }

    if let Some(reason) = validate_order_request(&req) {
        match reason {
            "INVALID_QTY" => {
                state.reject_invalid_qty.fetch_add(1, Ordering::Relaxed);
            }
            "INVALID_SYMBOL" => {
                state.reject_invalid_symbol.fetch_add(1, Ordering::Relaxed);
            }
            "INVALID_PRICE" => {
                state.reject_invalid_qty.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        record_ack(&state, t0);
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(OrderResponse::rejected(reason)),
        ));
    }

    if let Some(ref rate_limiter) = state.rate_limiter {
        if !rate_limiter.try_acquire(&principal.account_id) {
            state.reject_rate_limit.fetch_add(1, Ordering::Relaxed);
            record_ack(&state, t0);
            return Ok((
                StatusCode::TOO_MANY_REQUESTS,
                Json(OrderResponse::rejected("RATE_LIMITED")),
            ));
        }
    }

    let (mut inflight_guard, inflight) = match reserve_inflight(&state, t0).await {
        Ok(v) => v,
        Err((status, body)) => return Ok((status, body)),
    };
    if let Err((status, body)) = apply_backpressure(&state, t0, inflight) {
        return Ok((status, body));
    }

    let symbol = FastPathEngine::symbol_to_bytes(&req.symbol);

    let account_id = principal.account_id.clone();
    let account_id_num: u64 = account_id.parse().unwrap_or(0);
    let price = req.price.unwrap_or(0);

    let key = idempotency_key
        .as_ref()
        .expect("idempotency key is validated above");
    state.idempotency_checked.fetch_add(1, Ordering::Relaxed);
    let order_id = format!("ord_{}", uuid::Uuid::new_v4());
    let internal_order_id = state.order_id_seq.fetch_add(1, Ordering::Relaxed);
    let mut process_result = ProcessResult::ErrorQueueFull;

    let outcome = state
        .sharded_store
        .get_or_create_idempotency(&account_id, key, || {
            let result = state.engine.process_order(
                internal_order_id,
                account_id_num,
                symbol,
                req.side_byte(),
                req.qty as u32,
                price,
            );
            process_result = result.clone();
            if result == ProcessResult::Accepted {
                Some(OrderSnapshot::new(
                    order_id.clone(),
                    account_id.clone(),
                    req.symbol.clone(),
                    req.side.clone(),
                    req.order_type,
                    req.qty,
                    req.price,
                    req.time_in_force,
                    req.expire_at,
                    req.client_order_id.clone(),
                ))
            } else {
                None
            }
        });

    match outcome {
        crate::store::IdempotencyOutcome::Existing(existing) => {
            state.idempotency_hits.fetch_add(1, Ordering::Relaxed);
            let accept_seq = state.order_id_map.to_internal(&existing.order_id);
            let request_id = build_request_id(accept_seq);
            let (status, response) =
                map_existing_response(contract, &state, &existing, accept_seq, request_id);
            record_ack(&state, t0);
            Ok((status, Json(response)))
        }
        crate::store::IdempotencyOutcome::Created(snapshot) => {
            state.idempotency_creates.fetch_add(1, Ordering::Relaxed);
            state.order_id_map.register_with_internal(
                internal_order_id,
                snapshot.order_id.clone(),
                account_id,
            );

            let order_payload = serde_json::json!({
                "symbol": snapshot.symbol.clone(),
                "side": snapshot.side.clone(),
                "type": match snapshot.order_type {
                    crate::order::OrderType::Limit => "LIMIT",
                    crate::order::OrderType::Market => "MARKET",
                },
                "qty": snapshot.qty,
                "price": snapshot.price,
                "timeInForce": match snapshot.time_in_force {
                    crate::order::TimeInForce::Gtc => "GTC",
                    crate::order::TimeInForce::Gtd => "GTD",
                    crate::order::TimeInForce::Ioc => "IOC",
                    crate::order::TimeInForce::Fok => "FOK",
                },
                "expireAt": snapshot.expire_at,
                "clientOrderId": snapshot.client_order_id.clone(),
            });
            let bus_account_id = snapshot.account_id.clone();
            let bus_order_id = snapshot.order_id.clone();
            let bus_data = order_payload.clone();

            let audit_event_at = audit::now_millis();
            let audit_event = AuditEvent {
                event_type: "OrderAccepted".into(),
                at: audit_event_at,
                account_id: snapshot.account_id.clone(),
                order_id: Some(snapshot.order_id.clone()),
                data: order_payload,
            };
            let (timings, durable_receipt_rx) = if contract == OrderIngressContract::V2 {
                let append = state.audit_log.append_with_durable_receipt(audit_event, t0);
                (append.timings, append.durable_rx)
            } else {
                (state.audit_log.append_with_timings(audit_event, t0), None)
            };
            record_wal_enqueue(&state, t0, timings);
            if contract == OrderIngressContract::V2 {
                if timings.durable_done_ns > 0 {
                    finalize_sync_durable_v2(
                        &state,
                        &snapshot,
                        audit_event_at,
                        t0,
                        timings,
                        &mut inflight_guard,
                    );
                } else if timings.enqueue_done_ns == 0 {
                    state.sharded_store.remove(
                        &snapshot.order_id,
                        &snapshot.account_id,
                        Some(key.as_str()),
                    );
                    state.order_id_map.remove(internal_order_id);
                    record_ack(&state, t0);
                    return Ok((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(OrderResponse::rejected("WAL_DURABILITY_FAILED")),
                    ));
                } else if state.audit_log.async_enabled() {
                    if let Some(guard) = inflight_guard.as_mut() {
                        guard.disarm();
                    }
                    if let Some(rx) = durable_receipt_rx {
                        let timeout =
                            Duration::from_millis(state.v2_durable_wait_timeout_ms.max(1));
                        match tokio::time::timeout(timeout, rx).await {
                            Ok(Ok(receipt)) => {
                                if receipt.durable_done_ns == 0 {
                                    state.sharded_store.remove(
                                        &snapshot.order_id,
                                        &snapshot.account_id,
                                        Some(key.as_str()),
                                    );
                                    state.order_id_map.remove(internal_order_id);
                                    record_ack(&state, t0);
                                    return Ok((
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        Json(OrderResponse::rejected("WAL_DURABILITY_FAILED")),
                                    ));
                                }
                                finalize_sync_durable_v2(
                                    &state,
                                    &snapshot,
                                    audit_event_at,
                                    t0,
                                    audit::AuditAppendTimings {
                                        enqueue_done_ns: timings.enqueue_done_ns,
                                        durable_done_ns: receipt.durable_done_ns,
                                        fdatasync_ns: receipt.fdatasync_ns,
                                    },
                                    &mut inflight_guard,
                                );
                            }
                            Ok(Err(_)) => {
                                record_ack(&state, t0);
                                return Ok((
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    Json(OrderResponse::rejected("WAL_DURABILITY_FAILED")),
                                ));
                            }
                            Err(_) => {
                                state
                                    .v2_durable_wait_timeout_total
                                    .fetch_add(1, Ordering::Relaxed);
                                record_ack(&state, t0);
                                return Ok((
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    Json(OrderResponse::rejected("DURABILITY_WAIT_TIMEOUT")),
                                ));
                            }
                        }
                    } else {
                        state.sharded_store.remove(
                            &snapshot.order_id,
                            &snapshot.account_id,
                            Some(key.as_str()),
                        );
                        state.order_id_map.remove(internal_order_id);
                        record_ack(&state, t0);
                        return Ok((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(OrderResponse::rejected("WAL_DURABILITY_FAILED")),
                        ));
                    }
                } else {
                    state.sharded_store.remove(
                        &snapshot.order_id,
                        &snapshot.account_id,
                        Some(key.as_str()),
                    );
                    state.order_id_map.remove(internal_order_id);
                    record_ack(&state, t0);
                    return Ok((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(OrderResponse::rejected("WAL_DURABILITY_FAILED")),
                    ));
                }
            } else if state.audit_log.async_enabled() {
                if let Some(guard) = inflight_guard.as_mut() {
                    guard.disarm();
                }
            }
            if !state.bus_mode_outbox {
                state.bus_publisher.publish(BusEvent {
                    event_type: "OrderAccepted".into(),
                    at: crate::bus::format_event_time(audit::now_millis()),
                    account_id: bus_account_id,
                    order_id: Some(bus_order_id),
                    data: bus_data,
                });
            }
            record_ack(&state, t0);
            let accept_seq = Some(internal_order_id);
            let request_id = build_request_id(accept_seq);
            let (status, response) =
                map_created_response(contract, &snapshot, accept_seq, request_id);
            Ok((status, Json(response)))
        }
        crate::store::IdempotencyOutcome::NotCreated => {
            let (status, response) = match process_result {
                ProcessResult::RejectedMaxQty => (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    OrderResponse::rejected("INVALID_QTY"),
                ),
                ProcessResult::RejectedMaxNotional => (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    OrderResponse::rejected("RISK_REJECT"),
                ),
                ProcessResult::RejectedDailyLimit => (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    OrderResponse::rejected("RISK_REJECT"),
                ),
                ProcessResult::RejectedUnknownSymbol => (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    OrderResponse::rejected("INVALID_SYMBOL"),
                ),
                ProcessResult::ErrorQueueFull => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    OrderResponse::rejected("QUEUE_REJECT"),
                ),
                ProcessResult::Accepted => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    OrderResponse::rejected("ERROR"),
                ),
            };
            match process_result {
                ProcessResult::RejectedMaxQty => {
                    state.reject_invalid_qty.fetch_add(1, Ordering::Relaxed);
                }
                ProcessResult::RejectedMaxNotional | ProcessResult::RejectedDailyLimit => {
                    state.reject_risk.fetch_add(1, Ordering::Relaxed);
                }
                ProcessResult::RejectedUnknownSymbol => {
                    state.reject_invalid_symbol.fetch_add(1, Ordering::Relaxed);
                }
                ProcessResult::ErrorQueueFull => {
                    state.reject_queue_full.fetch_add(1, Ordering::Relaxed);
                }
                ProcessResult::Accepted => {}
            }
            if let Some(ref client_order_id) = client_order_id {
                if process_result != ProcessResult::Accepted {
                    state
                        .sharded_store
                        .mark_rejected_client_order(&principal.account_id, client_order_id);
                }
            }
            record_ack(&state, t0);
            Ok((status, Json(response)))
        }
    }
}

/// v2 注文受付（POST /v2/orders）
/// - `PendingAccepted` の durable 完了まで待って `PENDING` で返す。
pub(crate) async fn handle_order_v2(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<OrderRequest>,
) -> OrderResponseResult {
    handle_order_with_contract(state, headers, req, OrderIngressContract::V2).await
}

/// 注文詳細取得（GET /orders/{order_id}）
pub(crate) async fn handle_get_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
) -> Result<Json<OrderSnapshotResponse>, (StatusCode, Json<AuthErrorResponse>)> {
    let auth_header = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok());
    let principal = match state.jwt_auth.authenticate(auth_header) {
        AuthResult::Ok(p) => p,
        AuthResult::Err(e) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(AuthErrorResponse {
                    error: e.to_string(),
                }),
            ));
        }
    };

    let account_id_from_map = state.order_id_map.get_account_id_by_external(&order_id);
    let order = if let Some(ref acc_id) = account_id_from_map {
        state
            .sharded_store
            .find_by_id_with_account(&order_id, acc_id)
    } else {
        state
            .sharded_store
            .find_by_client_order_id(&principal.account_id, &order_id)
            .or_else(|| state.sharded_store.find_by_id(&order_id))
    };

    let order = match order {
        Some(o) => o,
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(AuthErrorResponse {
                    error: "NOT_FOUND".into(),
                }),
            ));
        }
    };

    if order.account_id != principal.account_id {
        return Err((
            StatusCode::NOT_FOUND,
            Json(AuthErrorResponse {
                error: "NOT_FOUND".into(),
            }),
        ));
    }

    Ok(Json(OrderSnapshotResponse::from(order)))
}

/// v2 注文詳細取得（GET /v2/orders/{order_id}）
pub(crate) async fn handle_get_order_v2(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
) -> Result<Json<OrderSnapshotResponse>, (StatusCode, Json<AuthErrorResponse>)> {
    let inner_state = state.clone();
    let Json(mut snapshot) = handle_get_order(State(inner_state), headers, Path(order_id)).await?;
    map_snapshot_status_to_v2(&state, &mut snapshot);
    Ok(Json(snapshot))
}

/// クライアント注文ID照会（GET /orders/client/{client_order_id}）
pub(crate) async fn handle_get_order_by_client_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(client_order_id): Path<String>,
) -> Result<Json<ClientOrderStatusResponse>, (StatusCode, Json<AuthErrorResponse>)> {
    let auth_header = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok());
    let principal = match state.jwt_auth.authenticate(auth_header) {
        AuthResult::Ok(p) => p,
        AuthResult::Err(e) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(AuthErrorResponse {
                    error: e.to_string(),
                }),
            ));
        }
    };

    if state
        .sharded_store
        .is_rejected_client_order(&principal.account_id, &client_order_id)
    {
        let response = ClientOrderStatusResponse {
            client_order_id,
            order_id: None,
            status: "REJECTED".into(),
        };
        return Ok(Json(response));
    }

    let order = state
        .sharded_store
        .find_by_client_order_id(&principal.account_id, &client_order_id);

    let response = if let Some(order) = order {
        let status = if order.status == crate::store::OrderStatus::Rejected {
            "REJECTED"
        } else if state
            .sharded_store
            .is_durable(&order.order_id, &order.account_id)
        {
            "DURABLE"
        } else {
            "PENDING"
        };
        ClientOrderStatusResponse {
            client_order_id,
            order_id: Some(order.order_id),
            status: status.into(),
        }
    } else {
        ClientOrderStatusResponse {
            client_order_id,
            order_id: None,
            status: "UNKNOWN".into(),
        }
    };

    Ok(Json(response))
}

/// 注文訂正（POST /orders/{order_id}/amend）
pub(crate) async fn handle_amend_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
    Json(req): Json<AmendRequest>,
) -> Result<(StatusCode, Json<AmendResponse>), (StatusCode, Json<AuthErrorResponse>)> {
    let auth_header = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok());

    let principal = match state.jwt_auth.authenticate(auth_header) {
        AuthResult::Ok(p) => p,
        AuthResult::Err(e) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(AuthErrorResponse {
                    error: e.to_string(),
                }),
            ));
        }
    };

    let account_id_from_map = state.order_id_map.get_account_id_by_external(&order_id);
    let order = if let Some(ref acc_id) = account_id_from_map {
        state
            .sharded_store
            .find_by_id_with_account(&order_id, acc_id)
    } else {
        state.sharded_store.find_by_id(&order_id)
    };

    let order = match order {
        Some(o) => o,
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(AuthErrorResponse {
                    error: "NOT_FOUND".into(),
                }),
            ));
        }
    };

    if order.account_id != principal.account_id {
        return Err((
            StatusCode::NOT_FOUND,
            Json(AuthErrorResponse {
                error: "NOT_FOUND".into(),
            }),
        ));
    }

    if order.status.is_terminal() || order.status == crate::store::OrderStatus::CancelRequested {
        return Ok((
            StatusCode::CONFLICT,
            Json(AmendResponse {
                order_id,
                status: "REJECTED".into(),
                reason: Some("ORDER_FINAL".into()),
            }),
        ));
    }

    if req.new_qty == 0 {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(AmendResponse {
                order_id,
                status: "REJECTED".into(),
                reason: Some("INVALID_QTY".into()),
            }),
        ));
    }

    if req.new_price == 0 {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(AmendResponse {
                order_id,
                status: "REJECTED".into(),
                reason: Some("INVALID_PRICE".into()),
            }),
        ));
    }

    if order.status == crate::store::OrderStatus::AmendRequested {
        return Ok((
            StatusCode::ACCEPTED,
            Json(AmendResponse {
                order_id,
                status: "AMEND_REQUESTED".into(),
                reason: None,
            }),
        ));
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let updated = state
        .sharded_store
        .update(&order.order_id, &order.account_id, |prev| {
            let mut next = prev.clone();
            next.qty = req.new_qty;
            next.price = Some(req.new_price);
            next.status = crate::store::OrderStatus::AmendRequested;
            next.last_update_at = now_ms;
            next
        });

    if updated.is_none() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(AuthErrorResponse {
                error: "NOT_FOUND".into(),
            }),
        ));
    }

    let amend_data = serde_json::json!({
        "newQty": req.new_qty,
        "newPrice": req.new_price,
        "comment": req.comment,
    });
    state.audit_log.append(AuditEvent {
        event_type: "AmendRequested".into(),
        at: audit::now_millis(),
        account_id: order.account_id.clone(),
        order_id: Some(order.order_id.clone()),
        data: amend_data.clone(),
    });
    if !state.bus_mode_outbox {
        state.bus_publisher.publish(BusEvent {
            event_type: "AmendRequested".into(),
            at: crate::bus::format_event_time(audit::now_millis()),
            account_id: order.account_id.clone(),
            order_id: Some(order.order_id.clone()),
            data: amend_data,
        });
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(AmendResponse {
            order_id,
            status: "AMEND_REQUESTED".into(),
            reason: None,
        }),
    ))
}

/// 注文置換（POST /orders/{order_id}/replace）
pub(crate) async fn handle_replace_order(
    state: State<AppState>,
    headers: HeaderMap,
    order_id: Path<String>,
    req: Json<AmendRequest>,
) -> Result<(StatusCode, Json<AmendResponse>), (StatusCode, Json<AuthErrorResponse>)> {
    handle_amend_order(state, headers, order_id, req).await
}

/// 注文キャンセル（POST /orders/{order_id}/cancel）
pub(crate) async fn handle_cancel_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
) -> Result<(StatusCode, Json<CancelResponse>), (StatusCode, Json<AuthErrorResponse>)> {
    let auth_header = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok());
    let principal = match state.jwt_auth.authenticate(auth_header) {
        AuthResult::Ok(p) => p,
        AuthResult::Err(e) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(AuthErrorResponse {
                    error: e.to_string(),
                }),
            ));
        }
    };

    let account_id_from_map = state.order_id_map.get_account_id_by_external(&order_id);
    let order = if let Some(ref acc_id) = account_id_from_map {
        state
            .sharded_store
            .find_by_id_with_account(&order_id, acc_id)
    } else {
        state.sharded_store.find_by_id(&order_id)
    };

    let order = match order {
        Some(o) => o,
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(AuthErrorResponse {
                    error: "NOT_FOUND".into(),
                }),
            ));
        }
    };

    if order.account_id != principal.account_id {
        return Err((
            StatusCode::NOT_FOUND,
            Json(AuthErrorResponse {
                error: "NOT_FOUND".into(),
            }),
        ));
    }

    if order.status.is_terminal() {
        return Ok((
            StatusCode::CONFLICT,
            Json(CancelResponse {
                order_id,
                status: "REJECTED".into(),
                reason: Some("ORDER_FINAL".into()),
            }),
        ));
    }

    if order.status == crate::store::OrderStatus::CancelRequested {
        return Ok((
            StatusCode::ACCEPTED,
            Json(CancelResponse {
                order_id,
                status: "CANCEL_REQUESTED".into(),
                reason: None,
            }),
        ));
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let _ = state
        .sharded_store
        .update(&order.order_id, &order.account_id, |prev| {
            let mut next = prev.clone();
            next.status = crate::store::OrderStatus::CancelRequested;
            next.last_update_at = now_ms;
            next
        });

    state.audit_log.append(AuditEvent {
        event_type: "CancelRequested".into(),
        at: audit::now_millis(),
        account_id: order.account_id.clone(),
        order_id: Some(order.order_id.clone()),
        data: serde_json::json!({}),
    });
    if !state.bus_mode_outbox {
        state.bus_publisher.publish(BusEvent {
            event_type: "CancelRequested".into(),
            at: crate::bus::format_event_time(audit::now_millis()),
            account_id: order.account_id.clone(),
            order_id: Some(order.order_id.clone()),
            data: serde_json::json!({}),
        });
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(CancelResponse {
            order_id,
            status: "CANCEL_REQUESTED".into(),
            reason: None,
        }),
    ))
}
