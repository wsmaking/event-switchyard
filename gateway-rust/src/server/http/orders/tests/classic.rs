use super::*;

#[tokio::test]
async fn v2_new_and_resend_returns_pending_then_durable() {
    let state = build_test_state();
    let req = request_with_client_id("cid_v2_resend");
    let idem_key = "idem_v2_resend";
    let account_id = "1001";

    let (status1, Json(resp1)) = handle_order_v2(
        State(state.clone()),
        headers(account_id, Some(idem_key)),
        Json(req.clone()),
    )
    .await
    .unwrap_or_else(|_| panic!("first request failed"));
    assert_eq!(status1, StatusCode::ACCEPTED);
    assert_eq!(resp1.status, "PENDING");
    assert!(!resp1.order_id.is_empty());
    let order_id = resp1.order_id.clone();

    let (status2, Json(resp2)) =
        handle_order_v2(State(state), headers(account_id, Some(idem_key)), Json(req))
            .await
            .unwrap_or_else(|_| panic!("second request failed"));
    assert_eq!(status2, StatusCode::OK);
    assert_eq!(resp2.status, "DURABLE");
    assert_eq!(resp2.order_id, order_id);
}

#[tokio::test]
async fn v2_get_order_normalizes_pending_durable_rejected() {
    let state = build_test_state();
    let account_id = "2001";
    let order_id = "ord_v2_norm_1";

    put_order(
        &state,
        order_id,
        account_id,
        Some("cid_v2_norm_1"),
        Some("idem_v2_norm_1"),
    );

    let Json(pending) = handle_get_order_v2(
        State(state.clone()),
        headers(account_id, None),
        Path(order_id.to_string()),
    )
    .await
    .unwrap_or_else(|_| panic!("pending lookup failed"));
    assert_eq!(pending.status, "PENDING");

    assert!(
        state
            .sharded_store
            .mark_durable(order_id, account_id, audit::now_millis())
    );
    let Json(durable) = handle_get_order_v2(
        State(state.clone()),
        headers(account_id, None),
        Path(order_id.to_string()),
    )
    .await
    .unwrap_or_else(|_| panic!("durable lookup failed"));
    assert_eq!(durable.status, "DURABLE");

    let _ = state.sharded_store.update(order_id, account_id, |prev| {
        let mut next = prev.clone();
        next.status = OrderStatus::Rejected;
        next
    });
    let Json(rejected) = handle_get_order_v2(
        State(state),
        headers(account_id, None),
        Path(order_id.to_string()),
    )
    .await
    .unwrap_or_else(|_| panic!("rejected lookup failed"));
    assert_eq!(rejected.status, "REJECTED");
}

#[tokio::test]
async fn v2_get_order_by_client_id_normalizes_states() {
    let state = build_test_state();
    let account_id = "3001";
    let client_order_id = "cid_v2_client_1";
    let order_id = "ord_v2_client_1";

    let Json(unknown) = handle_get_order_by_client_id(
        State(state.clone()),
        headers(account_id, None),
        Path("cid_v2_unknown".to_string()),
    )
    .await
    .unwrap_or_else(|_| panic!("unknown lookup failed"));
    assert_eq!(unknown.status, "UNKNOWN");

    put_order(
        &state,
        order_id,
        account_id,
        Some(client_order_id),
        Some("idem_v2_client_1"),
    );
    let Json(pending) = handle_get_order_by_client_id(
        State(state.clone()),
        headers(account_id, None),
        Path(client_order_id.to_string()),
    )
    .await
    .unwrap_or_else(|_| panic!("pending client lookup failed"));
    assert_eq!(pending.status, "PENDING");

    assert!(
        state
            .sharded_store
            .mark_durable(order_id, account_id, audit::now_millis())
    );
    let Json(durable) = handle_get_order_by_client_id(
        State(state.clone()),
        headers(account_id, None),
        Path(client_order_id.to_string()),
    )
    .await
    .unwrap_or_else(|_| panic!("durable client lookup failed"));
    assert_eq!(durable.status, "DURABLE");

    state
        .sharded_store
        .mark_rejected_client_order(account_id, "cid_v2_rejected");
    let Json(rejected) = handle_get_order_by_client_id(
        State(state),
        headers(account_id, None),
        Path("cid_v2_rejected".to_string()),
    )
    .await
    .unwrap_or_else(|_| panic!("rejected client lookup failed"));
    assert_eq!(rejected.status, "REJECTED");
}

#[tokio::test]
async fn amend_updates_active_order_to_amend_requested() {
    let state = build_test_state();
    let account_id = "4001";
    let order_id = "ord_amend_1";
    put_order(
        &state,
        order_id,
        account_id,
        Some("cid_amend_1"),
        Some("idem_amend_1"),
    );

    let request = AmendRequest {
        new_qty: 123,
        new_price: 15100,
        comment: Some("replace qty/price".into()),
    };
    let (status, Json(resp)) = handle_amend_order(
        State(state.clone()),
        headers(account_id, None),
        Path(order_id.to_string()),
        Json(request),
    )
    .await
    .unwrap_or_else(|_| panic!("amend request failed"));

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(resp.status, "AMEND_REQUESTED");
    assert!(resp.reason.is_none());

    let updated = state
        .sharded_store
        .find_by_id_with_account(order_id, account_id)
        .expect("updated order");
    assert_eq!(updated.status, OrderStatus::AmendRequested);
    assert_eq!(updated.qty, 123);
    assert_eq!(updated.price, Some(15100));
}

#[tokio::test]
async fn amend_handler_matches_amend_fixture_cases() {
    let fixture: AmendDecisionFixture = load_fixture("contracts/fixtures/amend_decision_v1.json");

    for case in fixture.cases {
        let state = build_test_state();
        let account_id = "4101";
        let order_id = format!("ord_amend_fixture_{}", case.id);
        put_order(
            &state,
            &order_id,
            account_id,
            Some(&format!("cid_amend_fixture_{}", case.id)),
            Some(&format!("idem_amend_fixture_{}", case.id)),
        );
        let from_status = fixture_order_status(&case.from_status);
        if from_status != OrderStatus::Accepted {
            let _ = state.sharded_store.update(&order_id, account_id, |prev| {
                let mut next = prev.clone();
                next.status = from_status;
                next
            });
        }

        let req = AmendRequest {
            new_qty: case.input.new_qty,
            new_price: case.input.new_price,
            comment: case.input.comment.clone(),
        };
        let (status, Json(resp)) = handle_amend_order(
            State(state.clone()),
            headers(account_id, None),
            Path(order_id.clone()),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("amend fixture case failed: {}", case.id));

        assert_eq!(
            status.as_u16(),
            case.expected.http_status,
            "case {} http status mismatch",
            case.id
        );
        if case.expected.allowed {
            assert_eq!(resp.status, "AMEND_REQUESTED", "case {}", case.id);
            assert!(resp.reason.is_none(), "case {}", case.id);
        } else {
            assert_eq!(resp.status, "REJECTED", "case {}", case.id);
            assert_eq!(
                resp.reason.as_deref(),
                case.expected.reason.as_deref(),
                "case {} reason mismatch",
                case.id
            );
        }

        let updated = state
            .sharded_store
            .find_by_id_with_account(&order_id, account_id)
            .expect("updated order");
        assert_eq!(
            updated.status.as_str(),
            case.expected.next_status,
            "case {} next status mismatch",
            case.id
        );
    }
}

#[tokio::test]
async fn replace_handler_matches_amend_fixture_cases() {
    let fixture: AmendDecisionFixture = load_fixture("contracts/fixtures/amend_decision_v1.json");

    for case in fixture.cases {
        let state = build_test_state();
        let account_id = "4102";
        let order_id = format!("ord_replace_fixture_{}", case.id);
        put_order(
            &state,
            &order_id,
            account_id,
            Some(&format!("cid_replace_fixture_{}", case.id)),
            Some(&format!("idem_replace_fixture_{}", case.id)),
        );
        let from_status = fixture_order_status(&case.from_status);
        if from_status != OrderStatus::Accepted {
            let _ = state.sharded_store.update(&order_id, account_id, |prev| {
                let mut next = prev.clone();
                next.status = from_status;
                next
            });
        }

        let req = AmendRequest {
            new_qty: case.input.new_qty,
            new_price: case.input.new_price,
            comment: case.input.comment.clone(),
        };
        let (status, Json(resp)) = handle_replace_order(
            State(state.clone()),
            headers(account_id, None),
            Path(order_id.clone()),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("replace fixture case failed: {}", case.id));

        assert_eq!(
            status.as_u16(),
            case.expected.http_status,
            "case {} http status mismatch",
            case.id
        );
        if case.expected.allowed {
            assert_eq!(resp.status, "AMEND_REQUESTED", "case {}", case.id);
            assert!(resp.reason.is_none(), "case {}", case.id);
        } else {
            assert_eq!(resp.status, "REJECTED", "case {}", case.id);
            assert_eq!(
                resp.reason.as_deref(),
                case.expected.reason.as_deref(),
                "case {} reason mismatch",
                case.id
            );
        }

        let updated = state
            .sharded_store
            .find_by_id_with_account(&order_id, account_id)
            .expect("updated order");
        assert_eq!(
            updated.status.as_str(),
            case.expected.next_status,
            "case {} next status mismatch",
            case.id
        );
    }
}

#[tokio::test]
async fn replace_alias_updates_active_order_to_amend_requested() {
    let state = build_test_state();
    let account_id = "4001";
    let order_id = "ord_replace_1";
    put_order(
        &state,
        order_id,
        account_id,
        Some("cid_replace_1"),
        Some("idem_replace_1"),
    );

    let request = AmendRequest {
        new_qty: 77,
        new_price: 14900,
        comment: Some("replace alias".into()),
    };
    let (status, Json(resp)) = handle_replace_order(
        State(state.clone()),
        headers(account_id, None),
        Path(order_id.to_string()),
        Json(request),
    )
    .await
    .unwrap_or_else(|_| panic!("replace request failed"));

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(resp.status, "AMEND_REQUESTED");

    let updated = state
        .sharded_store
        .find_by_id_with_account(order_id, account_id)
        .expect("updated order");
    assert_eq!(updated.status, OrderStatus::AmendRequested);
    assert_eq!(updated.qty, 77);
    assert_eq!(updated.price, Some(14900));
}

#[tokio::test]
async fn cancel_updates_active_order_to_cancel_requested() {
    let state = build_test_state();
    let account_id = "4201";
    let order_id = "ord_cancel_1";
    put_order(
        &state,
        order_id,
        account_id,
        Some("cid_cancel_1"),
        Some("idem_cancel_1"),
    );

    let (status, Json(resp)) = handle_cancel_order(
        State(state.clone()),
        headers(account_id, None),
        Path(order_id.to_string()),
    )
    .await
    .unwrap_or_else(|_| panic!("cancel request failed"));

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(resp.status, "CANCEL_REQUESTED");
    assert!(resp.reason.is_none());

    let updated = state
        .sharded_store
        .find_by_id_with_account(order_id, account_id)
        .expect("updated order");
    assert_eq!(updated.status, OrderStatus::CancelRequested);
}

#[tokio::test]
async fn cancel_is_idempotent_for_cancel_requested_order() {
    let state = build_test_state();
    let account_id = "4202";
    let order_id = "ord_cancel_2";
    put_order(
        &state,
        order_id,
        account_id,
        Some("cid_cancel_2"),
        Some("idem_cancel_2"),
    );
    let _ = state.sharded_store.update(order_id, account_id, |prev| {
        let mut next = prev.clone();
        next.status = OrderStatus::CancelRequested;
        next
    });

    let (status, Json(resp)) = handle_cancel_order(
        State(state.clone()),
        headers(account_id, None),
        Path(order_id.to_string()),
    )
    .await
    .unwrap_or_else(|_| panic!("cancel request failed"));

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(resp.status, "CANCEL_REQUESTED");
    assert!(resp.reason.is_none());

    let updated = state
        .sharded_store
        .find_by_id_with_account(order_id, account_id)
        .expect("updated order");
    assert_eq!(updated.status, OrderStatus::CancelRequested);
}

#[tokio::test]
async fn cancel_rejects_terminal_order_with_conflict() {
    let state = build_test_state();
    let account_id = "4203";
    let order_id = "ord_cancel_3";
    put_order(
        &state,
        order_id,
        account_id,
        Some("cid_cancel_3"),
        Some("idem_cancel_3"),
    );
    let _ = state.sharded_store.update(order_id, account_id, |prev| {
        let mut next = prev.clone();
        next.status = OrderStatus::Filled;
        next
    });

    let (status, Json(resp)) = handle_cancel_order(
        State(state.clone()),
        headers(account_id, None),
        Path(order_id.to_string()),
    )
    .await
    .unwrap_or_else(|_| panic!("cancel request failed"));

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(resp.status, "REJECTED");
    assert_eq!(resp.reason.as_deref(), Some("ORDER_FINAL"));

    let updated = state
        .sharded_store
        .find_by_id_with_account(order_id, account_id)
        .expect("updated order");
    assert_eq!(updated.status, OrderStatus::Filled);
}

#[tokio::test]
async fn amend_rejects_terminal_order_with_conflict() {
    let state = build_test_state();
    let account_id = "4002";
    let order_id = "ord_amend_2";
    put_order(
        &state,
        order_id,
        account_id,
        Some("cid_amend_2"),
        Some("idem_amend_2"),
    );
    let _ = state.sharded_store.update(order_id, account_id, |prev| {
        let mut next = prev.clone();
        next.status = OrderStatus::Filled;
        next
    });

    let request = AmendRequest {
        new_qty: 200,
        new_price: 15200,
        comment: None,
    };
    let (status, Json(resp)) = handle_amend_order(
        State(state),
        headers(account_id, None),
        Path(order_id.to_string()),
        Json(request),
    )
    .await
    .unwrap_or_else(|_| panic!("amend request failed"));

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(resp.status, "REJECTED");
    assert_eq!(resp.reason.as_deref(), Some("ORDER_FINAL"));
}

#[tokio::test]
async fn amend_rejects_cancel_requested_order_with_conflict() {
    let state = build_test_state();
    let account_id = "4004";
    let order_id = "ord_amend_4";
    put_order(
        &state,
        order_id,
        account_id,
        Some("cid_amend_4"),
        Some("idem_amend_4"),
    );
    let _ = state.sharded_store.update(order_id, account_id, |prev| {
        let mut next = prev.clone();
        next.status = OrderStatus::CancelRequested;
        next
    });

    let request = AmendRequest {
        new_qty: 200,
        new_price: 15200,
        comment: None,
    };
    let (status, Json(resp)) = handle_amend_order(
        State(state),
        headers(account_id, None),
        Path(order_id.to_string()),
        Json(request),
    )
    .await
    .unwrap_or_else(|_| panic!("amend request failed"));

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(resp.status, "REJECTED");
    assert_eq!(resp.reason.as_deref(), Some("ORDER_FINAL"));
}

#[tokio::test]
async fn amend_rejects_invalid_qty_and_price() {
    let state = build_test_state();
    let account_id = "4003";
    let order_id = "ord_amend_3";
    put_order(
        &state,
        order_id,
        account_id,
        Some("cid_amend_3"),
        Some("idem_amend_3"),
    );

    let bad_qty = AmendRequest {
        new_qty: 0,
        new_price: 15200,
        comment: None,
    };
    let (status1, Json(resp1)) = handle_amend_order(
        State(state.clone()),
        headers(account_id, None),
        Path(order_id.to_string()),
        Json(bad_qty),
    )
    .await
    .unwrap_or_else(|_| panic!("amend bad qty request failed"));
    assert_eq!(status1, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(resp1.reason.as_deref(), Some("INVALID_QTY"));

    let bad_price = AmendRequest {
        new_qty: 100,
        new_price: 0,
        comment: None,
    };
    let (status2, Json(resp2)) = handle_amend_order(
        State(state),
        headers(account_id, None),
        Path(order_id.to_string()),
        Json(bad_price),
    )
    .await
    .unwrap_or_else(|_| panic!("amend bad price request failed"));
    assert_eq!(status2, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(resp2.reason.as_deref(), Some("INVALID_PRICE"));
}

#[tokio::test]
async fn v2_rolls_back_when_wal_durability_fails() {
    let wal_path =
        std::env::temp_dir().join(format!("gateway-rust-orders-poison-{}.log", now_nanos()));
    let audit_log = Arc::new(AuditLog::new(wal_path).expect("create audit log"));
    audit_log.poison_writer_for_test();
    let state = build_test_state_with_audit_log(audit_log);

    let req = request_with_client_id("cid_v2_poison");
    let (status, Json(resp)) = handle_order_v2(
        State(state.clone()),
        headers("4001", Some("idem_v2_poison")),
        Json(req),
    )
    .await
    .unwrap_or_else(|_| panic!("poison response failed"));
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(resp.status, "REJECTED");
    assert_eq!(resp.reason.as_deref(), Some("WAL_DURABILITY_FAILED"));
    assert_eq!(state.sharded_store.count(), 0);
    assert_eq!(state.order_id_map.count(), 0);
}
