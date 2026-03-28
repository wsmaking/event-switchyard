use super::*;
use crate::server::http::V3AccountSymbolKey;

#[tokio::test]
async fn v3_hot_risk_fixture_matches_rust_implementation() {
    let fixture: RiskDecisionFixture = load_fixture("contracts/fixtures/risk_decision_v1.json");
    let mut mismatches = Vec::new();

    for case in fixture.cases {
        let mut state = build_test_state();
        state.v3_risk_strict_symbols = case.input.strict_symbols;
        state.v3_risk_max_order_qty = 100_000_000;
        state.v3_risk_max_notional = 1_000_000_000;

        let mut symbol_limits = HashMap::new();
        if case.input.symbol_known {
            if let Some(symbol_key) = parse_v3_symbol_key(&case.input.symbol) {
                symbol_limits.insert(
                    symbol_key,
                    gateway_core::SymbolLimits {
                        max_order_qty: state.v3_risk_max_order_qty.min(u32::MAX as u64) as u32,
                        max_notional: state.v3_risk_max_notional,
                        tick_size: 1,
                    },
                );
            }
        }
        state.v3_symbol_limits = Arc::new(symbol_limits);

        let req = OrderRequest {
            symbol: case.input.symbol.clone(),
            side: case.input.side.clone(),
            order_type: fixture_order_type(&case.input.order_type),
            qty: case.input.qty,
            price: Some(case.input.price),
            time_in_force: TimeInForce::Gtc,
            expire_at: None,
            client_order_id: None,
            intent_id: None,
            model_id: None,
            execution_run_id: None,
            decision_key: None,
            decision_attempt_seq: None,
        };
        let account_ref = state.intern_v3_account_id("fixture-risk");

        let (allowed, reason, http_status) =
            match evaluate_v3_hot_risk(&state, &account_ref, &req, true) {
                Ok(_) => (true, None, StatusCode::ACCEPTED.as_u16()),
                Err(reason) => (
                    false,
                    Some(reason.to_string()),
                    StatusCode::UNPROCESSABLE_ENTITY.as_u16(),
                ),
            };

        if allowed != case.expected.allowed
            || reason.as_ref() != case.expected.reason.as_ref()
            || http_status != case.expected.http_status
        {
            mismatches.push(format!(
                "{} expected=({}, {:?}, {}) actual=({}, {:?}, {})",
                case.id,
                case.expected.allowed,
                case.expected.reason,
                case.expected.http_status,
                allowed,
                reason,
                http_status
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "risk fixture mismatches: {:?}",
        mismatches
    );
}

#[test]
fn v3_reject_reason_fixture_matches_rust_tcp_reason_mapping() {
    let fixture: RejectReasonFixture = load_fixture("contracts/fixtures/reject_reason_v1.json");
    let mut mismatches = Vec::new();

    for case in fixture.reasons {
        let resp = VolatileOrderResponse::rejected("fixture", "REJECTED", &case.reason);
        let actual = v3_tcp_reason_code_from_reason(resp.reason.as_deref());
        if actual != case.tcp_reason_code {
            mismatches.push(format!(
                "{} expected={} actual={}",
                case.reason, case.tcp_reason_code, actual
            ));
        }
    }

    let unknown = VolatileOrderResponse::rejected("fixture", "REJECTED", "UNKNOWN_REASON");
    assert_eq!(
        v3_tcp_reason_code_from_reason(unknown.reason.as_deref()),
        9_999
    );
    assert!(
        mismatches.is_empty(),
        "reject reason fixture mismatches: {:?}",
        mismatches
    );
}

#[test]
fn amend_fixture_matches_phase05_contract_baseline() {
    let fixture: AmendDecisionFixture = load_fixture("contracts/fixtures/amend_decision_v1.json");
    assert!(
        !fixture.cases.is_empty(),
        "amend fixture must contain at least one case"
    );

    let mut reasons = HashSet::new();
    let mut has_amend_requested = false;

    for case in fixture.cases {
        assert_eq!(case.scope, "amend_v1");
        assert!(
            [202, 409, 422].contains(&case.expected.http_status),
            "unexpected http status: {}",
            case.expected.http_status
        );

        if case.expected.allowed {
            assert!(
                case.expected.reason.is_none(),
                "allowed case should not have reason"
            );
        } else {
            assert!(
                case.expected.reason.is_some(),
                "rejected case should have reason"
            );
        }

        if let Some(reason) = case.expected.reason {
            reasons.insert(reason);
        }
        if case.expected.next_status == "AMEND_REQUESTED" {
            has_amend_requested = true;
        }
    }

    assert!(
        has_amend_requested,
        "fixture must include AMEND_REQUESTED transition"
    );
    assert!(
        reasons.contains("ORDER_FINAL"),
        "fixture must include ORDER_FINAL"
    );
    assert!(
        reasons.contains("INVALID_QTY"),
        "fixture must include INVALID_QTY"
    );
    assert!(
        reasons.contains("INVALID_PRICE"),
        "fixture must include INVALID_PRICE"
    );
}

#[test]
fn tif_fixture_matches_phase05_contract_baseline() {
    let fixture: TifPolicyFixture = load_fixture("contracts/fixtures/tif_policy_v1.json");
    assert!(
        !fixture.cases.is_empty(),
        "tif fixture must contain at least one case"
    );

    let mut tifs = HashSet::new();
    let mut reasons = HashSet::new();
    for case in fixture.cases {
        assert_eq!(case.scope, "time_in_force_v1");
        tifs.insert(case.input.time_in_force);

        if case.expected.allowed {
            assert!(
                case.expected.reason.is_none(),
                "allowed case should not have reason"
            );
            assert!(
                case.expected.effective_time_in_force.is_some(),
                "allowed case should have effectiveTimeInForce"
            );
        } else {
            assert!(
                case.expected.reason.is_some(),
                "rejected case should have reason"
            );
        }

        if let Some(reason) = case.expected.reason {
            reasons.insert(reason);
        }
    }

    assert!(tifs.contains("IOC"), "fixture must include IOC");
    assert!(tifs.contains("FOK"), "fixture must include FOK");
    assert!(
        reasons.contains("INVALID_PRICE"),
        "fixture must include INVALID_PRICE scenario"
    );
}

#[tokio::test]
async fn tif_fixture_matches_order_ingress_behavior() {
    let fixture: TifPolicyFixture = load_fixture("contracts/fixtures/tif_policy_v1.json");
    for case in fixture.cases {
        let state = build_test_state();
        let account_id = "4301";
        let idempotency_key = format!("idem_tif_{}", case.id);
        let req = OrderRequest {
            symbol: "AAPL".into(),
            side: "BUY".into(),
            order_type: fixture_order_type(&case.input.order_type),
            qty: 100,
            price: Some(case.input.price),
            time_in_force: fixture_time_in_force(&case.input.time_in_force),
            expire_at: None,
            client_order_id: Some(format!("cid_tif_{}", case.id)),
            intent_id: None,
            model_id: None,
            execution_run_id: None,
            decision_key: None,
            decision_attempt_seq: None,
        };
        let (status, Json(resp)) = handle_order(
            State(state.clone()),
            headers(account_id, Some(idempotency_key.as_str())),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("tif fixture case failed: {}", case.id));

        assert_eq!(
            status.as_u16(),
            case.expected.http_status,
            "case {} http status mismatch",
            case.id
        );

        if case.expected.allowed {
            assert_eq!(resp.status, "ACCEPTED", "case {}", case.id);
            assert!(resp.reason.is_none(), "case {}", case.id);
            let stored = state
                .sharded_store
                .find_by_id_with_account(&resp.order_id, account_id)
                .expect("stored order for allowed tif");
            let expected_tif = case
                .expected
                .effective_time_in_force
                .as_deref()
                .expect("allowed tif must have effective value");
            assert_eq!(
                stored.time_in_force,
                fixture_time_in_force(expected_tif),
                "case {} effective tif mismatch",
                case.id
            );
        } else {
            assert_eq!(resp.status, "REJECTED", "case {}", case.id);
            assert_eq!(
                resp.reason.as_deref(),
                case.expected.reason.as_deref(),
                "case {} reason mismatch",
                case.id
            );
        }
    }
}

#[test]
fn position_cap_fixture_matches_phase05_contract_baseline() {
    let fixture: PositionCapFixture = load_fixture("contracts/fixtures/position_cap_v1.json");
    assert!(
        !fixture.cases.is_empty(),
        "position cap fixture must contain at least one case"
    );

    let mut reasons = HashSet::new();
    for case in fixture.cases {
        assert_eq!(case.scope, "position_cap_v1");
        assert!(
            [202, 422].contains(&case.expected.http_status),
            "unexpected http status: {}",
            case.expected.http_status
        );
        if case.expected.allowed {
            assert!(
                case.expected.reason.is_none(),
                "allowed case should not have reason"
            );
        } else {
            assert!(
                case.expected.reason.is_some(),
                "rejected case should have reason"
            );
        }
        if let Some(reason) = case.expected.reason {
            reasons.insert(reason);
        }
    }

    assert!(
        reasons.contains("POSITION_LIMIT_EXCEEDED"),
        "fixture must include POSITION_LIMIT_EXCEEDED"
    );
    assert!(
        reasons.contains("INVALID_QTY"),
        "fixture must include INVALID_QTY"
    );
    assert!(
        reasons.contains("INVALID_SIDE"),
        "fixture must include INVALID_SIDE"
    );
}

#[tokio::test]
async fn position_cap_fixture_matches_v3_hot_risk_oracle() {
    let fixture: PositionCapFixture = load_fixture("contracts/fixtures/position_cap_v1.json");
    let mut mismatches = Vec::new();

    for case in fixture.cases {
        let mut state = build_test_state();
        state.v3_risk_max_abs_position_qty = case.input.max_abs_position_qty;

        let symbol_key = parse_v3_symbol_key(&case.input.symbol).expect("valid symbol key");
        let account_ref = state.intern_v3_account_id("fixture-position");
        state.v3_account_symbol_position.insert(
            V3AccountSymbolKey::new(Arc::clone(&account_ref), symbol_key),
            case.input.current_position_qty,
        );

        let req = OrderRequest {
            symbol: case.input.symbol.clone(),
            side: case.input.side.clone(),
            order_type: OrderType::Limit,
            qty: case.input.qty,
            price: Some(15_000),
            time_in_force: TimeInForce::Gtc,
            expire_at: None,
            client_order_id: None,
            intent_id: None,
            model_id: None,
            execution_run_id: None,
            decision_key: None,
            decision_attempt_seq: None,
        };

        let (allowed, reason, http_status, projected_position_qty) =
            match evaluate_v3_hot_risk(&state, &account_ref, &req, true) {
                Ok(projection) => (
                    true,
                    None,
                    StatusCode::ACCEPTED.as_u16(),
                    case.input
                        .current_position_qty
                        .saturating_add(projection.delta_qty),
                ),
                Err(reason) => {
                    let projected = if reason == "POSITION_LIMIT_EXCEEDED" {
                        fixture_projected_position_qty(
                            &case.input.side,
                            case.input.current_position_qty,
                            case.input.qty,
                        )
                    } else {
                        case.input.current_position_qty
                    };
                    (
                        false,
                        Some(reason.to_string()),
                        StatusCode::UNPROCESSABLE_ENTITY.as_u16(),
                        projected,
                    )
                }
            };

        if allowed != case.expected.allowed
            || reason.as_ref() != case.expected.reason.as_ref()
            || http_status != case.expected.http_status
            || projected_position_qty != case.expected.projected_position_qty
        {
            mismatches.push(format!(
                "{} expected=({}, {:?}, {}, {}) actual=({}, {:?}, {}, {})",
                case.id,
                case.expected.allowed,
                case.expected.reason,
                case.expected.http_status,
                case.expected.projected_position_qty,
                allowed,
                reason,
                http_status,
                projected_position_qty
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "position fixture mismatches: {:?}",
        mismatches
    );
}
