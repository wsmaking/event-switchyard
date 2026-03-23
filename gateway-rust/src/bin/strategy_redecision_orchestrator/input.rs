use crate::strategy::catchup::{StrategyExecutionCatchupLoop, StrategyExecutionCatchupLoopSnapshot};
use crate::strategy::http_client::{
    fetch_catchup_page as fetch_strategy_catchup_page, http_get_body, parse_http_base_url,
};
use crate::strategy::market_input::StrategyMarketInput;
use crate::strategy::replay::StrategyExecutionCatchupInput;

use super::support::{OrchestratorConfig, OrchestratorRunConfig, RunScope, load_json_file, market_input_url};

pub(super) async fn fetch_catchup_snapshot(
    base_url: &str,
    scope: &RunScope,
    start_cursor: u64,
    limit: usize,
    max_pages: Option<usize>,
) -> Result<(StrategyExecutionCatchupLoopSnapshot, u64, Option<String>), String> {
    let mut loop_state = StrategyExecutionCatchupLoop::new();
    let mut next_cursor = start_cursor;
    let mut pages_read = 0usize;
    let mut incomplete_reason = None;

    loop {
        if let Some(max_pages) = max_pages {
            if pages_read >= max_pages {
                if loop_state.has_more() {
                    incomplete_reason = Some("CATCHUP_MAX_PAGES_REACHED".to_string());
                }
                break;
            }
        }

        let page = fetch_catchup_page(base_url, scope, next_cursor, limit).await?;
        loop_state
            .apply_page(&page)
            .map_err(|err| format!("{err:?}"))?;
        next_cursor = loop_state.next_cursor();
        pages_read = pages_read.saturating_add(1);

        if !page.has_more || !loop_state.has_more() || page.fact_count == 0 {
            break;
        }
    }

    let snapshot = loop_state.snapshot();
    Ok((snapshot, next_cursor, incomplete_reason))
}

pub(super) async fn load_market_input(
    config: &OrchestratorConfig,
    run: &OrchestratorRunConfig,
    scope: &RunScope,
) -> Result<StrategyMarketInput, String> {
    let input = if let Some(url) = market_input_url(config, run, scope)? {
        fetch_market_input_http(&url).await?
    } else if let Some(path) = run.market_input_path.as_deref() {
        load_json_file::<StrategyMarketInput>(path)?
    } else {
        return Err("market input source not configured".to_string());
    };
    input.validate().map_err(|err| err.to_string())?;
    Ok(input)
}

async fn fetch_market_input_http(url: &str) -> Result<StrategyMarketInput, String> {
    let parsed = parse_http_base_url(url)?;
    let path = if parsed.base_path.is_empty() {
        "/".to_string()
    } else {
        parsed.base_path.clone()
    };
    let raw = http_get_body(&parsed, &path).await?;
    serde_json::from_slice::<StrategyMarketInput>(&raw)
        .map_err(|err| format!("decode market input failed: {err}"))
}

async fn fetch_catchup_page(
    base_url: &str,
    scope: &RunScope,
    after_cursor: u64,
    limit: usize,
) -> Result<StrategyExecutionCatchupInput, String> {
    fetch_strategy_catchup_page(
        base_url,
        scope.kind.endpoint_path(),
        &scope.id,
        after_cursor,
        limit,
    )
    .await
}
