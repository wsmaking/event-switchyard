package ai.assist.audit;

import ai.assist.api.model.RiskNavigatorCheckRequest;
import ai.assist.domain.RuleEvaluation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AiAuditLogger {
    private static final Logger log = LoggerFactory.getLogger(AiAuditLogger.class);

    public void logRiskCheck(
            String requestId,
            RiskNavigatorCheckRequest request,
            RuleEvaluation evaluation,
            String suggestionSource,
            boolean llmUsed,
            boolean fallbackUsed,
            String fallbackReason,
            long processingMs
    ) {
        log.info(
                "event=ai_risk_check request_id={} account_id={} symbol={} side={} qty={} price={} decision={} source={} llm_used={} fallback_used={} fallback_reason={} processing_ms={} reasons={}",
                requestId,
                request.accountId(),
                request.symbol(),
                request.side(),
                request.qty(),
                request.price(),
                evaluation.decision(),
                suggestionSource,
                llmUsed,
                fallbackUsed,
                fallbackReason,
                processingMs,
                evaluation.reasons()
        );
    }
}

