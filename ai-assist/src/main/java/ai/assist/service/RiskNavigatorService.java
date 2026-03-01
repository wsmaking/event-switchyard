package ai.assist.service;

import ai.assist.api.model.RiskNavigatorCheckRequest;
import ai.assist.api.model.RiskNavigatorCheckResponse;
import ai.assist.audit.AiAuditLogger;
import ai.assist.domain.RuleEvaluation;
import ai.assist.domain.RiskRuleEngine;
import ai.assist.orchestrator.AiContext;
import ai.assist.orchestrator.AiContextBuilder;
import ai.assist.orchestrator.AiOrchestrator;
import ai.assist.orchestrator.OrchestrationResult;
import ai.assist.policy.ResponsePolicyEngine;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class RiskNavigatorService {
    private final RiskRuleEngine ruleEngine;
    private final AiContextBuilder contextBuilder;
    private final AiOrchestrator orchestrator;
    private final ResponsePolicyEngine policyEngine;
    private final AiAuditLogger auditLogger;
    private final MeterRegistry meterRegistry;

    public RiskNavigatorService(
            RiskRuleEngine ruleEngine,
            AiContextBuilder contextBuilder,
            AiOrchestrator orchestrator,
            ResponsePolicyEngine policyEngine,
            AiAuditLogger auditLogger,
            MeterRegistry meterRegistry
    ) {
        this.ruleEngine = ruleEngine;
        this.contextBuilder = contextBuilder;
        this.orchestrator = orchestrator;
        this.policyEngine = policyEngine;
        this.auditLogger = auditLogger;
        this.meterRegistry = meterRegistry;
    }

    public RiskNavigatorCheckResponse check(String requestId, RiskNavigatorCheckRequest request) {
        long startNs = System.nanoTime();
        RuleEvaluation evaluation = ruleEngine.evaluate(request);
        AiContext context = contextBuilder.build(request);
        OrchestrationResult orchestration = orchestrator.suggest(request, evaluation, context);
        String safeSuggestion = policyEngine.apply(orchestration.suggestion());
        long processingMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);

        recordMetrics(evaluation, orchestration, processingMs);
        auditLogger.logRiskCheck(
                requestId,
                request,
                evaluation,
                orchestration.source(),
                orchestration.llmUsed(),
                orchestration.fallbackUsed(),
                orchestration.fallbackReason(),
                processingMs
        );

        return new RiskNavigatorCheckResponse(
                requestId,
                evaluation.decision().name(),
                evaluation.reasons(),
                safeSuggestion,
                orchestration.source(),
                orchestration.llmUsed(),
                orchestration.fallbackUsed(),
                orchestration.fallbackReason(),
                processingMs
        );
    }

    private void recordMetrics(RuleEvaluation evaluation, OrchestrationResult result, long processingMs) {
        Counter.builder("ai_assist_risk_check_total")
                .tag("decision", evaluation.decision().name())
                .tag("source", result.source())
                .register(meterRegistry)
                .increment();

        if (result.fallbackUsed()) {
            Counter.builder("ai_assist_risk_fallback_total")
                    .tag("reason", result.fallbackReason())
                    .register(meterRegistry)
                    .increment();
        }

        Timer.builder("ai_assist_risk_check_latency")
                .publishPercentileHistogram()
                .register(meterRegistry)
                .record(processingMs, TimeUnit.MILLISECONDS);
    }
}

