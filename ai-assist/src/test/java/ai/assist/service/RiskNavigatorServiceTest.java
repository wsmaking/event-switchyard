package ai.assist.service;

import ai.assist.api.model.RiskNavigatorCheckRequest;
import ai.assist.api.model.RiskNavigatorCheckResponse;
import ai.assist.audit.AiAuditLogger;
import ai.assist.domain.RiskRuleEngine;
import ai.assist.llm.NoopLlmClient;
import ai.assist.orchestrator.AiContextBuilder;
import ai.assist.orchestrator.AiOrchestrator;
import ai.assist.policy.ResponsePolicyEngine;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RiskNavigatorServiceTest {
    @Test
    void shouldFallbackWhenLlmDisabled() {
        RiskNavigatorService service = new RiskNavigatorService(
                new RiskRuleEngine(),
                new AiContextBuilder(),
                new AiOrchestrator(new NoopLlmClient(), false, 100),
                new ResponsePolicyEngine(),
                new AiAuditLogger(),
                new SimpleMeterRegistry()
        );

        RiskNavigatorCheckRequest req = new RiskNavigatorCheckRequest(
                "acc-1", "6758.T", "BUY", 100, 12000, Map.of("intent", "entry")
        );

        RiskNavigatorCheckResponse response = service.check("test-req-1", req);

        assertEquals("OK", response.decision());
        assertTrue(response.fallbackUsed());
        assertEquals("LLM_DISABLED", response.fallbackReason());
        assertEquals("RULE_ONLY", response.suggestionSource());
        assertFalse(response.llmUsed());
        assertTrue(response.suggestion().contains("投資助言"));
    }

    @Test
    void shouldShortCircuitOnBlockDecision() {
        RiskNavigatorService service = new RiskNavigatorService(
                new RiskRuleEngine(),
                new AiContextBuilder(),
                new AiOrchestrator(new NoopLlmClient(), true, 100),
                new ResponsePolicyEngine(),
                new AiAuditLogger(),
                new SimpleMeterRegistry()
        );

        RiskNavigatorCheckRequest req = new RiskNavigatorCheckRequest(
                "acc-1", "", "BUY", 0, 100, Map.of()
        );

        RiskNavigatorCheckResponse response = service.check("test-req-2", req);

        assertEquals("BLOCK_SUGGEST", response.decision());
        assertEquals("RULE_ONLY", response.suggestionSource());
        assertEquals("BLOCK_DECISION", response.fallbackReason());
        assertFalse(response.llmUsed());
    }
}

