package ai.assist.domain;

import ai.assist.api.model.RiskNavigatorCheckRequest;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RiskRuleEngineTest {
    private final RiskRuleEngine engine = new RiskRuleEngine();

    @Test
    void shouldBlockOnInvalidInputs() {
        RiskNavigatorCheckRequest req = new RiskNavigatorCheckRequest(
                "acc-1", "", "BUY", -1, 100, Map.of()
        );

        RuleEvaluation result = engine.evaluate(req);

        assertEquals(RiskDecision.BLOCK_SUGGEST, result.decision());
        assertTrue(result.reasons().contains("SYMBOL_MISSING"));
        assertTrue(result.reasons().contains("QTY_NON_POSITIVE"));
    }

    @Test
    void shouldWarnOnLargeNotional() {
        RiskNavigatorCheckRequest req = new RiskNavigatorCheckRequest(
                "acc-1", "7203.T", "BUY", 100_000, 1000, Map.of()
        );

        RuleEvaluation result = engine.evaluate(req);

        assertEquals(RiskDecision.WARN, result.decision());
        assertTrue(result.reasons().contains("QTY_LARGE"));
        assertTrue(result.reasons().contains("NOTIONAL_LARGE"));
    }
}

