package ai.assist.domain;

import ai.assist.api.model.RiskNavigatorCheckRequest;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class RiskRuleEngine {
    private static final double SOFT_QTY_LIMIT = 50_000;
    private static final double SOFT_NOTIONAL_LIMIT = 50_000_000;

    public RuleEvaluation evaluate(RiskNavigatorCheckRequest request) {
        List<String> reasons = new ArrayList<>();
        List<String> hardReasons = new ArrayList<>();

        String symbol = trim(request.symbol());
        String side = trim(request.side()).toUpperCase();

        if (symbol.isEmpty()) {
            hardReasons.add("SYMBOL_MISSING");
        }
        if (!"BUY".equals(side) && !"SELL".equals(side)) {
            hardReasons.add("SIDE_INVALID");
        }
        if (request.qty() <= 0) {
            hardReasons.add("QTY_NON_POSITIVE");
        }
        if (request.price() <= 0) {
            hardReasons.add("PRICE_NON_POSITIVE");
        }

        if (!hardReasons.isEmpty()) {
            reasons.addAll(hardReasons);
            return new RuleEvaluation(RiskDecision.BLOCK_SUGGEST, reasons);
        }

        if (request.qty() > SOFT_QTY_LIMIT) {
            reasons.add("QTY_LARGE");
        }
        double notional = request.qty() * request.price();
        if (notional > SOFT_NOTIONAL_LIMIT) {
            reasons.add("NOTIONAL_LARGE");
        }

        if (reasons.isEmpty()) {
            reasons.add("RULES_OK");
            return new RuleEvaluation(RiskDecision.OK, reasons);
        }
        return new RuleEvaluation(RiskDecision.WARN, reasons);
    }

    private static String trim(String value) {
        return value == null ? "" : value.trim();
    }
}

