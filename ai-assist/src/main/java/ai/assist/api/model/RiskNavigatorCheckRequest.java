package ai.assist.api.model;

import java.util.Map;

public record RiskNavigatorCheckRequest(
        String accountId,
        String symbol,
        String side,
        double qty,
        double price,
        Map<String, Object> context
) {
    public Map<String, Object> safeContext() {
        return context == null ? Map.of() : context;
    }
}

