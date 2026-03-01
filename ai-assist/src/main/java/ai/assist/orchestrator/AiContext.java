package ai.assist.orchestrator;

import java.time.Instant;
import java.util.Map;

public record AiContext(
        Instant receivedAt,
        double notional,
        String notionalBucket,
        Map<String, Object> rawContext
) {}

