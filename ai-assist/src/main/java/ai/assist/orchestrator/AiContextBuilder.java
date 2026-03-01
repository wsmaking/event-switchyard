package ai.assist.orchestrator;

import ai.assist.api.model.RiskNavigatorCheckRequest;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class AiContextBuilder {
    public AiContext build(RiskNavigatorCheckRequest request) {
        double notional = request.qty() * request.price();
        String bucket;
        if (notional < 1_000_000) {
            bucket = "SMALL";
        } else if (notional < 10_000_000) {
            bucket = "MEDIUM";
        } else {
            bucket = "LARGE";
        }

        return new AiContext(
                Instant.now(),
                notional,
                bucket,
                request.safeContext()
        );
    }
}

