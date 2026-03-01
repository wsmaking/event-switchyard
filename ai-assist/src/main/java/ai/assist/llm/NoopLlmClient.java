package ai.assist.llm;

import ai.assist.api.model.RiskNavigatorCheckRequest;
import ai.assist.domain.RuleEvaluation;
import ai.assist.orchestrator.AiContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@ConditionalOnProperty(prefix = "ai.assist.llm", name = "provider", havingValue = "none", matchIfMissing = true)
public class NoopLlmClient implements LlmClient {
    @Override
    public Optional<LlmSuggestion> generateSuggestion(
            RiskNavigatorCheckRequest request,
            RuleEvaluation evaluation,
            AiContext context
    ) {
        return Optional.empty();
    }
}
