package ai.assist.llm;

import ai.assist.api.model.RiskNavigatorCheckRequest;
import ai.assist.domain.RuleEvaluation;
import ai.assist.orchestrator.AiContext;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
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

