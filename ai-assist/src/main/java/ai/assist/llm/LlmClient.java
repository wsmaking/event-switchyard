package ai.assist.llm;

import ai.assist.api.model.RiskNavigatorCheckRequest;
import ai.assist.domain.RuleEvaluation;
import ai.assist.orchestrator.AiContext;

import java.util.Optional;

public interface LlmClient {
    Optional<LlmSuggestion> generateSuggestion(
            RiskNavigatorCheckRequest request,
            RuleEvaluation evaluation,
            AiContext context
    );
}

