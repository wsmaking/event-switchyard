package ai.assist.orchestrator;

import ai.assist.api.model.RiskNavigatorCheckRequest;
import ai.assist.domain.RuleEvaluation;
import ai.assist.llm.LlmClient;
import ai.assist.llm.LlmSuggestion;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.*;

@Component
public class AiOrchestrator {
    private final LlmClient llmClient;
    private final boolean llmEnabled;
    private final long timeoutMs;
    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    public AiOrchestrator(
            LlmClient llmClient,
            @Value("${ai.assist.llm.enabled:false}") boolean llmEnabled,
            @Value("${ai.assist.llm.timeout-ms:600}") long timeoutMs
    ) {
        this.llmClient = llmClient;
        this.llmEnabled = llmEnabled;
        this.timeoutMs = timeoutMs;
    }

    public OrchestrationResult suggest(
            RiskNavigatorCheckRequest request,
            RuleEvaluation evaluation,
            AiContext context
    ) {
        if (evaluation.decision().name().equals("BLOCK_SUGGEST")) {
            return new OrchestrationResult(
                    "注文条件を修正して再確認してください。",
                    "RULE_ONLY",
                    false,
                    false,
                    "BLOCK_DECISION"
            );
        }

        if (!llmEnabled) {
            return new OrchestrationResult(
                    fallbackFor(evaluation),
                    "RULE_ONLY",
                    false,
                    true,
                    "LLM_DISABLED"
            );
        }

        try {
            CompletableFuture<Optional<LlmSuggestion>> future = CompletableFuture.supplyAsync(
                    () -> llmClient.generateSuggestion(request, evaluation, context),
                    executor
            );
            Optional<LlmSuggestion> suggestion = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            if (suggestion.isPresent() && !suggestion.get().text().isBlank()) {
                return new OrchestrationResult(
                        suggestion.get().text(),
                        "LLM",
                        true,
                        false,
                        "NONE"
                );
            }
            return new OrchestrationResult(
                    fallbackFor(evaluation),
                    "RULE_FALLBACK",
                    false,
                    true,
                    "LLM_EMPTY"
            );
        } catch (TimeoutException e) {
            return new OrchestrationResult(
                    fallbackFor(evaluation),
                    "RULE_FALLBACK",
                    false,
                    true,
                    "LLM_TIMEOUT"
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new OrchestrationResult(
                    fallbackFor(evaluation),
                    "RULE_FALLBACK",
                    false,
                    true,
                    "LLM_INTERRUPTED"
            );
        } catch (ExecutionException e) {
            return new OrchestrationResult(
                    fallbackFor(evaluation),
                    "RULE_FALLBACK",
                    false,
                    true,
                    "LLM_ERROR"
            );
        }
    }

    private static String fallbackFor(RuleEvaluation evaluation) {
        if (evaluation.decision().name().equals("WARN")) {
            return "数量または想定約定金額が大きいため、分割発注と数量再確認を推奨します。";
        }
        return "ルール判定は問題ありません。板状況とスリッページを確認してください。";
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}
