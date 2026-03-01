package ai.assist.llm;

import ai.assist.api.model.RiskNavigatorCheckRequest;
import ai.assist.domain.RuleEvaluation;
import ai.assist.orchestrator.AiContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;

@Component
@Primary
@ConditionalOnProperty(prefix = "ai.assist.llm", name = "provider", havingValue = "openai")
public class OpenAiLlmClient implements LlmClient {
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final String apiKey;
    private final String endpoint;
    private final String model;
    private final double temperature;
    private final int maxTokens;
    private final long httpTimeoutMs;

    public OpenAiLlmClient(
            ObjectMapper objectMapper,
            @Value("${ai.assist.llm.openai.api-key:}") String apiKey,
            @Value("${ai.assist.llm.openai.endpoint:https://api.openai.com/v1/chat/completions}") String endpoint,
            @Value("${ai.assist.llm.openai.model:gpt-4o-mini}") String model,
            @Value("${ai.assist.llm.openai.temperature:0.1}") double temperature,
            @Value("${ai.assist.llm.openai.max-tokens:120}") int maxTokens,
            @Value("${ai.assist.llm.openai.http-timeout-ms:1200}") long httpTimeoutMs
    ) {
        this.objectMapper = objectMapper;
        this.apiKey = apiKey;
        this.endpoint = endpoint;
        this.model = model;
        this.temperature = temperature;
        this.maxTokens = maxTokens;
        this.httpTimeoutMs = httpTimeoutMs;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(Math.max(100, httpTimeoutMs)))
                .build();
    }

    @Override
    public Optional<LlmSuggestion> generateSuggestion(
            RiskNavigatorCheckRequest request,
            RuleEvaluation evaluation,
            AiContext context
    ) {
        if (apiKey == null || apiKey.isBlank()) {
            return Optional.empty();
        }

        try {
            String payload = buildPayload(request, evaluation, context);
            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .timeout(Duration.ofMillis(Math.max(100, httpTimeoutMs)))
                    .header("Authorization", "Bearer " + apiKey)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();

            HttpResponse<String> response = httpClient.send(
                    httpRequest,
                    HttpResponse.BodyHandlers.ofString()
            );
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                return Optional.empty();
            }

            JsonNode root = objectMapper.readTree(response.body());
            JsonNode content = root.path("choices").path(0).path("message").path("content");
            if (content.isMissingNode() || content.asText().isBlank()) {
                return Optional.empty();
            }
            return Optional.of(new LlmSuggestion("openai", model, content.asText()));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private String buildPayload(
            RiskNavigatorCheckRequest request,
            RuleEvaluation evaluation,
            AiContext context
    ) throws Exception {
        String systemPrompt = "あなたは証券注文前チェックの補助アシスタントです。"
                + "断定的な投資助言はせず、リスク注記を添えて簡潔に提案してください。";
        String userPrompt = String.format(
                "account=%s symbol=%s side=%s qty=%.4f price=%.4f notional=%.4f bucket=%s decision=%s reasons=%s",
                redact(request.accountId()),
                request.symbol(),
                request.side(),
                request.qty(),
                request.price(),
                context.notional(),
                context.notionalBucket(),
                evaluation.decision(),
                evaluation.reasons()
        );

        ArrayNode messages = objectMapper.createArrayNode()
                .add(objectMapper.createObjectNode()
                        .put("role", "system")
                        .put("content", systemPrompt))
                .add(objectMapper.createObjectNode()
                        .put("role", "user")
                        .put("content", userPrompt));

        ObjectNode body = objectMapper.createObjectNode();
        body.put("model", model);
        body.put("temperature", temperature);
        body.put("max_tokens", maxTokens);
        body.set("messages", messages);
        return objectMapper.writeValueAsString(body);
    }

    private static String redact(String accountId) {
        if (accountId == null || accountId.isBlank()) {
            return "unknown";
        }
        if (accountId.length() <= 4) {
            return "****";
        }
        return "****" + accountId.substring(accountId.length() - 4);
    }
}
