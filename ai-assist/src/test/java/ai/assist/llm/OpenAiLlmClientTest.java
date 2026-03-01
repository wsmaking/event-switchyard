package ai.assist.llm;

import ai.assist.api.model.RiskNavigatorCheckRequest;
import ai.assist.domain.RiskDecision;
import ai.assist.domain.RuleEvaluation;
import ai.assist.orchestrator.AiContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class OpenAiLlmClientTest {
    @Test
    void shouldReturnEmptyWhenApiKeyMissing() {
        OpenAiLlmClient client = new OpenAiLlmClient(
                new ObjectMapper(),
                "",
                "http://127.0.0.1:65535/v1/chat/completions",
                "gpt-4o-mini",
                0.1,
                120,
                300
        );

        Optional<LlmSuggestion> result = client.generateSuggestion(sampleRequest(), sampleEval(), sampleContext());
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldParseSuggestionOnSuccess() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/chat/completions", exchange -> {
            String body = """
                    {"choices":[{"message":{"content":"数量を分割して発注してください。"}}]}
                    """;
            exchange.sendResponseHeaders(200, body.getBytes().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body.getBytes());
            }
        });
        server.start();
        try {
            String endpoint = "http://127.0.0.1:" + server.getAddress().getPort() + "/v1/chat/completions";
            OpenAiLlmClient client = new OpenAiLlmClient(
                    new ObjectMapper(),
                    "dummy-key",
                    endpoint,
                    "gpt-4o-mini",
                    0.1,
                    120,
                    500
            );

            Optional<LlmSuggestion> result = client.generateSuggestion(sampleRequest(), sampleEval(), sampleContext());
            assertTrue(result.isPresent());
            assertEquals("openai", result.get().provider());
            assertTrue(result.get().text().contains("分割"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void shouldReturnEmptyOnServerError() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/chat/completions", exchange -> {
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        server.start();
        try {
            String endpoint = "http://127.0.0.1:" + server.getAddress().getPort() + "/v1/chat/completions";
            OpenAiLlmClient client = new OpenAiLlmClient(
                    new ObjectMapper(),
                    "dummy-key",
                    endpoint,
                    "gpt-4o-mini",
                    0.1,
                    120,
                    500
            );

            Optional<LlmSuggestion> result = client.generateSuggestion(sampleRequest(), sampleEval(), sampleContext());
            assertTrue(result.isEmpty());
        } finally {
            server.stop(0);
        }
    }

    private static RiskNavigatorCheckRequest sampleRequest() {
        return new RiskNavigatorCheckRequest(
                "acc-123456",
                "7203.T",
                "BUY",
                1000,
                2500,
                Map.of("source", "test")
        );
    }

    private static RuleEvaluation sampleEval() {
        return new RuleEvaluation(RiskDecision.WARN, List.of("NOTIONAL_LARGE"));
    }

    private static AiContext sampleContext() {
        return new AiContext(Instant.now(), 2_500_000, "MEDIUM", Map.of());
    }
}

