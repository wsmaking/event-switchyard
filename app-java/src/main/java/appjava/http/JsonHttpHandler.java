package appjava.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class JsonHttpHandler implements HttpHandler {
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Handler handler;

    public JsonHttpHandler(Handler handler) {
        this.handler = handler;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        applyCors(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
            return;
        }
        try {
            JsonResponse response = handler.handle(exchange);
            byte[] body = response.body() == null ? new byte[0] : OBJECT_MAPPER.writeValueAsBytes(response.body());
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(response.statusCode(), body.length);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(body);
            }
        } catch (ValidationException e) {
            writeError(exchange, 422, e.getMessage());
        } catch (NotFoundException e) {
            writeError(exchange, 404, e.getMessage());
        } catch (MethodNotAllowedException e) {
            writeError(exchange, 405, e.getMessage());
        } catch (Exception e) {
            writeError(exchange, 500, e.getMessage() == null ? "internal_error" : e.getMessage());
        } finally {
            exchange.close();
        }
    }

    protected static void applyCors(HttpExchange exchange) {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type,Authorization,Idempotency-Key");
    }

    protected static Map<String, String> parseQuery(String rawQuery) {
        if (rawQuery == null || rawQuery.isBlank()) {
            return Map.of();
        }
        return Arrays.stream(rawQuery.split("&"))
            .filter(part -> !part.isBlank())
            .map(part -> {
                int index = part.indexOf('=');
                String key = index >= 0 ? part.substring(0, index) : part;
                String value = index >= 0 ? part.substring(index + 1) : "";
                return Map.entry(
                    URLDecoder.decode(key, StandardCharsets.UTF_8),
                    URLDecoder.decode(value, StandardCharsets.UTF_8)
                );
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (left, right) -> right));
    }

    protected static <T> T readJson(HttpExchange exchange, Class<T> type) throws IOException {
        return OBJECT_MAPPER.readValue(exchange.getRequestBody(), type);
    }

    private void writeError(HttpExchange exchange, int statusCode, String error) throws IOException {
        byte[] body = OBJECT_MAPPER.writeValueAsBytes(new ErrorResponse(error));
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, body.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(body);
        }
    }

    public interface Handler {
        JsonResponse handle(HttpExchange exchange) throws Exception;
    }

    public record JsonResponse(int statusCode, Object body) {
        public static JsonResponse ok(Object body) {
            return new JsonResponse(200, body);
        }
    }

    public record ErrorResponse(String error) {
    }

    public static final class ValidationException extends RuntimeException {
        public ValidationException(String message) {
            super(message);
        }
    }

    public static final class NotFoundException extends RuntimeException {
        public NotFoundException(String message) {
            super(message);
        }
    }

    public static final class MethodNotAllowedException extends RuntimeException {
        public MethodNotAllowedException(String message) {
            super(message);
        }
    }
}
