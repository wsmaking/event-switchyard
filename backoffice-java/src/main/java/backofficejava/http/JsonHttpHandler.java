package backofficejava.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;

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
        try {
            JsonResponse response = handler.handle(exchange);
            byte[] body = OBJECT_MAPPER.writeValueAsBytes(response.body());
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(response.statusCode(), body.length);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(body);
            }
        } catch (MethodNotAllowedException e) {
            writeError(exchange, 405, e.getMessage());
        } catch (NotFoundException e) {
            writeError(exchange, 404, e.getMessage());
        } catch (Exception e) {
            writeError(exchange, 500, e.getMessage() == null ? "internal_error" : e.getMessage());
        } finally {
            exchange.close();
        }
    }

    private void writeError(HttpExchange exchange, int statusCode, String message) throws IOException {
        byte[] body = OBJECT_MAPPER.writeValueAsBytes(new ErrorResponse(message));
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, body.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(body);
        }
    }

    protected static <T> T readJson(HttpExchange exchange, Class<T> type) throws IOException {
        return OBJECT_MAPPER.readValue(exchange.getRequestBody(), type);
    }

    public interface Handler {
        JsonResponse handle(HttpExchange exchange) throws Exception;
    }

    public record ErrorResponse(String error) {
    }

    public record JsonResponse(int statusCode, Object body) {
        public static JsonResponse ok(Object body) {
            return new JsonResponse(200, body);
        }
    }

    public static final class MethodNotAllowedException extends RuntimeException {
        public MethodNotAllowedException(String method) {
            super("method_not_allowed:" + method);
        }
    }

    public static final class NotFoundException extends RuntimeException {
        public NotFoundException(String message) {
            super(message);
        }
    }
}
