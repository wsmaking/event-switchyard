package backofficejava.http;

import backofficejava.account.PositionReadModel;
import backofficejava.account.PositionView;
import com.sun.net.httpserver.HttpExchange;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public final class PositionHttpHandler extends JsonHttpHandler {
    private final PositionReadModel readModel;

    public PositionHttpHandler(PositionReadModel readModel) {
        super(exchange -> route(exchange, readModel));
        this.readModel = readModel;
    }

    private static JsonResponse route(HttpExchange exchange, PositionReadModel readModel) {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            throw new MethodNotAllowedException(exchange.getRequestMethod());
        }
        Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
        String accountId = query.getOrDefault("accountId", System.getenv().getOrDefault("ACCOUNT_ID", "acct_demo"));
        List<PositionView> positions = readModel.findByAccountId(accountId);
        return JsonResponse.ok(new PositionsResponse(positions));
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            return Map.of();
        }
        return java.util.Arrays.stream(rawQuery.split("&"))
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
            .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (left, right) -> right));
    }

    public record PositionsResponse(List<PositionView> positions) {
    }
}
