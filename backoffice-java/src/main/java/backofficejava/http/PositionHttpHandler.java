package backofficejava.http;

import backofficejava.account.PositionReadModel;
import backofficejava.account.PositionView;
import com.sun.net.httpserver.HttpExchange;

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

    public record PositionsResponse(List<PositionView> positions) {
    }
}
