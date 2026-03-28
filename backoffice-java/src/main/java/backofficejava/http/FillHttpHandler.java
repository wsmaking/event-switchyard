package backofficejava.http;

import backofficejava.account.FillReadModel;
import backofficejava.account.FillView;
import com.sun.net.httpserver.HttpExchange;

import java.util.List;
import java.util.Map;

public final class FillHttpHandler extends JsonHttpHandler {
    public FillHttpHandler(FillReadModel fillReadModel) {
        super(exchange -> route(exchange, fillReadModel));
    }

    private static JsonResponse route(HttpExchange exchange, FillReadModel fillReadModel) {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            throw new MethodNotAllowedException(exchange.getRequestMethod());
        }
        Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
        String orderId = query.get("orderId");
        if (orderId == null || orderId.isBlank()) {
            throw new NotFoundException("missing_order_id");
        }
        List<FillView> fills = fillReadModel.findByOrderId(orderId);
        return JsonResponse.ok(new FillsResponse(fills));
    }

    public record FillsResponse(List<FillView> fills) {
    }
}
