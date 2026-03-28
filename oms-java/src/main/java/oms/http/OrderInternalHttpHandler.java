package oms.http;

import com.sun.net.httpserver.HttpExchange;
import oms.order.OrderReadModel;
import oms.order.OrderView;

public final class OrderInternalHttpHandler extends JsonHttpHandler {
    public OrderInternalHttpHandler(OrderReadModel orderReadModel) {
        super(exchange -> route(exchange, orderReadModel));
    }

    private static JsonResponse route(HttpExchange exchange, OrderReadModel orderReadModel) throws Exception {
        String path = exchange.getRequestURI().getPath();
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/orders/upsert".equals(path)) {
            OrderView request = readJson(exchange, OrderView.class);
            orderReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/orders/reset".equals(path)) {
            orderReadModel.reset();
            return JsonResponse.ok(new ResetResponse("RESET"));
        }
        throw new NotFoundException("route_not_found:" + path);
    }

    public record ResetResponse(String status) {
    }
}
