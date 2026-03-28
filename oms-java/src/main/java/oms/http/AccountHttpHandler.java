package oms.http;

import com.sun.net.httpserver.HttpExchange;
import oms.order.OrderReadModel;

public final class AccountHttpHandler extends JsonHttpHandler {
    public AccountHttpHandler(OrderReadModel orderReadModel) {
        super(exchange -> route(exchange, orderReadModel));
    }

    private static JsonResponse route(HttpExchange exchange, OrderReadModel orderReadModel) {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            throw new MethodNotAllowedException(exchange.getRequestMethod());
        }
        String path = exchange.getRequestURI().getPath();
        String[] segments = path.split("/");
        if (segments.length == 4 && "accounts".equals(segments[1]) && "reservations".equals(segments[3])) {
            return JsonResponse.ok(orderReadModel.findReservationsByAccountId(segments[2]));
        }
        throw new NotFoundException("route_not_found:" + path);
    }
}
