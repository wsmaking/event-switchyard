package oms.http;

import com.sun.net.httpserver.HttpExchange;
import oms.order.OrderEventView;
import oms.order.OrderReadModel;
import oms.order.OrderView;

import java.util.List;
import java.util.Optional;

public final class OrderHttpHandler extends JsonHttpHandler {
    public OrderHttpHandler(OrderReadModel orderReadModel) {
        super(exchange -> route(exchange, orderReadModel));
    }

    private static JsonResponse route(HttpExchange exchange, OrderReadModel orderReadModel) {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            throw new MethodNotAllowedException(exchange.getRequestMethod());
        }

        String path = exchange.getRequestURI().getPath();
        if ("/orders".equals(path) || "/orders/".equals(path)) {
            List<OrderView> orders = orderReadModel.findAll();
            return JsonResponse.ok(orders);
        }

        String[] segments = path.split("/");
        if (segments.length == 3 && "orders".equals(segments[1])) {
            Optional<OrderView> order = orderReadModel.findById(segments[2]);
            return order.<JsonResponse>map(JsonResponse::ok)
                .orElseThrow(() -> new NotFoundException("order_not_found:" + segments[2]));
        }
        if (segments.length == 4 && "orders".equals(segments[1]) && "events".equals(segments[3])) {
            List<OrderEventView> events = orderReadModel.findEventsByOrderId(segments[2]);
            return JsonResponse.ok(events);
        }

        throw new NotFoundException("route_not_found:" + path);
    }
}
