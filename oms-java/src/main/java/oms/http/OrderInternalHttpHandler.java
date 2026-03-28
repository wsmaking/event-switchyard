package oms.http;

import com.sun.net.httpserver.HttpExchange;
import oms.order.OrderEventView;
import oms.order.OrderReadModel;
import oms.order.ReservationView;
import oms.order.OrderView;

import java.util.List;

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
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/orders/events/replace".equals(path)) {
            ReplaceOrderEventsRequest request = readJson(exchange, ReplaceOrderEventsRequest.class);
            orderReadModel.replaceEvents(request.orderId(), request.events());
            return JsonResponse.ok(new ReplaceResponse("REPLACED", request.events().size()));
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/accounts/reservations/replace".equals(path)) {
            ReplaceReservationsRequest request = readJson(exchange, ReplaceReservationsRequest.class);
            orderReadModel.replaceReservations(request.accountId(), request.reservations());
            return JsonResponse.ok(new ReplaceResponse("REPLACED", request.reservations().size()));
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/orders/reset".equals(path)) {
            orderReadModel.reset();
            return JsonResponse.ok(new ResetResponse("RESET"));
        }
        throw new NotFoundException("route_not_found:" + path);
    }

    public record ReplaceOrderEventsRequest(String orderId, List<OrderEventView> events) {
    }

    public record ReplaceReservationsRequest(String accountId, List<ReservationView> reservations) {
    }

    public record ReplaceResponse(String status, int count) {
    }

    public record ResetResponse(String status) {
    }
}
