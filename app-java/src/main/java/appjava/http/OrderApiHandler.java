package appjava.http;

import appjava.account.AccountOverview;
import appjava.clients.BackOfficeClient;
import appjava.clients.BackOfficeClient.BackOfficePosition;
import appjava.clients.OmsClient;
import appjava.market.MarketDataService;
import appjava.order.BalanceEffectView;
import appjava.order.FillView;
import appjava.order.OrderFinalOut;
import appjava.order.OrderEventView;
import appjava.order.OrderRequest;
import appjava.order.OrderService;
import appjava.order.OrderTimelineEntry;
import appjava.order.ReservationView;
import appjava.order.OrderView;
import appjava.position.UiPosition;
import com.sun.net.httpserver.HttpExchange;

import java.util.List;

public final class OrderApiHandler extends JsonHttpHandler {
    private static final long INITIAL_CASH = 10_000_000L;

    public OrderApiHandler(
        String accountId,
        MarketDataService marketDataService,
        BackOfficeClient backOfficeClient,
        OmsClient omsClient,
        OrderService orderService
    ) {
        super(exchange -> route(exchange, accountId, marketDataService, backOfficeClient, omsClient, orderService));
    }

    private static JsonResponse route(
        HttpExchange exchange,
        String accountId,
        MarketDataService marketDataService,
        BackOfficeClient backOfficeClient,
        OmsClient omsClient,
        OrderService orderService
    ) throws Exception {
        String path = exchange.getRequestURI().getPath();
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/orders".equals(path)) {
            OrderRequest request = readJson(exchange, OrderRequest.class);
            return new JsonResponse(202, orderService.submit(request));
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/orders".equals(path)) {
            List<OrderView> orders = orderService.listOrders();
            return JsonResponse.ok(orders);
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            String[] segments = path.split("/");
            if (segments.length == 4 && "api".equals(segments[1]) && "orders".equals(segments[2])) {
                return JsonResponse.ok(orderService.getOrder(segments[3]));
            }
            if (segments.length == 5 && "api".equals(segments[1]) && "orders".equals(segments[2]) && "timeline".equals(segments[4])) {
                OrderView order = orderService.getOrder(segments[3]);
                return JsonResponse.ok(resolveTimeline(orderService, omsClient, order));
            }
            if (segments.length == 5 && "api".equals(segments[1]) && "orders".equals(segments[2]) && "final-out".equals(segments[4])) {
                OrderView order = orderService.getOrder(segments[3]);
                AccountOverview accountOverview = backOfficeClient.fetchOverview(accountId);
                List<OrderEventView> rawEvents = omsClient.fetchOrderEvents(order.id());
                List<ReservationView> reservations = omsClient.fetchReservations(accountId).stream()
                    .filter(reservation -> order.id().equals(reservation.orderId()))
                    .toList();
                List<FillView> fills = backOfficeClient.fetchFills(order.id());
                List<UiPosition> positions = backOfficeClient.fetchPositions(accountId).stream()
                    .map(position -> toUiPosition(position, marketDataService))
                    .toList();
                List<OrderTimelineEntry> timeline = resolveTimeline(orderService, omsClient, order);
                OrderFinalOut finalOut = new OrderFinalOut(
                    order,
                    accountOverview,
                    balanceEffect(accountOverview),
                    reservations,
                    fills,
                    positions,
                    timeline,
                    rawEvents
                );
                return JsonResponse.ok(finalOut);
            }
        }
        throw new NotFoundException("route_not_found:" + path);
    }

    private static List<OrderTimelineEntry> resolveTimeline(
        OrderService orderService,
        OmsClient omsClient,
        OrderView order
    ) {
        List<OrderEventView> rawEvents = omsClient.fetchOrderEvents(order.id());
        if (!rawEvents.isEmpty()) {
            return rawEvents.stream()
                .map(event -> new OrderTimelineEntry(event.eventType(), event.eventAt(), event.label(), event.detail()))
                .toList();
        }
        return orderService.buildTimeline(order);
    }

    private static BalanceEffectView balanceEffect(AccountOverview accountOverview) {
        return new BalanceEffectView(
            accountOverview.cashBalance() - INITIAL_CASH,
            accountOverview.availableBuyingPower() - INITIAL_CASH,
            accountOverview.reservedBuyingPower(),
            accountOverview.realizedPnl()
        );
    }

    private static UiPosition toUiPosition(BackOfficePosition position, MarketDataService marketDataService) {
        double currentPrice = marketDataService.getCurrentPrice(position.symbol());
        double avgPrice = position.avgPrice();
        double unrealizedPnl = (currentPrice - avgPrice) * position.netQty();
        double baseCost = Math.abs(avgPrice * position.netQty());
        double pnlPercent = baseCost > 0.0 ? (unrealizedPnl / baseCost) * 100.0 : 0.0;
        return new UiPosition(
            position.symbol(),
            (int) position.netQty(),
            round2(avgPrice),
            round2(currentPrice),
            round2(unrealizedPnl),
            round2(pnlPercent)
        );
    }

    private static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
