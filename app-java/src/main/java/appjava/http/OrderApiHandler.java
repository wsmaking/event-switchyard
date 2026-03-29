package appjava.http;

import appjava.account.AccountOverview;
import appjava.clients.BackOfficeClient;
import appjava.clients.BackOfficeClient.BackOfficePosition;
import appjava.clients.OmsClient;
import appjava.market.MarketDataService;
import appjava.market.MarketStructureSnapshot;
import appjava.order.BalanceEffectView;
import appjava.order.ExecutionBenchmark;
import appjava.order.ExecutionBenchmarkStore;
import appjava.order.ExecutionQualityView;
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
        OrderService orderService,
        ExecutionBenchmarkStore executionBenchmarkStore
    ) {
        super(exchange -> route(exchange, accountId, marketDataService, backOfficeClient, omsClient, orderService, executionBenchmarkStore));
    }

    private static JsonResponse route(
        HttpExchange exchange,
        String accountId,
        MarketDataService marketDataService,
        BackOfficeClient backOfficeClient,
        OmsClient omsClient,
        OrderService orderService,
        ExecutionBenchmarkStore executionBenchmarkStore
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
                ExecutionQualityView executionQuality = buildExecutionQuality(
                    order,
                    fills,
                    executionBenchmarkStore.get(order.id()).orElse(null),
                    marketDataService.getMarketStructure(order.symbol())
                );
                OrderFinalOut finalOut = new OrderFinalOut(
                    order,
                    accountOverview,
                    balanceEffect(accountOverview),
                    executionQuality,
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

    private static ExecutionQualityView buildExecutionQuality(
        OrderView order,
        List<FillView> fills,
        ExecutionBenchmark benchmark,
        MarketStructureSnapshot marketStructure
    ) {
        double arrivalLast = benchmark == null ? marketStructure.lastPrice() : benchmark.arrivalLastPrice();
        double arrivalBid = benchmark == null ? marketStructure.bidPrice() : benchmark.arrivalBidPrice();
        double arrivalAsk = benchmark == null ? marketStructure.askPrice() : benchmark.arrivalAskPrice();
        double arrivalMid = benchmark == null ? marketStructure.midPrice() : benchmark.arrivalMidPrice();
        double arrivalSpreadBps = benchmark == null ? marketStructure.spreadBps() : benchmark.arrivalSpreadBps();
        String venueState = benchmark == null ? marketStructure.venueState() : benchmark.venueState();
        String benchmarkLabel = benchmark == null ? "current-mid" : benchmark.benchmarkLabel();
        double fillRatePercent = order.quantity() == 0 ? 0.0 : round2((order.filledQuantity() * 100.0) / order.quantity());

        if (fills.isEmpty()) {
            return new ExecutionQualityView(
                benchmarkLabel,
                venueState,
                round2(arrivalLast),
                round2(arrivalBid),
                round2(arrivalAsk),
                round2(arrivalMid),
                round2(arrivalSpreadBps),
                null,
                fillRatePercent,
                null,
                null,
                null,
                "まだ fill が無いため、到着時基準と spread のみを表示"
            );
        }

        long totalQuantity = fills.stream().mapToLong(FillView::quantity).sum();
        long totalNotional = fills.stream().mapToLong(FillView::notional).sum();
        double averageExecutionPrice = totalQuantity == 0 ? arrivalMid : round2(totalNotional / (double) totalQuantity);
        double directionalSlippagePerShare = "SELL".equalsIgnoreCase(order.side())
            ? arrivalMid - averageExecutionPrice
            : averageExecutionPrice - arrivalMid;
        double directionalSlippageAmount = round2(directionalSlippagePerShare * totalQuantity);
        double directionalSlippageBps = arrivalMid == 0.0 ? 0.0 : round2((directionalSlippagePerShare / arrivalMid) * 10_000.0);
        String note = directionalSlippagePerShare <= 0.0
            ? "到着時基準対比では改善側の執行"
            : "到着時基準対比ではコスト側の執行";

        return new ExecutionQualityView(
            benchmarkLabel,
            venueState,
            round2(arrivalLast),
            round2(arrivalBid),
            round2(arrivalAsk),
            round2(arrivalMid),
            round2(arrivalSpreadBps),
            averageExecutionPrice,
            fillRatePercent,
            round2(directionalSlippagePerShare),
            directionalSlippageAmount,
            directionalSlippageBps,
            note
        );
    }
}
