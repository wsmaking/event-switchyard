package appjava.http;

import appjava.clients.BackOfficeClient;
import appjava.clients.BackOfficeClient.BackOfficePosition;
import appjava.market.MarketDataService;
import appjava.position.UiPosition;
import com.sun.net.httpserver.HttpExchange;

import java.util.List;

public final class PositionApiHandler extends JsonHttpHandler {
    public PositionApiHandler(
        String accountId,
        MarketDataService marketDataService,
        BackOfficeClient backOfficeClient
    ) {
        super(exchange -> route(exchange, accountId, marketDataService, backOfficeClient));
    }

    private static JsonResponse route(
        HttpExchange exchange,
        String accountId,
        MarketDataService marketDataService,
        BackOfficeClient backOfficeClient
    ) {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            throw new MethodNotAllowedException(exchange.getRequestMethod());
        }
        if (!"/api/positions".equals(exchange.getRequestURI().getPath())) {
            throw new NotFoundException("route_not_found:" + exchange.getRequestURI().getPath());
        }
        List<UiPosition> positions = backOfficeClient.fetchPositions(accountId).stream()
            .filter(position -> position.netQty() != 0L)
            .map(position -> toUiPosition(position, marketDataService))
            .toList();
        return JsonResponse.ok(positions);
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
