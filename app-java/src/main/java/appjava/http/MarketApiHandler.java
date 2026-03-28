package appjava.http;

import appjava.market.MarketDataService;
import com.sun.net.httpserver.HttpExchange;

public final class MarketApiHandler extends JsonHttpHandler {
    public MarketApiHandler(MarketDataService marketDataService) {
        super(exchange -> route(exchange, marketDataService));
    }

    private static JsonResponse route(HttpExchange exchange, MarketDataService marketDataService) {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            throw new MethodNotAllowedException(exchange.getRequestMethod());
        }
        String[] segments = exchange.getRequestURI().getPath().split("/");
        if (segments.length >= 4 && "api".equals(segments[1]) && "market".equals(segments[2])) {
            String symbol = segments[3];
            if (segments.length == 4) {
                return JsonResponse.ok(marketDataService.getStockInfo(symbol));
            }
            if (segments.length == 5 && "history".equals(segments[4])) {
                int limit = Integer.parseInt(parseQuery(exchange.getRequestURI().getRawQuery()).getOrDefault("limit", "120"));
                return JsonResponse.ok(marketDataService.getPriceHistory(symbol, limit));
            }
        }
        throw new NotFoundException("route_not_found:" + exchange.getRequestURI().getPath());
    }
}
