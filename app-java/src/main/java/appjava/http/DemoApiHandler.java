package appjava.http;

import appjava.clients.BackOfficeClient;
import appjava.demo.ReplayScenarioService;
import appjava.market.MarketDataService;
import appjava.order.OrderRequest;
import appjava.order.OrderService;
import com.sun.net.httpserver.HttpExchange;

public final class DemoApiHandler extends JsonHttpHandler {
    public DemoApiHandler(
        MarketDataService marketDataService,
        BackOfficeClient backOfficeClient,
        OrderService orderService,
        ReplayScenarioService replayScenarioService
    ) {
        super(exchange -> route(exchange, marketDataService, backOfficeClient, orderService, replayScenarioService));
    }

    private static JsonResponse route(
        HttpExchange exchange,
        MarketDataService marketDataService,
        BackOfficeClient backOfficeClient,
        OrderService orderService,
        ReplayScenarioService replayScenarioService
    ) throws Exception {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            throw new MethodNotAllowedException(exchange.getRequestMethod());
        }
        String path = exchange.getRequestURI().getPath();
        if ("/api/demo/reset".equals(path)) {
            marketDataService.reset();
            orderService.reset();
            backOfficeClient.resetState();
            return JsonResponse.ok(new ResetResponse("RESET"));
        }
        String[] segments = path.split("/");
        if (segments.length == 6 && "api".equals(segments[1]) && "demo".equals(segments[2]) && "scenarios".equals(segments[3]) && "run".equals(segments[5])) {
            OrderRequest request = readJson(exchange, OrderRequest.class);
            return JsonResponse.ok(replayScenarioService.runScenario(segments[4], request));
        }
        throw new NotFoundException("route_not_found:" + path);
    }

    public record ResetResponse(String status) {
    }
}
