package appjava.http;

import appjava.clients.BackOfficeClient;
import appjava.clients.OmsClient;
import appjava.demo.ReplayScenarioService;
import appjava.market.MarketDataService;
import appjava.mobile.MobileLearningService;
import appjava.order.OrderService;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public final class AppHttpServer {
    private final int port;
    private final String accountId;
    private final MarketDataService marketDataService;
    private final BackOfficeClient backOfficeClient;
    private final OmsClient omsClient;
    private final OrderService orderService;
    private final ReplayScenarioService replayScenarioService;
    private final MobileLearningService mobileLearningService;

    public AppHttpServer(
        int port,
        String accountId,
        MarketDataService marketDataService,
        BackOfficeClient backOfficeClient,
        OmsClient omsClient,
        OrderService orderService,
        ReplayScenarioService replayScenarioService,
        MobileLearningService mobileLearningService
    ) {
        this.port = port;
        this.accountId = accountId;
        this.marketDataService = marketDataService;
        this.backOfficeClient = backOfficeClient;
        this.omsClient = omsClient;
        this.orderService = orderService;
        this.replayScenarioService = replayScenarioService;
        this.mobileLearningService = mobileLearningService;
    }

    public void start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", new JsonHttpHandler(exchange ->
            JsonHttpHandler.JsonResponse.ok(new HealthResponse("UP", "app-java"))
        ));
        server.createContext("/api/orders", new OrderApiHandler(accountId, marketDataService, backOfficeClient, omsClient, orderService));
        server.createContext("/api/positions", new PositionApiHandler(accountId, marketDataService, backOfficeClient));
        server.createContext("/api/market", new MarketApiHandler(marketDataService));
        server.createContext("/api/accounts", new AccountApiHandler(backOfficeClient));
        server.createContext("/api/demo", new DemoApiHandler(marketDataService, backOfficeClient, orderService, replayScenarioService));
        server.createContext("/api/ops", new OpsApiHandler(accountId, omsClient, backOfficeClient));
        server.createContext("/api/order-stream", new OrderStreamHandler(omsClient, backOfficeClient));
        server.createContext("/api/mobile", new MobileApiHandler(mobileLearningService));
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("app-java listening on http://localhost:" + port);
    }

    public record HealthResponse(String status, String service) {
    }
}
