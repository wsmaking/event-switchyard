package appjava;

import appjava.auth.JwtSigner;
import appjava.clients.BackOfficeClient;
import appjava.clients.GatewayClient;
import appjava.clients.OmsClient;
import appjava.demo.ReplayScenarioService;
import appjava.http.AppHttpServer;
import appjava.market.MarketDataService;
import appjava.order.OrderService;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getProperty("app.http.port", "8080"));
        String accountId = System.getProperty("app.account.id", System.getenv().getOrDefault("ACCOUNT_ID", "acct_demo"));

        MarketDataService marketDataService = new MarketDataService();
        BackOfficeClient backOfficeClient = new BackOfficeClient(accountId);
        GatewayClient gatewayClient = new GatewayClient(accountId, JwtSigner.fromEnv());
        OmsClient omsClient = new OmsClient();
        OrderService orderService = new OrderService(accountId, gatewayClient, omsClient);
        ReplayScenarioService replayScenarioService = new ReplayScenarioService(
            accountId,
            marketDataService,
            backOfficeClient,
            omsClient
        );

        AppHttpServer server = new AppHttpServer(
            port,
            accountId,
            marketDataService,
            backOfficeClient,
            omsClient,
            orderService,
            replayScenarioService
        );
        server.start();
    }
}
