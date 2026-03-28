package backofficejava.http;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.FillReadModel;
import backofficejava.account.PositionReadModel;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public final class BackOfficeHttpServer {
    private final int port;
    private final AccountOverviewReadModel accountOverviewReadModel;
    private final PositionReadModel positionReadModel;
    private final FillReadModel fillReadModel;

    public BackOfficeHttpServer(
        int port,
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel
    ) {
        this.port = port;
        this.accountOverviewReadModel = accountOverviewReadModel;
        this.positionReadModel = positionReadModel;
        this.fillReadModel = fillReadModel;
    }

    public void start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", new JsonHttpHandler(exchange ->
            JsonHttpHandler.JsonResponse.ok(new HealthResponse("UP", "backoffice-java"))
        ));
        server.createContext("/accounts", new AccountOverviewHttpHandler(accountOverviewReadModel));
        server.createContext("/positions", new PositionHttpHandler(positionReadModel));
        server.createContext("/fills", new FillHttpHandler(fillReadModel));
        server.createContext("/demo/reset", new DemoResetHttpHandler(accountOverviewReadModel, positionReadModel, fillReadModel));
        server.createContext("/internal", new BackOfficeInternalHttpHandler(accountOverviewReadModel, positionReadModel, fillReadModel));
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
        System.out.println("backoffice-java listening on http://localhost:" + port);
    }

    public record HealthResponse(String status, String service) {
    }
}
