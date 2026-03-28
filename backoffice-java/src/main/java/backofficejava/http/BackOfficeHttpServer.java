package backofficejava.http;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.FillReadModel;
import backofficejava.account.LedgerReadModel;
import backofficejava.account.OrderProjectionStateStore;
import backofficejava.account.PositionReadModel;
import backofficejava.audit.GatewayAuditIntakeService;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public final class BackOfficeHttpServer {
    private final int port;
    private final AccountOverviewReadModel accountOverviewReadModel;
    private final PositionReadModel positionReadModel;
    private final FillReadModel fillReadModel;
    private final OrderProjectionStateStore orderProjectionStateStore;
    private final LedgerReadModel ledgerReadModel;
    private final GatewayAuditIntakeService intakeService;

    public BackOfficeHttpServer(
        int port,
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel,
        OrderProjectionStateStore orderProjectionStateStore,
        LedgerReadModel ledgerReadModel,
        GatewayAuditIntakeService intakeService
    ) {
        this.port = port;
        this.accountOverviewReadModel = accountOverviewReadModel;
        this.positionReadModel = positionReadModel;
        this.fillReadModel = fillReadModel;
        this.orderProjectionStateStore = orderProjectionStateStore;
        this.ledgerReadModel = ledgerReadModel;
        this.intakeService = intakeService;
    }

    public void start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", new JsonHttpHandler(exchange ->
            JsonHttpHandler.JsonResponse.ok(new HealthResponse("UP", "backoffice-java"))
        ));
        server.createContext("/stats", new BackOfficeStatsHttpHandler(intakeService));
        server.createContext("/reconcile", new BackOfficeReconcileHttpHandler(intakeService));
        server.createContext("/accounts", new AccountOverviewHttpHandler(accountOverviewReadModel));
        server.createContext("/positions", new PositionHttpHandler(positionReadModel));
        server.createContext("/fills", new FillHttpHandler(fillReadModel));
        server.createContext("/ledger", new LedgerHttpHandler(ledgerReadModel));
        server.createContext(
            "/demo/reset",
            new DemoResetHttpHandler(
                accountOverviewReadModel,
                positionReadModel,
                fillReadModel,
                orderProjectionStateStore,
                ledgerReadModel
            )
        );
        server.createContext(
            "/internal",
            new BackOfficeInternalHttpHandler(
                accountOverviewReadModel,
                positionReadModel,
                fillReadModel,
                orderProjectionStateStore,
                ledgerReadModel
            )
        );
        server.createContext("/internal/audit", new BackOfficeInternalAuditHttpHandler(intakeService));
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
        System.out.println("backoffice-java listening on http://localhost:" + port);
    }

    public record HealthResponse(String status, String service) {
    }
}
