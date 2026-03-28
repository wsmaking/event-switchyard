package backofficejava;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.FillReadModel;
import backofficejava.account.InMemoryAccountOverviewReadModel;
import backofficejava.account.InMemoryFillReadModel;
import backofficejava.account.InMemoryLedgerReadModel;
import backofficejava.account.InMemoryOrderProjectionStateStore;
import backofficejava.account.InMemoryPositionReadModel;
import backofficejava.account.LedgerReadModel;
import backofficejava.account.OrderProjectionStateStore;
import backofficejava.account.PositionReadModel;
import backofficejava.audit.GatewayAuditIntakeService;
import backofficejava.http.BackOfficeHttpServer;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getProperty("backoffice.http.port", "18082"));
        String accountId = System.getProperty("backoffice.account.id", System.getenv().getOrDefault("ACCOUNT_ID", "acct_demo"));
        AccountOverviewReadModel accountOverviewReadModel = new InMemoryAccountOverviewReadModel(accountId);
        PositionReadModel positionReadModel = new InMemoryPositionReadModel(accountId);
        FillReadModel fillReadModel = new InMemoryFillReadModel();
        OrderProjectionStateStore orderProjectionStateStore = new InMemoryOrderProjectionStateStore();
        LedgerReadModel ledgerReadModel = new InMemoryLedgerReadModel();
        GatewayAuditIntakeService intakeService = new GatewayAuditIntakeService(
            accountOverviewReadModel,
            positionReadModel,
            fillReadModel,
            orderProjectionStateStore,
            ledgerReadModel
        );
        BackOfficeHttpServer server = new BackOfficeHttpServer(
            port,
            accountOverviewReadModel,
            positionReadModel,
            fillReadModel,
            orderProjectionStateStore,
            ledgerReadModel,
            intakeService
        );
        server.start();
        intakeService.start();
    }
}
