package backofficejava;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.FillReadModel;
import backofficejava.account.LedgerReadModel;
import backofficejava.account.OrderProjectionStateStore;
import backofficejava.account.PositionReadModel;
import backofficejava.audit.GatewayAuditIntakeService;
import backofficejava.bus.BusEventIntakeService;
import backofficejava.http.BackOfficeHttpServer;
import backofficejava.persistence.BackOfficeRuntime;
import backofficejava.persistence.BackOfficeStoreFactory;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getProperty("backoffice.http.port", "18082"));
        String accountId = System.getProperty("backoffice.account.id", System.getenv().getOrDefault("ACCOUNT_ID", "acct_demo"));
        BackOfficeRuntime runtime = BackOfficeStoreFactory.create(accountId);
        AccountOverviewReadModel accountOverviewReadModel = runtime.accountOverviewReadModel();
        PositionReadModel positionReadModel = runtime.positionReadModel();
        FillReadModel fillReadModel = runtime.fillReadModel();
        OrderProjectionStateStore orderProjectionStateStore = runtime.orderProjectionStateStore();
        LedgerReadModel ledgerReadModel = runtime.ledgerReadModel();
        GatewayAuditIntakeService intakeService = new GatewayAuditIntakeService(
            accountOverviewReadModel,
            positionReadModel,
            fillReadModel,
            orderProjectionStateStore,
            ledgerReadModel,
            runtime.auditOffsetStore(),
            runtime.deadLetterStore(),
            runtime.pendingOrphanStore()
        );
        BusEventIntakeService busEventIntakeService = new BusEventIntakeService(intakeService);
        BackOfficeHttpServer server = new BackOfficeHttpServer(
            port,
            accountOverviewReadModel,
            positionReadModel,
            fillReadModel,
            orderProjectionStateStore,
            ledgerReadModel,
            intakeService,
            busEventIntakeService
        );
        server.start();
        intakeService.start();
        busEventIntakeService.start();
        System.out.println("backoffice-java store mode=" + runtime.storeMode());
    }
}
