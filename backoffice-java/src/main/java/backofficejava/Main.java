package backofficejava;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.FillReadModel;
import backofficejava.account.InMemoryAccountOverviewReadModel;
import backofficejava.account.InMemoryFillReadModel;
import backofficejava.account.InMemoryPositionReadModel;
import backofficejava.account.PositionReadModel;
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
        BackOfficeHttpServer server = new BackOfficeHttpServer(port, accountOverviewReadModel, positionReadModel, fillReadModel);
        server.start();
    }
}
