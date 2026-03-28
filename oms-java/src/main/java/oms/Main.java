package oms;

import oms.http.OmsHttpServer;
import oms.order.InMemoryOrderReadModel;
import oms.order.OrderReadModel;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getProperty("oms.http.port", "18081"));
        String accountId = System.getProperty("oms.account.id", System.getenv().getOrDefault("ACCOUNT_ID", "acct_demo"));
        OrderReadModel readModel = new InMemoryOrderReadModel(accountId);
        OmsHttpServer server = new OmsHttpServer(port, readModel);
        server.start();
    }
}
