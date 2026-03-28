package oms;

import oms.audit.GatewayAuditIntakeService;
import oms.bus.BusEventIntakeService;
import oms.http.OmsHttpServer;
import oms.order.OrderReadModel;
import oms.persistence.OmsRuntime;
import oms.persistence.OmsStoreFactory;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getProperty("oms.http.port", "18081"));
        String accountId = System.getProperty("oms.account.id", System.getenv().getOrDefault("ACCOUNT_ID", "acct_demo"));
        OmsRuntime runtime = OmsStoreFactory.create(accountId);
        OrderReadModel readModel = runtime.orderReadModel();
        GatewayAuditIntakeService auditIntakeService = new GatewayAuditIntakeService(readModel, runtime.auditOffsetStore());
        BusEventIntakeService busEventIntakeService = new BusEventIntakeService(auditIntakeService);
        OmsHttpServer server = new OmsHttpServer(port, readModel, auditIntakeService, busEventIntakeService);
        server.start();
        System.out.println("oms-java store mode=" + runtime.storeMode());
        auditIntakeService.start();
        busEventIntakeService.start();
    }
}
