package oms.http;

import com.sun.net.httpserver.HttpServer;
import oms.audit.GatewayAuditIntakeService;
import oms.order.OrderReadModel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public final class OmsHttpServer {
    private final int port;
    private final OrderReadModel orderReadModel;
    private final GatewayAuditIntakeService auditIntakeService;

    public OmsHttpServer(int port, OrderReadModel orderReadModel, GatewayAuditIntakeService auditIntakeService) {
        this.port = port;
        this.orderReadModel = orderReadModel;
        this.auditIntakeService = auditIntakeService;
    }

    public void start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", new JsonHttpHandler(exchange ->
            JsonHttpHandler.JsonResponse.ok(new HealthResponse("UP", "oms-java"))
        ));
        server.createContext("/stats", new OmsStatsHttpHandler(auditIntakeService));
        server.createContext("/reconcile", new OmsReconcileHttpHandler(auditIntakeService));
        server.createContext("/orders", new OrderHttpHandler(orderReadModel));
        server.createContext("/accounts", new AccountHttpHandler(orderReadModel));
        server.createContext("/internal/orders", new OrderInternalHttpHandler(orderReadModel));
        server.createContext("/internal/accounts", new OrderInternalHttpHandler(orderReadModel));
        server.createContext("/internal/audit", new OmsInternalAuditHttpHandler(auditIntakeService));
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
        System.out.println("oms-java listening on http://localhost:" + port);
    }

    public record HealthResponse(String status, String service) {
    }
}
