package oms.http;

import com.sun.net.httpserver.HttpServer;
import oms.order.OrderReadModel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public final class OmsHttpServer {
    private final int port;
    private final OrderReadModel orderReadModel;

    public OmsHttpServer(int port, OrderReadModel orderReadModel) {
        this.port = port;
        this.orderReadModel = orderReadModel;
    }

    public void start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", new JsonHttpHandler(exchange ->
            JsonHttpHandler.JsonResponse.ok(new HealthResponse("UP", "oms-java"))
        ));
        server.createContext("/orders", new OrderHttpHandler(orderReadModel));
        server.createContext("/internal/orders", new OrderInternalHttpHandler(orderReadModel));
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
        System.out.println("oms-java listening on http://localhost:" + port);
    }

    public record HealthResponse(String status, String service) {
    }
}
