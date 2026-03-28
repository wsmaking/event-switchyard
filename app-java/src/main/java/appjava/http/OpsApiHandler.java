package appjava.http;

import appjava.clients.BackOfficeClient;
import appjava.clients.OmsClient;
import appjava.ops.AuditReplayOverview;
import appjava.ops.OpsOverview;
import com.sun.net.httpserver.HttpExchange;

import java.util.Map;

public final class OpsApiHandler extends JsonHttpHandler {
    public OpsApiHandler(String accountId, OmsClient omsClient, BackOfficeClient backOfficeClient) {
        super(exchange -> route(exchange, accountId, omsClient, backOfficeClient));
    }

    private static JsonResponse route(
        HttpExchange exchange,
        String accountId,
        OmsClient omsClient,
        BackOfficeClient backOfficeClient
    ) throws Exception {
        String path = exchange.getRequestURI().getPath();
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/ops/overview".equals(path)) {
            Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
            String resolvedAccountId = query.getOrDefault("accountId", accountId);
            String orderId = query.get("orderId");
            return JsonResponse.ok(new OpsOverview(
                resolvedAccountId,
                orderId,
                omsClient.fetchStats(),
                omsClient.fetchReconcile(resolvedAccountId),
                omsClient.fetchOrphans(orderId, 20),
                backOfficeClient.fetchStats(),
                backOfficeClient.fetchReconcile(resolvedAccountId),
                backOfficeClient.fetchLedger(resolvedAccountId, orderId, 50),
                backOfficeClient.fetchOrphans(orderId, 20)
            ));
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/ops/audit/replay".equals(path)) {
            ReplayRequest request = exchange.getRequestBody().available() > 0
                ? readJson(exchange, ReplayRequest.class)
                : new ReplayRequest(true);
            OmsClient.ReplayResult omsReplay = omsClient.replayGatewayAudit(request.resetState());
            BackOfficeClient.ReplayResult backOfficeReplay = backOfficeClient.replayGatewayAudit(request.resetState());
            if (omsReplay == null || backOfficeReplay == null) {
                throw new IllegalStateException("audit_replay_failed");
            }
            return JsonResponse.ok(new AuditReplayOverview(omsReplay, backOfficeReplay));
        }
        throw new NotFoundException("route_not_found:" + path);
    }

    public record ReplayRequest(boolean resetState) {
    }
}
