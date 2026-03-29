package appjava.http;

import appjava.clients.BackOfficeClient;
import appjava.clients.OmsClient;
import appjava.ops.AuditReplayOverview;
import appjava.ops.OpsOverview;
import appjava.ops.ProductionEngineeringService;
import com.sun.net.httpserver.HttpExchange;

import java.util.Map;

public final class OpsApiHandler extends JsonHttpHandler {
    public OpsApiHandler(
        String accountId,
        OmsClient omsClient,
        BackOfficeClient backOfficeClient,
        ProductionEngineeringService productionEngineeringService
    ) {
        super(exchange -> route(exchange, accountId, omsClient, backOfficeClient, productionEngineeringService));
    }

    private static JsonResponse route(
        HttpExchange exchange,
        String accountId,
        OmsClient omsClient,
        BackOfficeClient backOfficeClient,
        ProductionEngineeringService productionEngineeringService
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
                omsClient.fetchBusStats(),
                omsClient.fetchReconcile(resolvedAccountId),
                omsClient.fetchOrphans(orderId, 20),
                omsClient.fetchPendingOrphans(orderId, 20),
                backOfficeClient.fetchStats(),
                backOfficeClient.fetchBusStats(),
                backOfficeClient.fetchReconcile(resolvedAccountId),
                backOfficeClient.fetchLedger(resolvedAccountId, orderId, 50),
                backOfficeClient.fetchOrphans(orderId, 20),
                backOfficeClient.fetchPendingOrphans(orderId, 20)
            ));
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/ops/production-engineering".equals(path)) {
            return JsonResponse.ok(productionEngineeringService.buildSnapshot());
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/ops/venue-sessions".equals(path)) {
            return JsonResponse.ok(productionEngineeringService.buildSnapshot().venueSessions());
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/ops/rollout-state".equals(path)) {
            return JsonResponse.ok(productionEngineeringService.buildSnapshot().rollout());
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/ops/go-no-go".equals(path)) {
            return JsonResponse.ok(productionEngineeringService.buildSnapshot().goNoGo());
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
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/ops/orphans/requeue".equals(path)) {
            RequeueRequest request = exchange.getRequestBody().available() > 0
                ? readJson(exchange, RequeueRequest.class)
                : new RequeueRequest(null);
            OmsClient.RequeueResult omsResult = omsClient.requeuePendingOrphans(request.orderId());
            BackOfficeClient.RequeueResult backOfficeResult = backOfficeClient.requeuePendingOrphans(request.orderId());
            if (omsResult == null || backOfficeResult == null) {
                throw new IllegalStateException("orphan_requeue_failed");
            }
            return JsonResponse.ok(new OrphanRequeueOverview(omsResult, backOfficeResult));
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/ops/dlq/requeue".equals(path)) {
            DeadLetterRequeueRequest request = exchange.getRequestBody().available() > 0
                ? readJson(exchange, DeadLetterRequeueRequest.class)
                : new DeadLetterRequeueRequest(null);
            OmsClient.DeadLetterRequeueResult omsResult = omsClient.requeueDeadLetter(request.eventRef());
            BackOfficeClient.DeadLetterRequeueResult backOfficeResult = backOfficeClient.requeueDeadLetter(request.eventRef());
            if (omsResult == null || backOfficeResult == null) {
                throw new IllegalStateException("dlq_requeue_failed");
            }
            return JsonResponse.ok(new DeadLetterRequeueOverview(omsResult, backOfficeResult));
        }
        throw new NotFoundException("route_not_found:" + path);
    }

    public record ReplayRequest(boolean resetState) {
    }

    public record RequeueRequest(String orderId) {
    }

    public record OrphanRequeueOverview(
        OmsClient.RequeueResult oms,
        BackOfficeClient.RequeueResult backOffice
    ) {
    }

    public record DeadLetterRequeueRequest(String eventRef) {
    }

    public record DeadLetterRequeueOverview(
        OmsClient.DeadLetterRequeueResult oms,
        BackOfficeClient.DeadLetterRequeueResult backOffice
    ) {
    }
}
