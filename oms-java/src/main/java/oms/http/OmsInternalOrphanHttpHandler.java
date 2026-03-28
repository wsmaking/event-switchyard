package oms.http;

import oms.audit.GatewayAuditIntakeService;

public final class OmsInternalOrphanHttpHandler extends JsonHttpHandler {
    public OmsInternalOrphanHttpHandler(GatewayAuditIntakeService intakeService) {
        super(exchange -> {
            String path = exchange.getRequestURI().getPath();
            if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/orphans/requeue".equals(path)) {
                RequeueRequest request = exchange.getRequestBody().available() > 0
                    ? readJson(exchange, RequeueRequest.class)
                    : new RequeueRequest(null);
                return JsonResponse.ok(intakeService.requeuePending(request.orderId()));
            }
            if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/orphans/dlq/requeue".equals(path)) {
                DeadLetterRequeueRequest request = exchange.getRequestBody().available() > 0
                    ? readJson(exchange, DeadLetterRequeueRequest.class)
                    : new DeadLetterRequeueRequest(null);
                return JsonResponse.ok(intakeService.requeueDeadLetter(request.eventRef()));
            }
            throw new NotFoundException("route_not_found:" + path);
        });
    }

    public record RequeueRequest(String orderId) {
    }

    public record DeadLetterRequeueRequest(String eventRef) {
    }
}
