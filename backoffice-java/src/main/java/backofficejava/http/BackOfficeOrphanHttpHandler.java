package backofficejava.http;

import backofficejava.audit.GatewayAuditIntakeService;

import java.util.Map;

public final class BackOfficeOrphanHttpHandler extends JsonHttpHandler {
    public BackOfficeOrphanHttpHandler(GatewayAuditIntakeService intakeService) {
        super(exchange -> {
            Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
            int limit = query.containsKey("limit") ? Integer.parseInt(query.get("limit")) : 50;
            return JsonResponse.ok(intakeService.findDeadLetters(query.get("orderId"), limit));
        });
    }
}
