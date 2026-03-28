package backofficejava.http;

import backofficejava.audit.GatewayAuditIntakeService;

public final class BackOfficeInternalAuditHttpHandler extends JsonHttpHandler {
    public BackOfficeInternalAuditHttpHandler(GatewayAuditIntakeService intakeService) {
        super(exchange -> {
            String path = exchange.getRequestURI().getPath();
            if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/audit/replay".equals(path)) {
                GatewayAuditIntakeService.ReplayRequest request =
                    exchange.getRequestBody().available() > 0
                        ? readJson(exchange, GatewayAuditIntakeService.ReplayRequest.class)
                        : new GatewayAuditIntakeService.ReplayRequest(true);
                return JsonResponse.ok(intakeService.replayFromStart(request.shouldReset()));
            }
            throw new NotFoundException("route_not_found:" + path);
        });
    }
}
