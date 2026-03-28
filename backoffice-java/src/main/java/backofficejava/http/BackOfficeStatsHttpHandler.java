package backofficejava.http;

import backofficejava.audit.GatewayAuditIntakeService;

public final class BackOfficeStatsHttpHandler extends JsonHttpHandler {
    public BackOfficeStatsHttpHandler(GatewayAuditIntakeService intakeService) {
        super(exchange -> JsonResponse.ok(intakeService.snapshot()));
    }
}
