package oms.http;

import oms.audit.GatewayAuditIntakeService;

public final class OmsStatsHttpHandler extends JsonHttpHandler {
    public OmsStatsHttpHandler(GatewayAuditIntakeService intakeService) {
        super(exchange -> JsonResponse.ok(intakeService.snapshot()));
    }
}
