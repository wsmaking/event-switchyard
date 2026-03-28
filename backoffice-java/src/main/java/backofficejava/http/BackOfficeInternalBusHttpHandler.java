package backofficejava.http;

import backofficejava.bus.BusEventIntakeService;
import backofficejava.bus.BusEventV2;

public final class BackOfficeInternalBusHttpHandler extends JsonHttpHandler {
    public BackOfficeInternalBusHttpHandler(BusEventIntakeService intakeService) {
        super(exchange -> {
            String path = exchange.getRequestURI().getPath();
            if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/bus/stats".equals(path)) {
                return JsonResponse.ok(intakeService.snapshot());
            }
            if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/bus/ingest".equals(path)) {
                return JsonResponse.ok(intakeService.ingest(readJson(exchange, BusEventV2.class)));
            }
            throw new NotFoundException("route_not_found:" + path);
        });
    }
}
