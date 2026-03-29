package backofficejava.http;

import backofficejava.business.StatementProjectionReadModel;

public final class StatementProjectionHttpHandler extends JsonHttpHandler {
    public StatementProjectionHttpHandler(StatementProjectionReadModel readModel) {
        super(exchange -> {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                throw new MethodNotAllowedException(exchange.getRequestMethod());
            }
            String orderId = parseQuery(exchange.getRequestURI().getRawQuery()).get("orderId");
            if (orderId == null || orderId.isBlank()) {
                throw new NotFoundException("missing_order_id");
            }
            return readModel.findByOrderId(orderId)
                .<JsonResponse>map(JsonResponse::ok)
                .orElseThrow(() -> new NotFoundException("statement_projection_not_found:" + orderId));
        });
    }
}
