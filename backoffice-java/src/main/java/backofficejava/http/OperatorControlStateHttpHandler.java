package backofficejava.http;

import backofficejava.business.OperatorControlStateReadModel;

public final class OperatorControlStateHttpHandler extends JsonHttpHandler {
    public OperatorControlStateHttpHandler(OperatorControlStateReadModel readModel) {
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
                .orElseThrow(() -> new NotFoundException("operator_control_state_not_found:" + orderId));
        });
    }
}
