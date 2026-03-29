package backofficejava.http;

import backofficejava.business.CorporateActionWorkflowReadModel;

public final class CorporateActionWorkflowHttpHandler extends JsonHttpHandler {
    public CorporateActionWorkflowHttpHandler(CorporateActionWorkflowReadModel readModel) {
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
                .orElseThrow(() -> new NotFoundException("corporate_action_workflow_not_found:" + orderId));
        });
    }
}
