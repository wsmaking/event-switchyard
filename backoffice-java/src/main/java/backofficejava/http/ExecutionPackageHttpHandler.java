package backofficejava.http;

import backofficejava.business.ExecutionPackageReadModel;

public final class ExecutionPackageHttpHandler extends JsonHttpHandler {
    public ExecutionPackageHttpHandler(ExecutionPackageReadModel executionPackageReadModel) {
        super(exchange -> {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                throw new MethodNotAllowedException(exchange.getRequestMethod());
            }
            String orderId = parseQuery(exchange.getRequestURI().getRawQuery()).get("orderId");
            if (orderId == null || orderId.isBlank()) {
                throw new NotFoundException("missing_order_id");
            }
            return executionPackageReadModel.findByOrderId(orderId)
                .<JsonResponse>map(JsonResponse::ok)
                .orElseThrow(() -> new NotFoundException("execution_package_not_found:" + orderId));
        });
    }
}
