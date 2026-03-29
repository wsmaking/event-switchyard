package backofficejava.http;

import backofficejava.business.PostTradePackageReadModel;

public final class PostTradePackageHttpHandler extends JsonHttpHandler {
    public PostTradePackageHttpHandler(PostTradePackageReadModel postTradePackageReadModel) {
        super(exchange -> {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                throw new MethodNotAllowedException(exchange.getRequestMethod());
            }
            String orderId = parseQuery(exchange.getRequestURI().getRawQuery()).get("orderId");
            if (orderId == null || orderId.isBlank()) {
                throw new NotFoundException("missing_order_id");
            }
            return postTradePackageReadModel.findByOrderId(orderId)
                .<JsonResponse>map(JsonResponse::ok)
                .orElseThrow(() -> new NotFoundException("post_trade_package_not_found:" + orderId));
        });
    }
}
