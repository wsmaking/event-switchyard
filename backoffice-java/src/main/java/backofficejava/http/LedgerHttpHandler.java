package backofficejava.http;

import backofficejava.account.LedgerReadModel;

import java.util.Map;

public final class LedgerHttpHandler extends JsonHttpHandler {
    public LedgerHttpHandler(LedgerReadModel ledgerReadModel) {
        super(exchange -> {
            Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
            int limit = query.containsKey("limit") ? Integer.parseInt(query.get("limit")) : 100;
            Long after = query.containsKey("after") ? Long.parseLong(query.get("after")) : null;
            return JsonResponse.ok(new LedgerResponse(
                query.get("accountId"),
                ledgerReadModel.find(query.get("accountId"), query.get("orderId"), query.get("type"), limit, after)
            ));
        });
    }

    public record LedgerResponse(String accountId, java.util.List<backofficejava.account.LedgerEntryView> entries) {
    }
}
