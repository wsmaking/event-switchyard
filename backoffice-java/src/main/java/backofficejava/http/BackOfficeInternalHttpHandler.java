package backofficejava.http;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.AccountOverviewView;
import backofficejava.account.FillReadModel;
import backofficejava.account.FillView;
import backofficejava.account.LedgerReadModel;
import backofficejava.account.LedgerEntryView;
import backofficejava.account.OrderProjectionState;
import backofficejava.account.OrderProjectionStateStore;
import backofficejava.account.PositionReadModel;
import backofficejava.account.PositionView;
import com.sun.net.httpserver.HttpExchange;

import java.util.List;

public final class BackOfficeInternalHttpHandler extends JsonHttpHandler {
    public BackOfficeInternalHttpHandler(
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel,
        OrderProjectionStateStore orderProjectionStateStore,
        LedgerReadModel ledgerReadModel
    ) {
        super(exchange -> route(exchange, accountOverviewReadModel, positionReadModel, fillReadModel, orderProjectionStateStore, ledgerReadModel));
    }

    private static JsonResponse route(
        HttpExchange exchange,
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel,
        OrderProjectionStateStore orderProjectionStateStore,
        LedgerReadModel ledgerReadModel
    ) throws Exception {
        String path = exchange.getRequestURI().getPath();
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/accounts/upsert".equals(path)) {
            AccountOverviewView request = readJson(exchange, AccountOverviewView.class);
            accountOverviewReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/positions/replace".equals(path)) {
            ReplacePositionsRequest request = readJson(exchange, ReplacePositionsRequest.class);
            positionReadModel.replacePositions(request.accountId(), request.positions());
            return JsonResponse.ok(new ReplacePositionsResponse("REPLACED", request.positions().size()));
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/fills/replace".equals(path)) {
            ReplaceFillsRequest request = readJson(exchange, ReplaceFillsRequest.class);
            fillReadModel.replaceFills(request.orderId(), request.fills());
            return JsonResponse.ok(new ReplaceFillsResponse("REPLACED", request.fills().size()));
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/orders/state/upsert".equals(path)) {
            OrderProjectionState request = readJson(exchange, OrderProjectionState.class);
            orderProjectionStateStore.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/ledger/replace".equals(path)) {
            ReplaceLedgerRequest request = readJson(exchange, ReplaceLedgerRequest.class);
            ledgerReadModel.reset();
            request.entries().forEach(ledgerReadModel::append);
            return JsonResponse.ok(new ReplaceLedgerResponse("REPLACED", request.entries().size()));
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/reset".equals(path)) {
            accountOverviewReadModel.reset();
            positionReadModel.reset();
            fillReadModel.reset();
            orderProjectionStateStore.reset();
            ledgerReadModel.reset();
            return JsonResponse.ok(new ResetResponse("RESET"));
        }
        throw new NotFoundException("route_not_found:" + path);
    }

    public record ReplacePositionsRequest(String accountId, List<PositionView> positions) {
    }

    public record ReplacePositionsResponse(String status, int count) {
    }

    public record ReplaceFillsRequest(String orderId, List<FillView> fills) {
    }

    public record ReplaceFillsResponse(String status, int count) {
    }

    public record ReplaceLedgerRequest(List<LedgerEntryView> entries) {
    }

    public record ReplaceLedgerResponse(String status, int count) {
    }

    public record ResetResponse(String status) {
    }
}
