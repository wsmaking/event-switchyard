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
import backofficejava.business.AllocationStateReadModel;
import backofficejava.business.AllocationStateView;
import backofficejava.business.ExecutionPackageReadModel;
import backofficejava.business.ExecutionPackageView;
import backofficejava.business.ParentExecutionStateReadModel;
import backofficejava.business.ParentExecutionStateView;
import backofficejava.business.PostTradePackageReadModel;
import backofficejava.business.PostTradePackageView;
import com.sun.net.httpserver.HttpExchange;

import java.util.List;

public final class BackOfficeInternalHttpHandler extends JsonHttpHandler {
    public BackOfficeInternalHttpHandler(
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel,
        OrderProjectionStateStore orderProjectionStateStore,
        LedgerReadModel ledgerReadModel,
        ExecutionPackageReadModel executionPackageReadModel,
        PostTradePackageReadModel postTradePackageReadModel,
        ParentExecutionStateReadModel parentExecutionStateReadModel,
        AllocationStateReadModel allocationStateReadModel
    ) {
        super(exchange -> route(
            exchange,
            accountOverviewReadModel,
            positionReadModel,
            fillReadModel,
            orderProjectionStateStore,
            ledgerReadModel,
            executionPackageReadModel,
            postTradePackageReadModel,
            parentExecutionStateReadModel,
            allocationStateReadModel
        ));
    }

    private static JsonResponse route(
        HttpExchange exchange,
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel,
        OrderProjectionStateStore orderProjectionStateStore,
        LedgerReadModel ledgerReadModel,
        ExecutionPackageReadModel executionPackageReadModel,
        PostTradePackageReadModel postTradePackageReadModel,
        ParentExecutionStateReadModel parentExecutionStateReadModel,
        AllocationStateReadModel allocationStateReadModel
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
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/execution-package/upsert".equals(path)) {
            ExecutionPackageView request = readJson(exchange, ExecutionPackageView.class);
            executionPackageReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/post-trade-package/upsert".equals(path)) {
            PostTradePackageView request = readJson(exchange, PostTradePackageView.class);
            postTradePackageReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/parent-execution-state/upsert".equals(path)) {
            ParentExecutionStateView request = readJson(exchange, ParentExecutionStateView.class);
            parentExecutionStateReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/allocation-state/upsert".equals(path)) {
            AllocationStateView request = readJson(exchange, AllocationStateView.class);
            allocationStateReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/reset".equals(path)) {
            accountOverviewReadModel.reset();
            positionReadModel.reset();
            fillReadModel.reset();
            orderProjectionStateStore.reset();
            ledgerReadModel.reset();
            executionPackageReadModel.reset();
            postTradePackageReadModel.reset();
            parentExecutionStateReadModel.reset();
            allocationStateReadModel.reset();
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
