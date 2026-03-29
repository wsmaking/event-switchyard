package backofficejava.http;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.FillReadModel;
import backofficejava.account.LedgerReadModel;
import backofficejava.account.OrderProjectionStateStore;
import backofficejava.account.PositionReadModel;
import backofficejava.business.ExecutionPackageReadModel;
import backofficejava.business.PostTradePackageReadModel;
import com.sun.net.httpserver.HttpExchange;

public final class DemoResetHttpHandler extends JsonHttpHandler {
    public DemoResetHttpHandler(
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel,
        OrderProjectionStateStore orderProjectionStateStore,
        LedgerReadModel ledgerReadModel,
        ExecutionPackageReadModel executionPackageReadModel,
        PostTradePackageReadModel postTradePackageReadModel
    ) {
        super(exchange -> route(
            exchange,
            accountOverviewReadModel,
            positionReadModel,
            fillReadModel,
            orderProjectionStateStore,
            ledgerReadModel,
            executionPackageReadModel,
            postTradePackageReadModel
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
        PostTradePackageReadModel postTradePackageReadModel
    ) {
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            return new JsonResponse(204, new ResetResponse("OK"));
        }
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            throw new MethodNotAllowedException(exchange.getRequestMethod());
        }
        accountOverviewReadModel.reset();
        positionReadModel.reset();
        fillReadModel.reset();
        orderProjectionStateStore.reset();
        ledgerReadModel.reset();
        executionPackageReadModel.reset();
        postTradePackageReadModel.reset();
        return JsonResponse.ok(new ResetResponse("RESET"));
    }

    public record ResetResponse(String status) {
    }
}
