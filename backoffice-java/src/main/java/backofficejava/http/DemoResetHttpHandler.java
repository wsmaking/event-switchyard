package backofficejava.http;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.FillReadModel;
import backofficejava.account.PositionReadModel;
import com.sun.net.httpserver.HttpExchange;

public final class DemoResetHttpHandler extends JsonHttpHandler {
    public DemoResetHttpHandler(
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel
    ) {
        super(exchange -> route(exchange, accountOverviewReadModel, positionReadModel, fillReadModel));
    }

    private static JsonResponse route(
        HttpExchange exchange,
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel
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
        return JsonResponse.ok(new ResetResponse("RESET"));
    }

    public record ResetResponse(String status) {
    }
}
