package backofficejava.http;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.PositionReadModel;
import com.sun.net.httpserver.HttpExchange;

public final class DemoResetHttpHandler extends JsonHttpHandler {
    public DemoResetHttpHandler(
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel
    ) {
        super(exchange -> route(exchange, accountOverviewReadModel, positionReadModel));
    }

    private static JsonResponse route(
        HttpExchange exchange,
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel
    ) {
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            return new JsonResponse(204, new ResetResponse("OK"));
        }
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            throw new MethodNotAllowedException(exchange.getRequestMethod());
        }
        accountOverviewReadModel.reset();
        positionReadModel.reset();
        return JsonResponse.ok(new ResetResponse("RESET"));
    }

    public record ResetResponse(String status) {
    }
}
