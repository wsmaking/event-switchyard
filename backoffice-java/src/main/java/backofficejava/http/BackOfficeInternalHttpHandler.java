package backofficejava.http;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.AccountOverviewView;
import backofficejava.account.PositionReadModel;
import backofficejava.account.PositionView;
import com.sun.net.httpserver.HttpExchange;

import java.util.List;

public final class BackOfficeInternalHttpHandler extends JsonHttpHandler {
    public BackOfficeInternalHttpHandler(
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel
    ) {
        super(exchange -> route(exchange, accountOverviewReadModel, positionReadModel));
    }

    private static JsonResponse route(
        HttpExchange exchange,
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel
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
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/reset".equals(path)) {
            accountOverviewReadModel.reset();
            positionReadModel.reset();
            return JsonResponse.ok(new ResetResponse("RESET"));
        }
        throw new NotFoundException("route_not_found:" + path);
    }

    public record ReplacePositionsRequest(String accountId, List<PositionView> positions) {
    }

    public record ReplacePositionsResponse(String status, int count) {
    }

    public record ResetResponse(String status) {
    }
}
