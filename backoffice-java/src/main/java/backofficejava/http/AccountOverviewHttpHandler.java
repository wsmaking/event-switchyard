package backofficejava.http;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.AccountOverviewView;
import com.sun.net.httpserver.HttpExchange;

import java.util.Optional;

public final class AccountOverviewHttpHandler extends JsonHttpHandler {
    public AccountOverviewHttpHandler(AccountOverviewReadModel readModel) {
        super(exchange -> route(exchange, readModel));
    }

    private static JsonResponse route(HttpExchange exchange, AccountOverviewReadModel readModel) {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            throw new MethodNotAllowedException(exchange.getRequestMethod());
        }

        String path = exchange.getRequestURI().getPath();
        String[] segments = path.split("/");
        if (segments.length == 4 && "accounts".equals(segments[1]) && "overview".equals(segments[3])) {
            Optional<AccountOverviewView> overview = readModel.findByAccountId(segments[2]);
            return overview.<JsonResponse>map(JsonResponse::ok)
                .orElseThrow(() -> new NotFoundException("account_not_found:" + segments[2]));
        }

        throw new NotFoundException("route_not_found:" + path);
    }
}
