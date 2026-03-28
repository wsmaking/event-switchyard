package appjava.http;

import appjava.clients.BackOfficeClient;
import com.sun.net.httpserver.HttpExchange;

public final class AccountApiHandler extends JsonHttpHandler {
    public AccountApiHandler(BackOfficeClient backOfficeClient) {
        super(exchange -> route(exchange, backOfficeClient));
    }

    private static JsonResponse route(HttpExchange exchange, BackOfficeClient backOfficeClient) {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            throw new MethodNotAllowedException(exchange.getRequestMethod());
        }
        String[] segments = exchange.getRequestURI().getPath().split("/");
        if (segments.length == 5 && "api".equals(segments[1]) && "accounts".equals(segments[2]) && "overview".equals(segments[4])) {
            return JsonResponse.ok(backOfficeClient.fetchOverview(segments[3]));
        }
        throw new NotFoundException("route_not_found:" + exchange.getRequestURI().getPath());
    }
}
