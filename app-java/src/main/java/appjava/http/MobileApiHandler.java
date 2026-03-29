package appjava.http;

import appjava.mobile.MobileLearningService;
import com.sun.net.httpserver.HttpExchange;

public final class MobileApiHandler extends JsonHttpHandler {
    public MobileApiHandler(MobileLearningService mobileLearningService) {
        super(exchange -> route(exchange, mobileLearningService));
    }

    private static JsonResponse route(HttpExchange exchange, MobileLearningService mobileLearningService) throws Exception {
        String path = exchange.getRequestURI().getPath();
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/mobile/home".equals(path)) {
            return JsonResponse.ok(mobileLearningService.buildHome());
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/mobile/cards".equals(path)) {
            return JsonResponse.ok(mobileLearningService.listCards());
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/mobile/drills".equals(path)) {
            return JsonResponse.ok(mobileLearningService.listDrills());
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/mobile/institutional-flow".equals(path)) {
            return JsonResponse.ok(mobileLearningService.buildInstitutionalFlow());
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/mobile/post-trade".equals(path)) {
            return JsonResponse.ok(mobileLearningService.buildPostTradeGuide());
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/mobile/risk/deep-dive".equals(path)) {
            return JsonResponse.ok(mobileLearningService.buildRiskDeepDive());
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/mobile/asset-classes".equals(path)) {
            return JsonResponse.ok(mobileLearningService.buildAssetClassGuide());
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/mobile/operations".equals(path)) {
            return JsonResponse.ok(mobileLearningService.buildOperationsEngineering());
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/mobile/progress".equals(path)) {
            return JsonResponse.ok(mobileLearningService.getProgress());
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/mobile/progress".equals(path)) {
            return JsonResponse.ok(mobileLearningService.applyProgress(readJson(exchange, MobileLearningService.ProgressUpdateRequest.class)));
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/mobile/drills/attempt".equals(path)) {
            return JsonResponse.ok(mobileLearningService.applyDrillAttempt(readJson(exchange, MobileLearningService.DrillAttemptRequest.class)));
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/mobile/risk/scenarios".equals(path)) {
            return JsonResponse.ok(mobileLearningService.listRiskScenarios());
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/mobile/risk/evaluate".equals(path)) {
            MobileLearningService.RiskEvaluationRequest request = exchange.getRequestBody().available() > 0
                ? readJson(exchange, MobileLearningService.RiskEvaluationRequest.class)
                : new MobileLearningService.RiskEvaluationRequest(null, null, null);
            return JsonResponse.ok(mobileLearningService.evaluateRisk(request));
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/api/mobile/risk/options/evaluate".equals(path)) {
            MobileLearningService.OptionEvaluateRequest request = exchange.getRequestBody().available() > 0
                ? readJson(exchange, MobileLearningService.OptionEvaluateRequest.class)
                : new MobileLearningService.OptionEvaluateRequest(null, null, null, null, null, null, null, null);
            return JsonResponse.ok(mobileLearningService.evaluateOption(request));
        }
        if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            String[] segments = path.split("/");
            if (segments.length == 5 && "api".equals(segments[1]) && "mobile".equals(segments[2]) && "cards".equals(segments[3])) {
                return JsonResponse.ok(mobileLearningService.getCard(segments[4]));
            }
            if (segments.length == 5 && "api".equals(segments[1]) && "mobile".equals(segments[2]) && "drills".equals(segments[3])) {
                return JsonResponse.ok(mobileLearningService.getDrill(segments[4]));
            }
        }
        throw new NotFoundException("route_not_found:" + path);
    }
}
