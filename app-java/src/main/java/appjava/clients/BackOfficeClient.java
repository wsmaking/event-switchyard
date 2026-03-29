package appjava.clients;

import appjava.account.AccountOverview;
import appjava.order.FillView;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

public final class BackOfficeClient {
    private final String accountId;
    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public BackOfficeClient(String accountId) {
        this.accountId = accountId;
        this.baseUrl = System.getenv().getOrDefault("BACKOFFICE_JAVA_BASE_URL", "http://localhost:18082");
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public AccountOverview fetchOverview(String requestedAccountId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/accounts/" + requestedAccountId + "/overview"))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), AccountOverview.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return AccountOverview.empty(requestedAccountId);
    }

    public List<BackOfficePosition> fetchPositions(String requestedAccountId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/positions?accountId=" + requestedAccountId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                PositionsResponse parsed = objectMapper.readValue(response.body(), PositionsResponse.class);
                return parsed.positions == null ? List.of() : parsed.positions;
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return List.of();
    }

    public List<FillView> fetchFills(String orderId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/fills?orderId=" + orderId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                FillsResponse parsed = objectMapper.readValue(response.body(), FillsResponse.class);
                return parsed.fills == null ? List.of() : parsed.fills;
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return List.of();
    }

    public ExecutionPackage fetchExecutionPackage(String orderId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/business/execution-package?orderId=" + orderId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), ExecutionPackage.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public PostTradePackage fetchPostTradePackage(String orderId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/business/post-trade-package?orderId=" + orderId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), PostTradePackage.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public ParentExecutionState fetchParentExecutionState(String orderId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/business/parent-execution-state?orderId=" + orderId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), ParentExecutionState.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public AllocationState fetchAllocationState(String orderId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/business/allocation-state?orderId=" + orderId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), AllocationState.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public SettlementProjection fetchSettlementProjection(String orderId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/business/settlement-projection?orderId=" + orderId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), SettlementProjection.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public StatementProjection fetchStatementProjection(String orderId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/business/statement-projection?orderId=" + orderId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), StatementProjection.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public RiskSnapshot fetchRiskSnapshot(String requestedAccountId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/business/risk-snapshot?accountId=" + requestedAccountId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), RiskSnapshot.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public SettlementExceptionWorkflow fetchSettlementExceptionWorkflow(String orderId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/business/settlement-exception-workflow?orderId=" + orderId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), SettlementExceptionWorkflow.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public CorporateActionWorkflow fetchCorporateActionWorkflow(String orderId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/business/corporate-action-workflow?orderId=" + orderId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), CorporateActionWorkflow.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public MarginProjection fetchMarginProjection(String requestedAccountId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/business/margin-projection?accountId=" + requestedAccountId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), MarginProjection.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public ScenarioEvaluationHistory fetchScenarioEvaluationHistory(String requestedAccountId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/business/scenario-evaluation-history?accountId=" + requestedAccountId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), ScenarioEvaluationHistory.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public BacktestHistory fetchBacktestHistory(String requestedAccountId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/business/backtest-history?accountId=" + requestedAccountId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), BacktestHistory.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public void resetDemo() {
        postNoBody("/demo/reset");
    }

    public void resetState() {
        postNoBody("/internal/reset");
    }

    public void upsertOverview(AccountOverview overview) {
        postJson("/internal/accounts/upsert", overview);
    }

    public void replacePositions(String requestedAccountId, List<BackOfficePosition> positions) {
        postJson("/internal/positions/replace", new ReplacePositionsRequest(requestedAccountId, positions));
    }

    public void replaceFills(String orderId, List<FillView> fills) {
        postJson("/internal/fills/replace", new ReplaceFillsRequest(orderId, fills));
    }

    public void upsertOrderState(BackOfficeOrderState state) {
        postJson("/internal/orders/state/upsert", state);
    }

    public void replaceLedger(List<LedgerEntry> entries) {
        postJson("/internal/ledger/replace", new ReplaceLedgerRequest(entries));
    }

    public void upsertExecutionPackage(ExecutionPackage executionPackage) {
        postJson("/internal/business/execution-package/upsert", executionPackage);
    }

    public void upsertPostTradePackage(PostTradePackage postTradePackage) {
        postJson("/internal/business/post-trade-package/upsert", postTradePackage);
    }

    public void upsertParentExecutionState(ParentExecutionState parentExecutionState) {
        postJson("/internal/business/parent-execution-state/upsert", parentExecutionState);
    }

    public void upsertAllocationState(AllocationState allocationState) {
        postJson("/internal/business/allocation-state/upsert", allocationState);
    }

    public void upsertSettlementProjection(SettlementProjection settlementProjection) {
        postJson("/internal/business/settlement-projection/upsert", settlementProjection);
    }

    public void upsertStatementProjection(StatementProjection statementProjection) {
        postJson("/internal/business/statement-projection/upsert", statementProjection);
    }

    public void upsertRiskSnapshot(RiskSnapshot riskSnapshot) {
        postJson("/internal/business/risk-snapshot/upsert", riskSnapshot);
    }

    public void upsertSettlementExceptionWorkflow(SettlementExceptionWorkflow workflow) {
        postJson("/internal/business/settlement-exception-workflow/upsert", workflow);
    }

    public void upsertCorporateActionWorkflow(CorporateActionWorkflow workflow) {
        postJson("/internal/business/corporate-action-workflow/upsert", workflow);
    }

    public void upsertMarginProjection(MarginProjection projection) {
        postJson("/internal/business/margin-projection/upsert", projection);
    }

    public void upsertScenarioEvaluationHistory(ScenarioEvaluationHistory history) {
        postJson("/internal/business/scenario-evaluation-history/upsert", history);
    }

    public void upsertBacktestHistory(BacktestHistory history) {
        postJson("/internal/business/backtest-history/upsert", history);
    }

    public BackOfficeStats fetchStats() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/stats"))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), BackOfficeStats.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public BackOfficeReconcile fetchReconcile(String requestedAccountId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/reconcile?accountId=" + requestedAccountId))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), BackOfficeReconcile.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public BackOfficeBusStats fetchBusStats() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/internal/bus/stats"))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), BackOfficeBusStats.class);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public List<LedgerEntry> fetchLedger(String requestedAccountId, String orderId, int limit) {
        try {
            StringBuilder uri = new StringBuilder(baseUrl + "/ledger?accountId=" + requestedAccountId + "&limit=" + limit);
            if (orderId != null && !orderId.isBlank()) {
                uri.append("&orderId=").append(orderId);
            }
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri.toString()))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                LedgerResponse parsed = objectMapper.readValue(response.body(), LedgerResponse.class);
                return parsed.entries == null ? List.of() : parsed.entries;
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return List.of();
    }

    public ReplayResult replayGatewayAudit(boolean resetState) {
        return postJsonWithResponse("/internal/audit/replay", new ReplayRequest(resetState), ReplayResult.class);
    }

    public List<DeadLetterEntry> fetchOrphans(String orderId, int limit) {
        try {
            StringBuilder uri = new StringBuilder(baseUrl + "/orphans?limit=" + limit);
            if (orderId != null && !orderId.isBlank()) {
                uri.append("&orderId=").append(orderId);
            }
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri.toString()))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), objectMapper.getTypeFactory().constructCollectionType(List.class, DeadLetterEntry.class));
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return List.of();
    }

    public List<PendingOrphanEntry> fetchPendingOrphans(String orderId, int limit) {
        try {
            StringBuilder uri = new StringBuilder(baseUrl + "/orphans/pending?limit=" + limit);
            if (orderId != null && !orderId.isBlank()) {
                uri.append("&orderId=").append(orderId);
            }
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri.toString()))
                .GET()
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), objectMapper.getTypeFactory().constructCollectionType(List.class, PendingOrphanEntry.class));
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return List.of();
    }

    public RequeueResult requeuePendingOrphans(String orderId) {
        return postJsonWithResponse("/internal/orphans/requeue", new RequeueRequest(orderId), RequeueResult.class);
    }

    public DeadLetterRequeueResult requeueDeadLetter(String eventRef) {
        return postJsonWithResponse("/internal/orphans/dlq/requeue", new DeadLetterRequeueRequest(eventRef), DeadLetterRequeueResult.class);
    }

    private void postNoBody(String path) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .POST(HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(3))
                .build();
            httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
    }

    private void postJson(String path, Object payload) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)))
                .timeout(Duration.ofSeconds(3))
                .build();
            httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
    }

    private <T> T postJsonWithResponse(String path, Object payload, Class<T> responseType) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)))
                .timeout(Duration.ofSeconds(3))
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return objectMapper.readValue(response.body(), responseType);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } catch (Exception ignored) {
        }
        return null;
    }

    public String accountId() {
        return accountId;
    }

    public record BackOfficePosition(
        String accountId,
        String symbol,
        long netQty,
        double avgPrice
    ) {
    }

    public record PositionsResponse(List<BackOfficePosition> positions) {
    }

    public record ReplacePositionsRequest(String accountId, List<BackOfficePosition> positions) {
    }

    public record FillsResponse(List<FillView> fills) {
    }

    public record ReplaceFillsRequest(String orderId, List<FillView> fills) {
    }

    public record BackOfficeOrderState(
        String orderId,
        String accountId,
        String symbol,
        String side,
        long quantity,
        long workingPrice,
        long submittedAt,
        long lastEventAt,
        String status,
        long filledQuantity,
        long reservedAmount
    ) {
    }

    public record ReplaceLedgerRequest(List<LedgerEntry> entries) {
    }

    public record ExecutionPackage(
        long generatedAt,
        String orderId,
        String accountId,
        String symbol,
        String symbolName,
        String clientIntent,
        List<ExecutionStyle> executionStyles,
        ParentOrderPlan parentOrderPlan,
        List<ChildOrderSlice> childOrders,
        AllocationPlan allocationPlan,
        List<String> operatorChecks
    ) {
    }

    public record ExecutionStyle(
        String name,
        String useCase,
        String businessRule,
        String systemImplication,
        List<String> tradeoffs
    ) {
    }

    public record ParentOrderPlan(
        String side,
        long totalQuantity,
        double arrivalMidPrice,
        double targetParticipationPercent,
        int scheduleWindowMinutes,
        String chosenStyle,
        List<String> whyNotOtherChoices
    ) {
    }

    public record ChildOrderSlice(
        String id,
        String venueIntent,
        long plannedQuantity,
        double benchmarkPrice,
        double expectedFillPrice,
        double expectedSlippageBps,
        String timeBucketLabel,
        String learningPoint
    ) {
    }

    public record AllocationPlan(
        String blockBook,
        double averagePrice,
        long totalQuantity,
        List<AllocationSlice> allocations,
        String settlementNote,
        List<String> controlChecks
    ) {
    }

    public record AllocationSlice(
        String targetBook,
        long quantity,
        double ratioPercent,
        String note
    ) {
    }

    public record PostTradePackage(
        long generatedAt,
        String orderId,
        String accountId,
        String symbol,
        String symbolName,
        String orderStatus,
        List<PostTradeStage> stages,
        FeeBreakdown feeBreakdown,
        StatementPreview statementPreview,
        List<SettlementCheck> settlementChecks,
        List<CorporateActionHook> corporateActionHooks
    ) {
    }

    public record PostTradeStage(
        String name,
        String owner,
        String purpose,
        String currentView,
        String whyItMatters
    ) {
    }

    public record FeeBreakdown(
        long grossNotional,
        long commission,
        long exchangeFee,
        long taxes,
        long netCashMovement,
        List<String> assumptions
    ) {
    }

    public record StatementPreview(
        String accountId,
        String symbol,
        String symbolName,
        long settledQuantity,
        double averagePrice,
        String settlementDateLabel,
        String netCashMovementLabel,
        List<String> notes
    ) {
    }

    public record SettlementCheck(
        String title,
        String rule,
        String currentValue
    ) {
    }

    public record CorporateActionHook(
        String name,
        String businessImpact,
        String systemImpact
    ) {
    }

    public record ParentExecutionState(
        long generatedAt,
        String orderId,
        String accountId,
        String symbol,
        String parentStatus,
        String executionStyle,
        long targetQuantity,
        long executedQuantity,
        long remainingQuantity,
        double participationTargetPercent,
        double participationActualPercent,
        String scheduleWindowLabel,
        List<ChildExecutionState> childStates,
        List<String> operatorAlerts
    ) {
    }

    public record ChildExecutionState(
        String childId,
        String state,
        String venueIntent,
        long plannedQuantity,
        long executedQuantity,
        long remainingQuantity,
        double benchmarkPrice,
        double averageFillPrice,
        double slippageBps,
        String nextAction
    ) {
    }

    public record AllocationState(
        long generatedAt,
        String orderId,
        String accountId,
        String symbol,
        String allocationStatus,
        long allocatedQuantity,
        long pendingQuantity,
        double allocationAveragePrice,
        List<BookAllocationState> books,
        List<String> controls
    ) {
    }

    public record BookAllocationState(
        String book,
        long targetQuantity,
        long allocatedQuantity,
        String status,
        String rationale
    ) {
    }

    public record SettlementProjection(
        long generatedAt,
        String orderId,
        String accountId,
        String symbol,
        String settlementStatus,
        String tradeDateLabel,
        String settlementDateLabel,
        long grossNotional,
        long netCashMovement,
        long settledQuantity,
        String cashLegStatus,
        String securitiesLegStatus,
        List<String> exceptionFlags,
        String nextAction
    ) {
    }

    public record StatementProjection(
        long generatedAt,
        String orderId,
        String accountId,
        String symbol,
        String statementStatus,
        String confirmReference,
        String statementReference,
        String customerFacingSummary,
        List<StatementLine> lines,
        List<String> controls
    ) {
    }

    public record StatementLine(
        String label,
        String value,
        String note
    ) {
    }

    public record RiskSnapshot(
        long generatedAt,
        String accountId,
        double marketValue,
        double cashBalance,
        List<RiskConcentrationMetric> concentration,
        List<RiskLiquidityMetric> liquidity,
        List<RiskScenarioLibraryEntry> scenarioLibrary,
        RiskBacktestingPreview backtesting,
        List<RiskModelBoundary> modelBoundaries,
        List<String> marginAlerts
    ) {
    }

    public record RiskConcentrationMetric(
        String symbol,
        String symbolName,
        double exposure,
        double weightPercent,
        String note
    ) {
    }

    public record RiskLiquidityMetric(
        String symbol,
        String symbolName,
        long positionQuantity,
        long visibleTopOfBookQuantity,
        double participationPercent,
        double estimatedDaysToExit,
        String note
    ) {
    }

    public record RiskScenarioLibraryEntry(
        String id,
        String title,
        String category,
        String shock,
        String rationale,
        String focus
    ) {
    }

    public record RiskBacktestingPreview(
        int observationCount,
        double breachRatePercent,
        double averageTailLoss,
        String note,
        List<RiskBacktestSample> samples
    ) {
    }

    public record RiskBacktestSample(
        String label,
        double pnl,
        boolean breached
    ) {
    }

    public record RiskModelBoundary(
        String title,
        String whyItMatters,
        String whatIncluded,
        String whatExcluded
    ) {
    }

    public record SettlementExceptionWorkflow(
        long generatedAt,
        String orderId,
        String accountId,
        String symbol,
        String workflowStatus,
        String exceptionType,
        String blockedStage,
        String ageingLabel,
        String rootCause,
        String nextAction,
        List<String> controls,
        List<String> operatorNotes
    ) {
    }

    public record CorporateActionWorkflow(
        long generatedAt,
        String orderId,
        String accountId,
        String symbol,
        String eventName,
        String workflowStatus,
        String recordDateLabel,
        String effectiveDateLabel,
        String customerImpact,
        String ledgerImpact,
        String nextAction,
        List<String> controls
    ) {
    }

    public record MarginProjection(
        long generatedAt,
        String accountId,
        String methodology,
        double marginLimit,
        double marginUsed,
        double utilizationPercent,
        String breachStatus,
        List<String> breachedLimits,
        List<String> requiredActions,
        List<String> modelNotes
    ) {
    }

    public record ScenarioEvaluationHistory(
        long generatedAt,
        String accountId,
        String lastEvaluatedAtLabel,
        List<ScenarioEvaluationEntry> evaluations
    ) {
    }

    public record ScenarioEvaluationEntry(
        String title,
        String shock,
        double pnlDelta,
        String note
    ) {
    }

    public record BacktestHistory(
        long generatedAt,
        String accountId,
        String windowLabel,
        double breachRatePercent,
        List<BacktestHistoryPoint> history
    ) {
    }

    public record BacktestHistoryPoint(
        String label,
        double pnl,
        boolean breached,
        String note
    ) {
    }

    public record BackOfficeStats(
        boolean enabled,
        String state,
        String auditPath,
        String offsetPath,
        String startMode,
        String startedAt,
        long processed,
        long skipped,
        long duplicates,
        long orphans,
        long sequenceGaps,
        long replays,
        Long lastEventAt,
        long currentOffset,
        long currentAuditSize,
        int ledgerEntryCount,
        int deadLetterCount,
        int pendingOrphanCount,
        int aggregateProgressCount
    ) {
    }

    public record BackOfficeReconcile(
        String accountId,
        long cashBalance,
        long availableBuyingPower,
        long reservedBuyingPower,
        long realizedPnl,
        long expectedCashBalance,
        long expectedReservedBuyingPower,
        long expectedRealizedPnl,
        List<BackOfficePosition> positions,
        List<String> issues
    ) {
    }

    public record BackOfficeBusStats(
        boolean enabled,
        boolean kafkaEnabled,
        String state,
        String topic,
        String groupId,
        String startedAt,
        long received,
        long applied,
        long duplicates,
        long pending,
        long deadLetters,
        long errors,
        Long lastEventAt
    ) {
    }

    public record LedgerEntry(
        String entryId,
        String eventRef,
        String accountId,
        String orderId,
        String eventType,
        String symbol,
        String side,
        long quantityDelta,
        long cashDelta,
        long reservedBuyingPowerDelta,
        long realizedPnlDelta,
        String detail,
        long eventAt,
        String source
    ) {
    }

    public record LedgerResponse(String accountId, List<LedgerEntry> entries) {
    }

    public record ReplayRequest(boolean resetState) {
    }

    public record ReplayResult(
        String status,
        long offset,
        long processed,
        long skipped,
        long duplicates,
        long orphans
    ) {
    }

    public record DeadLetterEntry(
        String entryId,
        String eventRef,
        String accountId,
        String orderId,
        String eventType,
        String reason,
        String detail,
        String rawLine,
        long eventAt,
        long recordedAt,
        String source
    ) {
    }

    public record PendingOrphanEntry(
        String entryId,
        String eventRef,
        String accountId,
        String orderId,
        String eventType,
        String reason,
        String rawLine,
        long eventAt,
        long recordedAt,
        String source
    ) {
    }

    public record RequeueRequest(String orderId) {
    }

    public record RequeueResult(
        String status,
        String orderId,
        int reprocessed,
        int pendingRemaining
    ) {
    }

    public record DeadLetterRequeueRequest(String eventRef) {
    }

    public record DeadLetterRequeueResult(
        String status,
        String eventRef,
        String outcome,
        int pendingRemaining,
        int deadLetterRemaining
    ) {
    }
}
