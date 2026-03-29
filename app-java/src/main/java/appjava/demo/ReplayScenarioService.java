package appjava.demo;

import appjava.account.AccountOverview;
import appjava.clients.BackOfficeClient;
import appjava.clients.BackOfficeClient.BackOfficeOrderState;
import appjava.clients.BackOfficeClient.BackOfficePosition;
import appjava.clients.BackOfficeClient.ExecutionPackage;
import appjava.clients.BackOfficeClient.ExecutionStyle;
import appjava.clients.BackOfficeClient.LedgerEntry;
import appjava.clients.BackOfficeClient.ParentOrderPlan;
import appjava.clients.BackOfficeClient.PostTradePackage;
import appjava.clients.OmsClient;
import appjava.http.JsonHttpHandler;
import appjava.market.MarketDataService;
import appjava.market.MarketStructureSnapshot;
import appjava.order.ExecutionBenchmark;
import appjava.order.ExecutionBenchmarkStore;
import appjava.order.FillView;
import appjava.order.OrderEventView;
import appjava.order.OrderRequest;
import appjava.order.ReservationView;
import appjava.order.OrderStatus;
import appjava.order.OrderView;
import appjava.clients.BackOfficeClient.AllocationPlan;
import appjava.clients.BackOfficeClient.AllocationSlice;
import appjava.clients.BackOfficeClient.AllocationState;
import appjava.clients.BackOfficeClient.BookAllocationState;
import appjava.clients.BackOfficeClient.ChildOrderSlice;
import appjava.clients.BackOfficeClient.ChildExecutionState;
import appjava.clients.BackOfficeClient.CorporateActionHook;
import appjava.clients.BackOfficeClient.FeeBreakdown;
import appjava.clients.BackOfficeClient.ParentExecutionState;
import appjava.clients.BackOfficeClient.PostTradeStage;
import appjava.clients.BackOfficeClient.RiskBacktestSample;
import appjava.clients.BackOfficeClient.RiskBacktestingPreview;
import appjava.clients.BackOfficeClient.RiskConcentrationMetric;
import appjava.clients.BackOfficeClient.RiskLiquidityMetric;
import appjava.clients.BackOfficeClient.RiskModelBoundary;
import appjava.clients.BackOfficeClient.RiskScenarioLibraryEntry;
import appjava.clients.BackOfficeClient.RiskSnapshot;
import appjava.clients.BackOfficeClient.SettlementExceptionWorkflow;
import appjava.clients.BackOfficeClient.SettlementProjection;
import appjava.clients.BackOfficeClient.SettlementCheck;
import appjava.clients.BackOfficeClient.StatementLine;
import appjava.clients.BackOfficeClient.StatementProjection;
import appjava.clients.BackOfficeClient.StatementPreview;
import appjava.clients.BackOfficeClient.CorporateActionWorkflow;
import appjava.clients.BackOfficeClient.MarginProjection;
import appjava.clients.BackOfficeClient.ScenarioEvaluationHistory;
import appjava.clients.BackOfficeClient.ScenarioEvaluationEntry;
import appjava.clients.BackOfficeClient.BacktestHistory;
import appjava.clients.BackOfficeClient.BacktestHistoryPoint;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

public final class ReplayScenarioService {
    private static final long INITIAL_CASH = 10_000_000L;

    private final String accountId;
    private final MarketDataService marketDataService;
    private final BackOfficeClient backOfficeClient;
    private final OmsClient omsClient;
    private final ExecutionBenchmarkStore executionBenchmarkStore;

    public ReplayScenarioService(
        String accountId,
        MarketDataService marketDataService,
        BackOfficeClient backOfficeClient,
        OmsClient omsClient,
        ExecutionBenchmarkStore executionBenchmarkStore
    ) {
        this.accountId = accountId;
        this.marketDataService = marketDataService;
        this.backOfficeClient = backOfficeClient;
        this.omsClient = omsClient;
        this.executionBenchmarkStore = executionBenchmarkStore;
    }

    public OrderView runScenario(String scenarioName, OrderRequest request) {
        validate(request);
        String scenario = normalizeScenario(scenarioName);
        marketDataService.reset();
        omsClient.reset();
        backOfficeClient.resetState();

        long now = Instant.now().toEpochMilli();
        long submittedAt = now - 4_000L;
        String symbol = request.symbol();
        String side = request.side().toUpperCase(Locale.ROOT);
        String type = request.type().toUpperCase(Locale.ROOT);
        int quantity = request.quantity();
        double workingPrice = request.price() != null ? request.price() : marketDataService.getCurrentPrice(symbol);
        MarketStructureSnapshot marketStructure = marketDataService.getMarketStructure(symbol);

        ScenarioSnapshot snapshot = switch (scenario) {
            case "accepted" -> acceptedSnapshot(request, submittedAt, workingPrice);
            case "partial-fill" -> partialFillSnapshot(request, submittedAt, workingPrice);
            case "filled" -> filledSnapshot(request, submittedAt, workingPrice);
            case "canceled" -> canceledSnapshot(request, submittedAt, workingPrice);
            case "expired" -> expiredSnapshot(request, submittedAt, workingPrice);
            case "rejected" -> rejectedSnapshot(request, submittedAt, workingPrice);
            default -> throw new JsonHttpHandler.NotFoundException("scenario_not_found:" + scenario);
        };

        OrderView order = new OrderView(
            "demo-" + scenario + "-" + UUID.randomUUID(),
            accountId,
            symbol,
            side,
            type,
            quantity,
            request.price(),
            snapshot.timeInForce(),
            snapshot.expireAt(),
            snapshot.status(),
            submittedAt,
            snapshot.statusEventAt(),
            snapshot.executionTimeMs(),
            snapshot.statusReason(),
            snapshot.filledQuantity(),
            snapshot.remainingQuantity()
        );

        AccountOverview overview = new AccountOverview(
            accountId,
            snapshot.cashBalance(),
            snapshot.availableBuyingPower(),
            snapshot.reservedBuyingPower(),
            snapshot.positions().size(),
            snapshot.realizedPnl(),
            Instant.ofEpochMilli(snapshot.statusEventAt() != null ? snapshot.statusEventAt() : submittedAt).toString()
        );

        List<OrderEventView> events = buildEvents(order, snapshot);
        List<ReservationView> reservations = buildReservations(order, snapshot, workingPrice);
        List<FillView> fills = buildFills(order, snapshot, workingPrice);
        BackOfficeOrderState orderState = buildOrderState(order, snapshot, workingPrice);
        List<LedgerEntry> ledgerEntries = buildLedgerEntries(order, snapshot);
        ExecutionPackage executionPackage = buildExecutionPackage(order, snapshot, marketStructure, workingPrice);
        ParentExecutionState parentExecutionState = buildParentExecutionState(order, snapshot, executionPackage);
        AllocationState allocationState = buildAllocationState(order, snapshot, executionPackage);
        PostTradePackage postTradePackage = buildPostTradePackage(order, snapshot, fills, marketStructure);
        SettlementProjection settlementProjection = buildSettlementProjection(order, snapshot, postTradePackage);
        StatementProjection statementProjection = buildStatementProjection(order, snapshot, postTradePackage);
        RiskSnapshot riskSnapshot = buildRiskSnapshot(snapshot);
        SettlementExceptionWorkflow settlementExceptionWorkflow = buildSettlementExceptionWorkflow(order, snapshot);
        CorporateActionWorkflow corporateActionWorkflow = buildCorporateActionWorkflow(order, snapshot);
        MarginProjection marginProjection = buildMarginProjection(snapshot, riskSnapshot);
        ScenarioEvaluationHistory scenarioEvaluationHistory = buildScenarioEvaluationHistory(snapshot);
        BacktestHistory backtestHistory = buildBacktestHistory(riskSnapshot);
        executionBenchmarkStore.put(new ExecutionBenchmark(
            order.id(),
            symbol,
            submittedAt,
            marketStructure.lastPrice(),
            marketStructure.bidPrice(),
            marketStructure.askPrice(),
            marketStructure.midPrice(),
            marketStructure.spreadBps(),
            marketStructure.venueState(),
            "arrival-mid"
        ));

        omsClient.upsertOrder(order);
        omsClient.replaceOrderEvents(order.id(), events);
        omsClient.replaceReservations(accountId, reservations);
        backOfficeClient.upsertOverview(overview);
        backOfficeClient.upsertOrderState(orderState);
        backOfficeClient.replacePositions(accountId, snapshot.positions());
        backOfficeClient.replaceFills(order.id(), fills);
        backOfficeClient.replaceLedger(ledgerEntries);
        backOfficeClient.upsertExecutionPackage(executionPackage);
        backOfficeClient.upsertParentExecutionState(parentExecutionState);
        backOfficeClient.upsertAllocationState(allocationState);
        backOfficeClient.upsertPostTradePackage(postTradePackage);
        backOfficeClient.upsertSettlementProjection(settlementProjection);
        backOfficeClient.upsertStatementProjection(statementProjection);
        backOfficeClient.upsertRiskSnapshot(riskSnapshot);
        backOfficeClient.upsertSettlementExceptionWorkflow(settlementExceptionWorkflow);
        backOfficeClient.upsertCorporateActionWorkflow(corporateActionWorkflow);
        backOfficeClient.upsertMarginProjection(marginProjection);
        backOfficeClient.upsertScenarioEvaluationHistory(scenarioEvaluationHistory);
        backOfficeClient.upsertBacktestHistory(backtestHistory);
        return omsClient.fetchOrder(order.id()).orElse(order);
    }

    private ScenarioSnapshot acceptedSnapshot(OrderRequest request, long submittedAt, double workingPrice) {
        long reserved = request.side().equalsIgnoreCase("BUY") ? Math.round(workingPrice * request.quantity()) : 0L;
        return new ScenarioSnapshot(
            OrderStatus.ACCEPTED,
            "VENUE_ACCEPTED",
            0L,
            request.quantity(),
            submittedAt + 250L,
            0.82,
            INITIAL_CASH,
            Math.max(0L, INITIAL_CASH - reserved),
            reserved,
            0L,
            List.of(),
            0L,
            reserved > 0L ? request.quantity() : 0L,
            reserved,
            reserved > 0L ? "ACTIVE" : "NONE",
            request.timeInForce(),
            request.expireAt()
        );
    }

    private ScenarioSnapshot partialFillSnapshot(OrderRequest request, long submittedAt, double workingPrice) {
        if (request.quantity() < 2) {
            throw new JsonHttpHandler.ValidationException("PARTIAL_FILL_REQUIRES_QTY_GTE_2");
        }
        long filledQuantity = Math.max(1L, request.quantity() / 2L);
        long remainingQuantity = request.quantity() - filledQuantity;
        long filledNotional = Math.round(workingPrice * filledQuantity);
        long reserved = request.side().equalsIgnoreCase("BUY") ? Math.round(workingPrice * remainingQuantity) : 0L;
        long cashBalance = INITIAL_CASH + signedCashDelta(request.side(), filledNotional);
        return new ScenarioSnapshot(
            OrderStatus.PARTIALLY_FILLED,
            "PARTIAL_FILL " + filledQuantity + "/" + request.quantity(),
            filledQuantity,
            remainingQuantity,
            submittedAt + 2_100L,
            1.46,
            cashBalance,
            Math.max(0L, cashBalance - reserved),
            reserved,
            0L,
            filledQuantity == 0L ? List.of() : List.of(newPosition(request, filledQuantity, workingPrice)),
            filledNotional,
            reserved > 0L ? request.quantity() : 0L,
            request.side().equalsIgnoreCase("BUY") ? Math.round(workingPrice * request.quantity()) : 0L,
            reserved > 0L ? "ACTIVE" : "NONE",
            request.timeInForce(),
            request.expireAt()
        );
    }

    private ScenarioSnapshot filledSnapshot(OrderRequest request, long submittedAt, double workingPrice) {
        long filledNotional = Math.round(workingPrice * request.quantity());
        long cashBalance = INITIAL_CASH + signedCashDelta(request.side(), filledNotional);
        return new ScenarioSnapshot(
            OrderStatus.FILLED,
            "FULL_FILL " + request.quantity() + "/" + request.quantity(),
            request.quantity(),
            0L,
            submittedAt + 1_650L,
            1.12,
            cashBalance,
            cashBalance,
            0L,
            0L,
            List.of(newPosition(request, request.quantity(), workingPrice)),
            filledNotional,
            request.side().equalsIgnoreCase("BUY") ? request.quantity() : 0L,
            request.side().equalsIgnoreCase("BUY") ? filledNotional : 0L,
            request.side().equalsIgnoreCase("BUY") ? "CONSUMED" : "NONE",
            request.timeInForce(),
            request.expireAt()
        );
    }

    private ScenarioSnapshot canceledSnapshot(OrderRequest request, long submittedAt, double workingPrice) {
        long originalReservedAmount = request.side().equalsIgnoreCase("BUY") ? Math.round(workingPrice * request.quantity()) : 0L;
        return new ScenarioSnapshot(
            OrderStatus.CANCELED,
            "USER_CANCELLED",
            0L,
            request.quantity(),
            submittedAt + 2_400L,
            0.94,
            INITIAL_CASH,
            INITIAL_CASH,
            0L,
            0L,
            List.of(),
            0L,
            originalReservedAmount > 0L ? request.quantity() : 0L,
            originalReservedAmount,
            originalReservedAmount > 0L ? "RELEASED" : "NONE",
            request.timeInForce(),
            request.expireAt()
        );
    }

    private ScenarioSnapshot expiredSnapshot(OrderRequest request, long submittedAt, double workingPrice) {
        long expireAt = request.expireAt() != null ? request.expireAt() : submittedAt + 2_000L;
        long statusAt = Math.max(submittedAt + 2_200L, expireAt);
        long originalReservedAmount = request.side().equalsIgnoreCase("BUY") ? Math.round(workingPrice * request.quantity()) : 0L;
        return new ScenarioSnapshot(
            OrderStatus.EXPIRED,
            "TIME_IN_FORCE_EXPIRED",
            0L,
            request.quantity(),
            statusAt,
            0.88,
            INITIAL_CASH,
            INITIAL_CASH,
            0L,
            0L,
            List.of(),
            0L,
            originalReservedAmount > 0L ? request.quantity() : 0L,
            originalReservedAmount,
            originalReservedAmount > 0L ? "RELEASED" : "NONE",
            "GTD",
            expireAt
        );
    }

    private ScenarioSnapshot rejectedSnapshot(OrderRequest request, long submittedAt, double workingPrice) {
        return new ScenarioSnapshot(
            OrderStatus.REJECTED,
            "RISK_LIMIT_EXCEEDED",
            0L,
            request.quantity(),
            submittedAt + 180L,
            0.31,
            INITIAL_CASH,
            INITIAL_CASH,
            0L,
            0L,
            List.of(),
            0L,
            0L,
            0L,
            "NONE",
            request.timeInForce(),
            request.expireAt()
        );
    }

    private BackOfficePosition newPosition(OrderRequest request, long filledQuantity, double avgPrice) {
        long signedQuantity = request.side().equalsIgnoreCase("BUY") ? filledQuantity : -filledQuantity;
        return new BackOfficePosition(accountId, request.symbol(), signedQuantity, round2(avgPrice));
    }

    private List<OrderEventView> buildEvents(OrderView order, ScenarioSnapshot snapshot) {
        long submittedAt = order.submittedAt();
        long acceptedAt = submittedAt + 250L;
        long statusAt = snapshot.statusEventAt() != null ? snapshot.statusEventAt() : submittedAt + 1_000L;
        java.util.ArrayList<OrderEventView> events = new java.util.ArrayList<>();
        events.add(event(order.id(), "ORDER_SUBMITTED", submittedAt, "注文受付", order.symbol() + " " + order.side() + " " + order.quantity() + "株", 1));
        switch (snapshot.status()) {
            case ACCEPTED -> {
                events.add(event(order.id(), "ORDER_ACCEPTED", acceptedAt, "OMS受付済", "取引所受付済", 2));
                if (snapshot.reservedBuyingPower() > 0L) {
                    events.add(event(order.id(), "RESERVATION_CREATED", acceptedAt + 50L, "余力拘束", "拘束余力 ¥" + snapshot.reservedBuyingPower(), 3));
                }
            }
            case PARTIALLY_FILLED -> {
                events.add(event(order.id(), "ORDER_ACCEPTED", acceptedAt, "OMS受付済", "取引所受付済", 2));
                if (snapshot.reservedBuyingPower() + snapshot.fillNotional() > 0L) {
                    events.add(event(order.id(), "RESERVATION_CREATED", acceptedAt + 50L, "余力拘束", "拘束余力 ¥" + (snapshot.reservedBuyingPower() + snapshot.fillNotional()), 3));
                }
                events.add(event(order.id(), "PARTIAL_FILL", statusAt, "一部約定", snapshot.filledQuantity() + "株約定 / 残" + snapshot.remainingQuantity() + "株", 4));
                if (snapshot.reservedBuyingPower() > 0L) {
                    events.add(event(order.id(), "RESERVATION_REDUCED", statusAt + 50L, "拘束縮小", "残拘束 ¥" + snapshot.reservedBuyingPower(), 5));
                }
            }
            case FILLED -> {
                events.add(event(order.id(), "ORDER_ACCEPTED", acceptedAt, "OMS受付済", "取引所受付済", 2));
                if (snapshot.fillNotional() > 0L) {
                    events.add(event(order.id(), "RESERVATION_CREATED", acceptedAt + 50L, "余力拘束", "拘束余力 ¥" + snapshot.fillNotional(), 3));
                }
                events.add(event(order.id(), "FULL_FILL", statusAt, "全量約定", order.quantity() + "株が約定", 4));
                events.add(event(order.id(), "RESERVATION_CONSUMED", statusAt + 50L, "拘束消化", "約定代金に振替", 5));
            }
            case CANCELED -> {
                events.add(event(order.id(), "ORDER_ACCEPTED", acceptedAt, "OMS受付済", "取引所受付済", 2));
                if (snapshot.wasReserved()) {
                    events.add(event(order.id(), "RESERVATION_CREATED", acceptedAt + 50L, "余力拘束", "拘束余力 ¥" + snapshot.originalReservedAmount(), 3));
                }
                events.add(event(order.id(), "CANCEL_REQUESTED", statusAt - 600L, "取消送信", "取消要求を取引所へ送信", 4));
                events.add(event(order.id(), "ORDER_CANCELED", statusAt, "取消完了", snapshot.statusReason(), 5));
                if (snapshot.wasReserved()) {
                    events.add(event(order.id(), "RESERVATION_RELEASED", statusAt + 50L, "拘束解放", "拘束余力を解放", 6));
                }
            }
            case EXPIRED -> {
                events.add(event(order.id(), "ORDER_ACCEPTED", acceptedAt, "OMS受付済", "取引所受付済", 2));
                if (snapshot.wasReserved()) {
                    events.add(event(order.id(), "RESERVATION_CREATED", acceptedAt + 50L, "余力拘束", "拘束余力 ¥" + snapshot.originalReservedAmount(), 3));
                }
                events.add(event(order.id(), "ORDER_EXPIRED", statusAt, "失効", snapshot.statusReason(), 4));
                if (snapshot.wasReserved()) {
                    events.add(event(order.id(), "RESERVATION_RELEASED", statusAt + 50L, "拘束解放", "期限切れに伴い解放", 5));
                }
            }
            case REJECTED -> events.add(event(order.id(), "ORDER_REJECTED", statusAt, "注文拒否", snapshot.statusReason(), 2));
            case PENDING_ACCEPT, CANCEL_PENDING, AMEND_PENDING -> {
                events.add(event(order.id(), snapshot.status().name(), statusAt, snapshot.status().name(), snapshot.statusReason(), 2));
            }
        }
        return List.copyOf(events);
    }

    private List<ReservationView> buildReservations(OrderView order, ScenarioSnapshot snapshot, double workingPrice) {
        if (!snapshot.wasReserved()) {
            return List.of();
        }
        long openedAt = order.submittedAt() + 300L;
        return List.of(new ReservationView(
            "resv-" + order.id(),
            accountId,
            order.id(),
            order.symbol(),
            order.side(),
            snapshot.originalReservedQuantity(),
            snapshot.originalReservedAmount(),
            snapshot.originalReservedAmount() - snapshot.reservedBuyingPower(),
            snapshot.reservationStatus(),
            openedAt,
            snapshot.statusEventAt() != null ? snapshot.statusEventAt() : openedAt
        ));
    }

    private List<FillView> buildFills(OrderView order, ScenarioSnapshot snapshot, double workingPrice) {
        if (snapshot.filledQuantity() <= 0L) {
            return List.of();
        }
        return List.of(new FillView(
            "fill-" + order.id(),
            order.id(),
            accountId,
            order.symbol(),
            order.side(),
            snapshot.filledQuantity(),
            round2(workingPrice),
            snapshot.fillNotional(),
            snapshot.status() == OrderStatus.FILLED ? "TAKER" : "MAKER",
            snapshot.statusEventAt() != null ? snapshot.statusEventAt() : order.submittedAt() + 1_000L
        ));
    }

    private BackOfficeOrderState buildOrderState(OrderView order, ScenarioSnapshot snapshot, double workingPrice) {
        long lastEventAt = snapshot.statusEventAt() != null ? snapshot.statusEventAt() : order.submittedAt();
        return new BackOfficeOrderState(
            order.id(),
            accountId,
            order.symbol(),
            order.side(),
            order.quantity(),
            Math.round(workingPrice),
            order.submittedAt(),
            lastEventAt,
            order.status().name(),
            snapshot.filledQuantity(),
            snapshot.reservedBuyingPower()
        );
    }

    private ExecutionPackage buildExecutionPackage(
        OrderView order,
        ScenarioSnapshot snapshot,
        MarketStructureSnapshot marketStructure,
        double workingPrice
    ) {
        long parentQuantity = Math.max(order.quantity(), 100L);
        long sliceOne = Math.max(1L, Math.round(parentQuantity * 0.25));
        long sliceTwo = Math.max(1L, Math.round(parentQuantity * 0.35));
        long sliceThree = Math.max(1L, parentQuantity - sliceOne - sliceTwo);
        double arrivalMid = round2(marketStructure.midPrice());
        double targetParticipation = round2(
            Math.min(18.0, Math.max(4.0, ((double) parentQuantity / Math.max(1L, marketStructure.bidQuantity() + marketStructure.askQuantity())) * 100.0))
        );
        String symbolName = marketDataService.getStockInfo(order.symbol()).name();
        double takerPrice = round2(Math.max(workingPrice, marketStructure.askPrice()));
        double patientPrice = round2(Math.max(arrivalMid, marketStructure.bidPrice()));
        double closePrice = round2(arrivalMid + Math.max(0.5, marketStructure.spread() * 0.7));

        return new ExecutionPackage(
            snapshot.statusEventAt() != null ? snapshot.statusEventAt() : order.submittedAt(),
            order.id(),
            accountId,
            order.symbol(),
            symbolName,
            order.side().equalsIgnoreCase("BUY")
                ? symbolName + " を価格を飛ばしすぎず集め、説明可能な execution quality を残したい。"
                : symbolName + " を崩しすぎず売却し、close 前に inventory を閉じたい。",
            List.of(
                new ExecutionStyle(
                    "DMA",
                    "価格を細かく調整しながら queue priority を取りたい場面",
                    "最良気配の内側に無理に飛び込まず、cancel-replace の理由を残す",
                    "clientOrderId / venueOrderId / amend history の追跡が必須",
                    List.of("柔軟だが operator の判断負荷が高い", "相場急変時は説明責任が個人に寄りやすい")
                ),
                new ExecutionStyle(
                    "Care Order",
                    "顧客説明と価格保護を両立したい場面",
                    "板の外にある流動性も見ながら aggressiveness を調整する",
                    "親注文と child slice の状態を分け、判断理由を operator に残す",
                    List.of("丁寧だが人手依存が増える", "状態遷移が雑だと説明が破綻する")
                ),
                new ExecutionStyle(
                    "POV",
                    "出来高参加率を守りながら footprint を抑えたい場面",
                    "market volume の変化に応じて child slice のサイズを動かす",
                    "arrival benchmark と participation limit を親注文単位で固定する",
                    List.of("未約定残が残りやすい", "平時は benchmark 説明がしやすい")
                )
            ),
            new ParentOrderPlan(
                order.side(),
                parentQuantity,
                arrivalMid,
                targetParticipation,
                "EXPIRED".equals(order.status().name()) ? 30 : 45,
                snapshot.status() == OrderStatus.REJECTED ? "Care Order" : "POV",
                List.of(
                    "一撃の成行で spread を取りに行かず、arrival benchmark を残す",
                    "operator 裁量だけに寄せず、child order の根拠を時間帯と市場状態に結びつける",
                    order.quantity() < 500 ? "demo size のため quantity は小さいが、判断軸は institutional flow と同じに置く" : "親注文を child order に分けて footprint を制御する"
                )
            ),
            List.of(
                new ChildOrderSlice("slice-01", "opening-queue", sliceOne, arrivalMid, patientPrice, slippageBps(arrivalMid, patientPrice), "前場寄り", "queue を取りに行く初手。板を飛ばさない"),
                new ChildOrderSlice("slice-02", "volume-follow", sliceTwo, arrivalMid, round2((patientPrice + takerPrice) / 2.0), slippageBps(arrivalMid, round2((patientPrice + takerPrice) / 2.0)), "前場中盤", "出来高を見ながら participation を守る"),
                new ChildOrderSlice("slice-03", "close-control", sliceThree, arrivalMid, closePrice, slippageBps(arrivalMid, closePrice), "大引け前", snapshot.remainingQuantity() > 0L ? "未約定残を持ちすぎず、close までに説明可能な形で片付ける" : "残数量を持ち越さず平均価格で block allocation を閉じる")
            ),
            new AllocationPlan(
                "block-equities-book",
                snapshot.filledQuantity() > 0L ? round2((double) snapshot.fillNotional() / snapshot.filledQuantity()) : arrivalMid,
                snapshot.filledQuantity(),
                snapshot.filledQuantity() <= 0L
                    ? List.of()
                    : List.of(
                        new AllocationSlice("long-only-japan", Math.round(snapshot.filledQuantity() * 0.50), 50.0, "benchmark 追随を優先"),
                        new AllocationSlice("event-driven", Math.round(snapshot.filledQuantity() * 0.30), 30.0, "材料イベント前に inventory を確保"),
                        new AllocationSlice(
                            "multi-strat",
                            snapshot.filledQuantity() - Math.round(snapshot.filledQuantity() * 0.50) - Math.round(snapshot.filledQuantity() * 0.30),
                            20.0,
                            "残数量を book balance に応じて配賦"
                        )
                    ),
                "allocation truth は parent fill 総数に置き、child slice 単価ではなく平均約定単価で book 配賦する。",
                List.of(
                    "親注文 working 中に allocation を確定しない",
                    "book 配賦後の数量合計を parent fill 総数と常に一致させる",
                    snapshot.status() == OrderStatus.PARTIALLY_FILLED ? "partial fill の間は残数量と allocation 済数量を混同しない" : "fill 完了後に block average を fix する"
                )
            ),
            List.of(
                "arrival benchmark を親注文単位で固定しているか",
                "child slice の aggressiveness を市場状態と結びつけて説明できるか",
                snapshot.status() == OrderStatus.REJECTED
                    ? "reject reason と再発注方針を operator log に残しているか"
                    : "allocation の総和と fill 総数が一致しているか"
            )
        );
    }

    private PostTradePackage buildPostTradePackage(
        OrderView order,
        ScenarioSnapshot snapshot,
        List<FillView> fills,
        MarketStructureSnapshot marketStructure
    ) {
        long grossNotional = snapshot.fillNotional();
        long commission = grossNotional <= 0L ? 0L : Math.max(120L, Math.round(grossNotional * 0.0003));
        long exchangeFee = grossNotional <= 0L ? 0L : Math.max(40L, Math.round(grossNotional * 0.00005));
        long taxes = order.side().equalsIgnoreCase("SELL") && grossNotional > 0L ? Math.max(0L, Math.round(grossNotional * 0.00002)) : 0L;
        long netCashMovement = signedCashDelta(order.side(), grossNotional) - commission - exchangeFee - taxes;
        long settledQuantity = snapshot.filledQuantity();
        double averagePrice = settledQuantity <= 0L ? 0.0 : round2((double) grossNotional / settledQuantity);
        long stageTime = snapshot.statusEventAt() != null ? snapshot.statusEventAt() : order.submittedAt();
        String symbolName = marketDataService.getStockInfo(order.symbol()).name();

        return new PostTradePackage(
            stageTime,
            order.id(),
            accountId,
            order.symbol(),
            symbolName,
            order.status().name(),
            List.of(
                new PostTradeStage("Execution", "front office", "約定数量と平均単価を固める", fills.isEmpty() ? "未約定" : fills.size() + " fill / 平均 " + averagePrice, fills.isEmpty() ? "fill が無ければ post-trade は始まらない" : "child fill を parent fill に束ねる入口"),
                new PostTradeStage("Allocation", "middle office", "block fill を各 book に配賦する", settledQuantity <= 0L ? "未配賦" : settledQuantity + "株を平均単価で配賦", "book ごとの cash/position を child slice 単位ではなく block 単位で説明する"),
                new PostTradeStage("Confirmation", "operations", "顧客向け確認情報を確定する", order.status().name() + " / T+2 予定", "約定したのか、まだ working なのかを言葉で閉じる"),
                new PostTradeStage("Settlement", "back office", "受渡と cash movement を確定する", settledQuantity <= 0L ? "未受渡" : settlementLabel(stageTime, 2), "position と cash の truth を ledger に落とす")
            ),
            new FeeBreakdown(
                grossNotional,
                commission,
                exchangeFee,
                taxes,
                netCashMovement,
                List.of(
                    "commission は 3bps の簡易モデル",
                    "exchange fee は venue fee の教育用近似",
                    "実在の税率・手数料テーブルとは接続しない"
                )
            ),
            new StatementPreview(
                accountId,
                order.symbol(),
                symbolName,
                settledQuantity,
                averagePrice,
                settledQuantity <= 0L ? "T+0 (未受渡)" : settlementLabel(stageTime, 2),
                formatSignedYen(netCashMovement),
                List.of(
                    order.side().equalsIgnoreCase("BUY") ? "buy side では cash outflow と position increase が対になる" : "sell side では cash inflow と position decrease が対になる",
                    snapshot.status() == OrderStatus.PARTIALLY_FILLED ? "remaining quantity は statement ではなく OMS 側で追う" : "statement は fill 済数量だけを表す",
                    "market structure reference: " + marketStructure.venueState() + " / spread " + round2(marketStructure.spreadBps()) + "bps"
                )
            ),
            List.of(
                new SettlementCheck("数量整合", "fill 総数 = statement settled quantity", settledQuantity + "株"),
                new SettlementCheck("現金整合", "gross notional + fee/tax = net cash movement", formatSignedYen(netCashMovement)),
                new SettlementCheck("受渡日", "現物株は T+2 を基準に扱う", settledQuantity <= 0L ? "未受渡" : settlementLabel(stageTime, 2))
            ),
            List.of(
                new CorporateActionHook("Cash Dividend", "権利落ち後の statement 説明に影響", "position date と record date を結びつける必要がある"),
                new CorporateActionHook("Stock Split", "保有数量と平均単価の見え方が変わる", "post-trade book の数量換算が必要"),
                new CorporateActionHook("Tender Offer", "通常受渡と違う案内が必要", "corporate action workflow に引き渡す入口が必要")
            )
        );
    }

    private ParentExecutionState buildParentExecutionState(
        OrderView order,
        ScenarioSnapshot snapshot,
        ExecutionPackage executionPackage
    ) {
        List<ChildExecutionState> childStates = executionPackage.childOrders().stream()
            .map(slice -> {
                long executedQuantity;
                String state;
                String nextAction;
                if (snapshot.status() == OrderStatus.REJECTED) {
                    executedQuantity = 0L;
                    state = "REJECTED";
                    nextAction = "reject reason を確認して再設計";
                } else if (snapshot.status() == OrderStatus.CANCELED || snapshot.status() == OrderStatus.EXPIRED) {
                    executedQuantity = 0L;
                    state = snapshot.status().name();
                    nextAction = "残数量は再執行方針を別途決める";
                } else {
                    long ratioExecuted = Math.min(slice.plannedQuantity(), Math.round((double) snapshot.filledQuantity() * slice.plannedQuantity() / Math.max(1L, order.quantity())));
                    executedQuantity = ratioExecuted;
                    state = ratioExecuted >= slice.plannedQuantity()
                        ? "FILLED"
                        : ratioExecuted > 0L
                        ? "WORKING"
                        : "QUEUED";
                    nextAction = ratioExecuted >= slice.plannedQuantity()
                        ? "次 slice へ進む"
                        : snapshot.remainingQuantity() > 0L
                        ? "participation を確認しつつ残数量を継続"
                        : "親注文完了";
                }
                return new ChildExecutionState(
                    slice.id(),
                    state,
                    slice.venueIntent(),
                    slice.plannedQuantity(),
                    executedQuantity,
                    Math.max(0L, slice.plannedQuantity() - executedQuantity),
                    slice.benchmarkPrice(),
                    executedQuantity > 0L ? slice.expectedFillPrice() : 0.0,
                    executedQuantity > 0L ? slice.expectedSlippageBps() : 0.0,
                    nextAction
                );
            })
            .toList();

        return new ParentExecutionState(
            snapshot.statusEventAt() != null ? snapshot.statusEventAt() : order.submittedAt(),
            order.id(),
            accountId,
            order.symbol(),
            snapshot.status().name(),
            executionPackage.parentOrderPlan().chosenStyle(),
            executionPackage.parentOrderPlan().totalQuantity(),
            snapshot.filledQuantity(),
            snapshot.remainingQuantity(),
            executionPackage.parentOrderPlan().targetParticipationPercent(),
            round2(snapshot.filledQuantity() <= 0L ? 0.0 : ((double) snapshot.filledQuantity() / Math.max(1L, order.quantity())) * 100.0),
            executionPackage.parentOrderPlan().scheduleWindowMinutes() + "分",
            childStates,
            List.of(
                snapshot.status() == OrderStatus.PARTIALLY_FILLED ? "partial fill の間は arrival benchmark と remaining quantity を同時に追う" : "親注文の benchmark は固定して child の判断だけを動かす",
                snapshot.status() == OrderStatus.REJECTED ? "reject 後に working state を成功扱いへ丸めない" : "child state の executed total と parent filled total を一致させる",
                snapshot.remainingQuantity() > 0L ? "未約定残は close まで carry するか cancel するかを operator が決める" : "親注文完了後に allocation を fix する"
            )
        );
    }

    private AllocationState buildAllocationState(
        OrderView order,
        ScenarioSnapshot snapshot,
        ExecutionPackage executionPackage
    ) {
        long allocatedQuantity = snapshot.filledQuantity();
        List<BookAllocationState> books = executionPackage.allocationPlan().allocations().stream()
            .map(book -> {
                long target = book.quantity();
                long allocated = snapshot.filledQuantity() <= 0L ? 0L : Math.min(target, Math.round((double) allocatedQuantity * target / Math.max(1L, executionPackage.allocationPlan().totalQuantity())));
                String status = allocated >= target && target > 0L
                    ? "ALLOCATED"
                    : allocated > 0L
                    ? "PARTIAL"
                    : snapshot.filledQuantity() <= 0L
                    ? "PENDING_FILL"
                    : "PENDING";
                return new BookAllocationState(book.targetBook(), target, allocated, status, book.note());
            })
            .toList();

        return new AllocationState(
            snapshot.statusEventAt() != null ? snapshot.statusEventAt() : order.submittedAt(),
            order.id(),
            accountId,
            order.symbol(),
            snapshot.filledQuantity() <= 0L ? "PENDING_FILL" : snapshot.remainingQuantity() > 0L ? "PARTIAL" : "ALLOCATED",
            allocatedQuantity,
            Math.max(0L, order.quantity() - allocatedQuantity),
            executionPackage.allocationPlan().averagePrice(),
            books,
            List.of(
                "book 配賦の合計数量と parent fill 総数を一致させる",
                "allocation は child fill 単価ではなく平均単価を基準にする",
                snapshot.remainingQuantity() > 0L ? "working 中は未配賦残を持ち、statement へ先出ししない" : "配賦完了後に statement と confirm の数量を閉じる"
            )
        );
    }

    private SettlementProjection buildSettlementProjection(
        OrderView order,
        ScenarioSnapshot snapshot,
        PostTradePackage postTradePackage
    ) {
        String settlementStatus = snapshot.filledQuantity() <= 0L
            ? "PENDING_FILL"
            : snapshot.remainingQuantity() > 0L
            ? "PARTIAL"
            : "READY_TO_SETTLE";
        String cashLegStatus = snapshot.filledQuantity() <= 0L ? "UNPOSTED" : "PENDING_VALUE_DATE";
        String securitiesLegStatus = snapshot.filledQuantity() <= 0L ? "UNPOSTED" : "PENDING_DELIVERY";

        return new SettlementProjection(
            snapshot.statusEventAt() != null ? snapshot.statusEventAt() : order.submittedAt(),
            order.id(),
            accountId,
            order.symbol(),
            settlementStatus,
            "Trade Date " + new java.util.Date(order.submittedAt()),
            postTradePackage.statementPreview().settlementDateLabel(),
            postTradePackage.feeBreakdown().grossNotional(),
            postTradePackage.feeBreakdown().netCashMovement(),
            postTradePackage.statementPreview().settledQuantity(),
            cashLegStatus,
            securitiesLegStatus,
            List.of(
                snapshot.status() == OrderStatus.PARTIALLY_FILLED ? "未約定残があるため statement quantity と parent quantity を混同しない" : "受渡数量は fill 済数量だけで確定する",
                postTradePackage.feeBreakdown().taxes() > 0L ? "sell side の税控除を cash leg に含める" : "buy side は cash outflow のみを扱う"
            ),
            snapshot.filledQuantity() <= 0L ? "fill 確定待ち" : "settlement calendar と fail exception を監視する"
        );
    }

    private StatementProjection buildStatementProjection(
        OrderView order,
        ScenarioSnapshot snapshot,
        PostTradePackage postTradePackage
    ) {
        return new StatementProjection(
            snapshot.statusEventAt() != null ? snapshot.statusEventAt() : order.submittedAt(),
            order.id(),
            accountId,
            order.symbol(),
            snapshot.filledQuantity() <= 0L ? "DRAFT" : "READY",
            "CONF-" + order.id().toUpperCase(Locale.ROOT),
            "STMT-" + order.id().toUpperCase(Locale.ROOT),
            order.symbol() + " " + postTradePackage.statementPreview().settledQuantity() + "株 / 平均 " + postTradePackage.statementPreview().averagePrice(),
            List.of(
                new StatementLine("口座", accountId, "statement の受取先"),
                new StatementLine("銘柄", postTradePackage.statementPreview().symbolName(), "内部 symbol と表示名を分けて保持"),
                new StatementLine("受渡数量", postTradePackage.statementPreview().settledQuantity() + "株", "fill 済数量のみを使う"),
                new StatementLine("平均単価", String.valueOf(postTradePackage.statementPreview().averagePrice()), "block average"),
                new StatementLine("現金移動", postTradePackage.statementPreview().netCashMovementLabel(), "fee/tax 控除後"),
                new StatementLine("受渡日", postTradePackage.statementPreview().settlementDateLabel(), "trade date と混同しない")
            ),
            List.of(
                "confirm / statement は final-out より短く、対外説明に耐える文言を優先する",
                "order status が canceled でも fill 済数量があれば statement は draft ではなく partial execution を示す",
                "顧客向け表現と raw event ref を混在させない"
            )
        );
    }

    private RiskSnapshot buildRiskSnapshot(ScenarioSnapshot snapshot) {
        double marketValue = snapshot.positions().stream()
            .mapToDouble(position -> marketDataService.getCurrentPrice(position.symbol()) * position.netQty())
            .sum();

        List<RiskConcentrationMetric> concentration = snapshot.positions().stream()
            .filter(position -> position.netQty() != 0L)
            .map(position -> {
                double currentPrice = marketDataService.getCurrentPrice(position.symbol());
                double exposure = currentPrice * position.netQty();
                double weight = marketValue == 0.0 ? 0.0 : (exposure / marketValue) * 100.0;
                String note = Math.abs(weight) >= 45.0
                    ? "single-name 依存が強く stress と liquidity を同時に見る領域"
                    : Math.abs(weight) >= 25.0
                    ? "portfolio の主因。削減や hedge の優先候補"
                    : "補助的寄与。相関と scenario で読む";
                return new RiskConcentrationMetric(position.symbol(), marketDataService.getStockInfo(position.symbol()).name(), round2(exposure), round2(weight), note);
            })
            .toList();

        List<RiskLiquidityMetric> liquidity = snapshot.positions().stream()
            .filter(position -> position.netQty() != 0L)
            .map(position -> {
                MarketStructureSnapshot structure = marketDataService.getMarketStructure(position.symbol());
                long visibleTop = structure.bidQuantity() + structure.askQuantity();
                double participation = visibleTop == 0 ? 0.0 : ((double) Math.abs(position.netQty()) / visibleTop) * 100.0;
                double daysToExit = visibleTop == 0 ? 0.0 : Math.max(0.5, Math.abs(position.netQty()) / Math.max(visibleTop * 6.0, 1.0));
                String note = participation > 150.0
                    ? "top-of-book に対して大きく複数セッション前提"
                    : participation > 60.0
                    ? "POV や care order が欲しい水準"
                    : "通常の participation 範囲で説明しやすい";
                return new RiskLiquidityMetric(
                    position.symbol(),
                    marketDataService.getStockInfo(position.symbol()).name(),
                    position.netQty(),
                    visibleTop,
                    round2(participation),
                    round2(daysToExit),
                    note
                );
            })
            .toList();

        List<RiskScenarioLibraryEntry> scenarios = List.of(
            new RiskScenarioLibraryEntry("single-name-gap", "単一銘柄ギャップダウン", "concentration", "-12%", "材料イベントで one-name risk が顕在化", "single-name exposure と liquidity を同時に確認"),
            new RiskScenarioLibraryEntry("market-beta-down", "市場全体の beta shock", "portfolio", "-5%", "beta の高い book で同方向リスクが顕在化", "gross / net を一緒に説明"),
            new RiskScenarioLibraryEntry("spread-widening", "流動性悪化と spread 拡大", "liquidity", "+40% spread", "価格より execution cost が悪化", "arrival benchmark と fill quality を再評価"),
            new RiskScenarioLibraryEntry("basis-break", "ヘッジ不一致", "hedge", "相関崩れ", "ヘッジ対象と実ポジションの基礎関係が崩れる", "hedge ratio だけで安心しない")
        );

        List<RiskBacktestSample> samples = buildRiskBacktestSamples(snapshot.positions());
        long breachCount = samples.stream().filter(RiskBacktestSample::breached).count();
        double breachRate = samples.isEmpty() ? 0.0 : (breachCount * 100.0) / samples.size();
        double averageTailLoss = samples.stream()
            .mapToDouble(RiskBacktestSample::pnl)
            .filter(value -> value < 0.0)
            .map(Math::abs)
            .average()
            .orElse(0.0);

        return new RiskSnapshot(
            System.currentTimeMillis(),
            accountId,
            round2(marketValue),
            round2(snapshot.cashBalance()),
            concentration,
            liquidity,
            scenarios,
            new RiskBacktestingPreview(
                samples.size(),
                round2(breachRate),
                round2(averageTailLoss),
                "教育用 backtest。直近 tick を使い、stress breach を単純判定している。",
                samples
            ),
            List.of(
                new RiskModelBoundary("Concentration", "gross / net と weight を見る入口", "current price と保有数量", "相関と beta decomposition"),
                new RiskModelBoundary("Liquidity", "exit difficulty を直感で掴む入口", "top-of-book と position size", "真の market impact model、queue depletion"),
                new RiskModelBoundary("Scenario Library", "何を shock するかを業務言語に変換", "single-name / market / spread widening", "stochastic correlation、vol surface"),
                new RiskModelBoundary("Backtesting", "前提が外れた回数をざっくり把握", "簡易 breach count と tail loss", "regulatory backtesting や model governance")
            ),
            List.of(
                concentration.stream().anyMatch(metric -> Math.abs(metric.weightPercent()) >= 45.0)
                    ? "single-name concentration が高く、liquidity と stress を同時に見る必要あり"
                    : "concentration は分散されているが、相関 shock を別で確認する",
                liquidity.stream().anyMatch(metric -> metric.participationPercent() >= 100.0)
                    ? "visible top-of-book に対して position が大きく、exit 設計が必要"
                    : "liquidity 余地はあるが spread widening scenario を別途確認する"
            )
        );
    }

    private SettlementExceptionWorkflow buildSettlementExceptionWorkflow(OrderView order, ScenarioSnapshot snapshot) {
        String workflowStatus = switch (snapshot.status()) {
            case FILLED -> "CLEAR";
            case PARTIALLY_FILLED -> "WATCH";
            case REJECTED -> "NOT_APPLICABLE";
            case CANCELED, EXPIRED -> "RELEASED";
            default -> "PENDING_FILL";
        };
        String exceptionType = switch (snapshot.status()) {
            case PARTIALLY_FILLED -> "PARTIAL_EXECUTION";
            case CANCELED -> "MANUAL_RELEASE";
            case EXPIRED -> "TIME_IN_FORCE";
            case REJECTED -> "PRE_TRADE_REJECT";
            default -> "NONE";
        };
        String blockedStage = switch (snapshot.status()) {
            case PARTIALLY_FILLED -> "allocation";
            case CANCELED, EXPIRED -> "settlement";
            case REJECTED -> "execution";
            default -> "none";
        };
        String nextAction = switch (snapshot.status()) {
            case PARTIALLY_FILLED -> "残数量の allocation / statement 出し分けを確認する";
            case CANCELED, EXPIRED -> "拘束解放と顧客向け説明が一致しているか確認する";
            case REJECTED -> "risk reject と post-trade 入口が混ざっていないか確認する";
            default -> "例外ワークフローは閉じている";
        };
        return new SettlementExceptionWorkflow(
            snapshot.statusEventAt() != null ? snapshot.statusEventAt() : order.submittedAt(),
            order.id(),
            accountId,
            order.symbol(),
            workflowStatus,
            exceptionType,
            blockedStage,
            snapshot.status() == OrderStatus.PARTIALLY_FILLED ? "T+0 監視中" : "T+0 問題なし",
            switch (snapshot.status()) {
                case PARTIALLY_FILLED -> "親注文がまだ working で fill / statement 数量が分かれる";
                case CANCELED -> "reservation 解放は済んでも fill 不在のまま order は終了";
                case EXPIRED -> "time-in-force 失効で settlement 不要になった";
                case REJECTED -> "execution 前 reject で post-trade workflow は起動しない";
                default -> "追加例外なし";
            },
            nextAction,
            List.of(
                "fill quantity と settlement quantity を同一視しない",
                "statement draft を final-out 完了より先に確定しない",
                "fail / exception は order status ではなく post-trade workflow で扱う"
            ),
            List.of(
                snapshot.status() == OrderStatus.PARTIALLY_FILLED ? "middle office に partial allocation の確認を依頼" : "operator escalation 不要",
                "ledger と statement を同じ event ref で説明できるように残す"
            )
        );
    }

    private CorporateActionWorkflow buildCorporateActionWorkflow(OrderView order, ScenarioSnapshot snapshot) {
        String eventName = switch (order.symbol()) {
            case "7203" -> "Cash Dividend";
            case "6758" -> "Stock Split";
            default -> "Ticker Change";
        };
        return new CorporateActionWorkflow(
            snapshot.statusEventAt() != null ? snapshot.statusEventAt() : order.submittedAt(),
            order.id(),
            accountId,
            order.symbol(),
            eventName,
            snapshot.filledQuantity() > 0L ? "WATCH" : "STANDBY",
            "record date pending",
            "effective date pending",
            snapshot.filledQuantity() > 0L
                ? "保有数量と平均単価の説明に corporate action を織り込む必要がある"
                : "保有残が無いので顧客向け影響は限定的",
            eventName + " が発生すると statement line と position quantity の見え方が変わる",
            snapshot.filledQuantity() > 0L ? "asset master と statement 文面を同時に更新する" : "watch list のみ更新する",
            List.of(
                "order history は変えず position / statement 側で変換する",
                "record date / effective date / payment date を別欄で持つ",
                "corporate action 前後で ledger continuity を維持する"
            )
        );
    }

    private MarginProjection buildMarginProjection(ScenarioSnapshot snapshot, RiskSnapshot riskSnapshot) {
        double marketValue = Math.abs(riskSnapshot.marketValue());
        double liquidityBuffer = riskSnapshot.liquidity().stream()
            .mapToDouble(metric -> metric.participationPercent() >= 100.0 ? 120_000.0 : metric.participationPercent() >= 60.0 ? 60_000.0 : 25_000.0)
            .sum();
        double concentrationBuffer = riskSnapshot.concentration().stream()
            .mapToDouble(metric -> Math.abs(metric.weightPercent()) >= 45.0 ? 180_000.0 : Math.abs(metric.weightPercent()) >= 25.0 ? 90_000.0 : 30_000.0)
            .sum();
        double marginUsed = round2((marketValue * 0.18) + liquidityBuffer + concentrationBuffer);
        double marginLimit = Math.max(1_500_000.0, round2(snapshot.availableBuyingPower() + snapshot.reservedBuyingPower() + 750_000.0));
        double utilization = marginLimit == 0.0 ? 0.0 : round2((marginUsed / marginLimit) * 100.0);
        String breachStatus = utilization >= 95.0
            ? "BREACH"
            : utilization >= 80.0
            ? "WATCH"
            : "WITHIN_LIMIT";
        List<String> breachedLimits = new java.util.ArrayList<>();
        if (utilization >= 80.0) {
            breachedLimits.add("portfolio margin utilization " + utilization + "%");
        }
        if (riskSnapshot.marginAlerts().stream().anyMatch(alert -> alert.contains("single-name"))) {
            breachedLimits.add("single-name concentration alert");
        }
        return new MarginProjection(
            System.currentTimeMillis(),
            accountId,
            "educational portfolio margin proxy",
            marginLimit,
            marginUsed,
            utilization,
            breachStatus,
            breachedLimits,
            breachStatus.equals("BREACH")
                ? List.of("new order を抑制し、削減 / hedge を優先する", "liquidity stress と concentration explanation を更新する")
                : breachStatus.equals("WATCH")
                ? List.of("headroom を毎注文で再確認する", "scenario stress を追加で回す")
                : List.of("現在は margin headroom あり"),
            List.of(
                "reservation は order-level の拘束、margin は portfolio-level の許容度",
                "資産相関と清算機関ルールは簡略化している",
                "feed stale 時は valuation と margin の両方を guard 対象にする"
            )
        );
    }

    private ScenarioEvaluationHistory buildScenarioEvaluationHistory(ScenarioSnapshot snapshot) {
        return new ScenarioEvaluationHistory(
            System.currentTimeMillis(),
            accountId,
            "latest replay at " + new java.util.Date(snapshot.statusEventAt() != null ? snapshot.statusEventAt() : System.currentTimeMillis()),
            List.of(
                new ScenarioEvaluationEntry("single-name gap", "-12%", round2(-snapshot.fillNotional() * 0.12), "one-name shock の最初の説明軸"),
                new ScenarioEvaluationEntry("market beta down", "-5%", round2(-snapshot.fillNotional() * 0.05), "gross / net の影響を見る軸"),
                new ScenarioEvaluationEntry("spread widening", "+40% spread", round2(-Math.abs(snapshot.fillNotional()) * 0.008), "execution quality と liquidity を同時に見る軸")
            )
        );
    }

    private BacktestHistory buildBacktestHistory(RiskSnapshot riskSnapshot) {
        return new BacktestHistory(
            System.currentTimeMillis(),
            accountId,
            "直近 8 tick",
            riskSnapshot.backtesting().breachRatePercent(),
            riskSnapshot.backtesting().samples().stream()
                .map(sample -> new BacktestHistoryPoint(
                    sample.label(),
                    sample.pnl(),
                    sample.breached(),
                    sample.breached() ? "loss limit を超えたサンプル" : "想定範囲内"
                ))
                .toList()
        );
    }

    private List<RiskBacktestSample> buildRiskBacktestSamples(List<BackOfficePosition> positions) {
        java.util.ArrayList<RiskBacktestSample> samples = new java.util.ArrayList<>();
        if (positions.isEmpty()) {
            return List.of();
        }
        int sampleCount = 8;
        for (int index = 0; index < sampleCount; index++) {
            double pnl = 0.0;
            for (BackOfficePosition position : positions) {
                List<appjava.market.PricePoint> history = marketDataService.getPriceHistory(position.symbol(), 60);
                if (history.size() < index + 2) {
                    continue;
                }
                int currentIndex = history.size() - 1 - index;
                double previous = history.get(currentIndex - 1).price();
                double current = history.get(currentIndex).price();
                pnl += (current - previous) * position.netQty();
            }
            double rounded = round2(pnl);
            samples.add(new RiskBacktestSample("tick-" + (sampleCount - index), rounded, rounded < -80_000.0));
        }
        return List.copyOf(samples);
    }

    private List<LedgerEntry> buildLedgerEntries(OrderView order, ScenarioSnapshot snapshot) {
        long submittedAt = order.submittedAt();
        long acceptedAt = submittedAt + 250L;
        long reservationAt = acceptedAt + 50L;
        long statusAt = snapshot.statusEventAt() != null ? snapshot.statusEventAt() : submittedAt + 1_000L;
        long signedQuantityDelta = order.side().equalsIgnoreCase("BUY") ? snapshot.filledQuantity() : -snapshot.filledQuantity();
        long signedCashDelta = signedCashDelta(order.side(), snapshot.fillNotional());
        java.util.ArrayList<LedgerEntry> entries = new java.util.ArrayList<>();

        if (snapshot.originalReservedAmount() > 0L) {
            entries.add(new LedgerEntry(
                "ledger-" + order.id() + "-reserve",
                "replay-ledger:" + order.id() + ":1",
                accountId,
                order.id(),
                "RESERVATION_CREATED",
                order.symbol(),
                order.side(),
                0L,
                0L,
                snapshot.originalReservedAmount(),
                0L,
                "拘束余力を計上",
                reservationAt,
                "replay-scenario"
            ));
        }

        switch (snapshot.status()) {
            case PARTIALLY_FILLED -> entries.add(new LedgerEntry(
                "ledger-" + order.id() + "-partial-fill",
                "replay-ledger:" + order.id() + ":2",
                accountId,
                order.id(),
                "PARTIAL_FILL",
                order.symbol(),
                order.side(),
                signedQuantityDelta,
                signedCashDelta,
                -(snapshot.originalReservedAmount() - snapshot.reservedBuyingPower()),
                snapshot.realizedPnl(),
                "一部約定を反映",
                statusAt,
                "replay-scenario"
            ));
            case FILLED -> entries.add(new LedgerEntry(
                "ledger-" + order.id() + "-fill",
                "replay-ledger:" + order.id() + ":2",
                accountId,
                order.id(),
                "FULL_FILL",
                order.symbol(),
                order.side(),
                signedQuantityDelta,
                signedCashDelta,
                -snapshot.originalReservedAmount(),
                snapshot.realizedPnl(),
                "全量約定を反映",
                statusAt,
                "replay-scenario"
            ));
            case CANCELED -> {
                if (snapshot.originalReservedAmount() > 0L) {
                    entries.add(new LedgerEntry(
                        "ledger-" + order.id() + "-cancel",
                        "replay-ledger:" + order.id() + ":2",
                        accountId,
                        order.id(),
                        "ORDER_CANCELED",
                        order.symbol(),
                        order.side(),
                        0L,
                        0L,
                        -snapshot.originalReservedAmount(),
                        0L,
                        "取消に伴い拘束を解放",
                        statusAt,
                        "replay-scenario"
                    ));
                }
            }
            case EXPIRED -> {
                if (snapshot.originalReservedAmount() > 0L) {
                    entries.add(new LedgerEntry(
                        "ledger-" + order.id() + "-expired",
                        "replay-ledger:" + order.id() + ":2",
                        accountId,
                        order.id(),
                        "ORDER_EXPIRED",
                        order.symbol(),
                        order.side(),
                        0L,
                        0L,
                        -snapshot.originalReservedAmount(),
                        0L,
                        "失効に伴い拘束を解放",
                        statusAt,
                        "replay-scenario"
                    ));
                }
            }
            case REJECTED -> entries.add(new LedgerEntry(
                "ledger-" + order.id() + "-rejected",
                "replay-ledger:" + order.id() + ":1",
                accountId,
                order.id(),
                "ORDER_REJECTED",
                order.symbol(),
                order.side(),
                0L,
                0L,
                0L,
                0L,
                "注文拒否",
                statusAt,
                "replay-scenario"
            ));
            default -> {
            }
        }
        return List.copyOf(entries);
    }

    private OrderEventView event(
        String orderId,
        String eventType,
        long eventAt,
        String label,
        String detail,
        int sequence
    ) {
        return new OrderEventView(
            orderId,
            eventType,
            eventAt,
            label,
            detail,
            "replay-scenario",
            "replay:" + orderId + ":" + sequence
        );
    }

    private void validate(OrderRequest request) {
        if (request.symbol() == null || request.symbol().isBlank()) {
            throw new JsonHttpHandler.ValidationException("INVALID_SYMBOL");
        }
        marketDataService.getStockInfo(request.symbol());
        if (request.quantity() <= 0) {
            throw new JsonHttpHandler.ValidationException("INVALID_QTY");
        }
        if (request.side() == null || request.side().isBlank()) {
            throw new JsonHttpHandler.ValidationException("INVALID_SIDE");
        }
        if (request.type() == null || request.type().isBlank()) {
            throw new JsonHttpHandler.ValidationException("INVALID_TYPE");
        }
        if ("LIMIT".equalsIgnoreCase(request.type()) && (request.price() == null || request.price() <= 0.0)) {
            throw new JsonHttpHandler.ValidationException("INVALID_PRICE");
        }
    }

    private String normalizeScenario(String scenarioName) {
        return scenarioName == null ? "" : scenarioName.trim().toLowerCase(Locale.ROOT);
    }

    private long signedCashDelta(String side, long notional) {
        return side.equalsIgnoreCase("BUY") ? -notional : notional;
    }

    private double slippageBps(double benchmarkPrice, double executionPrice) {
        if (benchmarkPrice <= 0.0) {
            return 0.0;
        }
        return round2(((executionPrice - benchmarkPrice) / benchmarkPrice) * 10_000.0);
    }

    private String settlementLabel(long baseTimeMillis, int plusBusinessDays) {
        return "T+" + plusBusinessDays + " 想定 (" + new java.util.Date(baseTimeMillis + (plusBusinessDays * 24L * 60L * 60L * 1000L)).toString() + ")";
    }

    private String formatSignedYen(long value) {
        String prefix = value > 0 ? "+" : "";
        return prefix + String.format(Locale.US, "%,d円", value);
    }

    private static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    private record ScenarioSnapshot(
        OrderStatus status,
        String statusReason,
        long filledQuantity,
        long remainingQuantity,
        Long statusEventAt,
        double executionTimeMs,
        long cashBalance,
        long availableBuyingPower,
        long reservedBuyingPower,
        long realizedPnl,
        List<BackOfficePosition> positions,
        long fillNotional,
        long originalReservedQuantity,
        long originalReservedAmount,
        String reservationStatus,
        String timeInForce,
        Long expireAt
    ) {
        boolean wasReserved() {
            return originalReservedAmount > 0L || originalReservedQuantity > 0L;
        }
    }
}
