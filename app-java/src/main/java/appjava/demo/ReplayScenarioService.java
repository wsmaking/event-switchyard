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
import appjava.clients.BackOfficeClient.SettlementCheck;
import appjava.clients.BackOfficeClient.StatementPreview;

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
