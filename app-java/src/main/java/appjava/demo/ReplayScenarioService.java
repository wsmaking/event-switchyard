package appjava.demo;

import appjava.account.AccountOverview;
import appjava.clients.BackOfficeClient;
import appjava.clients.BackOfficeClient.BackOfficeOrderState;
import appjava.clients.BackOfficeClient.BackOfficePosition;
import appjava.clients.BackOfficeClient.LedgerEntry;
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
