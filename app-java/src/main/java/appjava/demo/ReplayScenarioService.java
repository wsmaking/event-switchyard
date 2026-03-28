package appjava.demo;

import appjava.account.AccountOverview;
import appjava.clients.BackOfficeClient;
import appjava.clients.BackOfficeClient.BackOfficePosition;
import appjava.clients.OmsClient;
import appjava.http.JsonHttpHandler;
import appjava.market.MarketDataService;
import appjava.order.OrderRequest;
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

    public ReplayScenarioService(
        String accountId,
        MarketDataService marketDataService,
        BackOfficeClient backOfficeClient,
        OmsClient omsClient
    ) {
        this.accountId = accountId;
        this.marketDataService = marketDataService;
        this.backOfficeClient = backOfficeClient;
        this.omsClient = omsClient;
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

        omsClient.upsertOrder(order);
        backOfficeClient.upsertOverview(overview);
        backOfficeClient.replacePositions(accountId, snapshot.positions());
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
            request.timeInForce(),
            request.expireAt()
        );
    }

    private ScenarioSnapshot canceledSnapshot(OrderRequest request, long submittedAt, double workingPrice) {
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
            request.timeInForce(),
            request.expireAt()
        );
    }

    private ScenarioSnapshot expiredSnapshot(OrderRequest request, long submittedAt, double workingPrice) {
        long expireAt = request.expireAt() != null ? request.expireAt() : submittedAt + 2_000L;
        long statusAt = Math.max(submittedAt + 2_200L, expireAt);
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
            request.timeInForce(),
            request.expireAt()
        );
    }

    private BackOfficePosition newPosition(OrderRequest request, long filledQuantity, double avgPrice) {
        long signedQuantity = request.side().equalsIgnoreCase("BUY") ? filledQuantity : -filledQuantity;
        return new BackOfficePosition(accountId, request.symbol(), signedQuantity, round2(avgPrice));
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
        String timeInForce,
        Long expireAt
    ) {
    }
}
