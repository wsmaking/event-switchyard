package appjava.order;

import appjava.account.AccountOverview;
import appjava.clients.GatewayClient;
import appjava.clients.GatewayClient.GatewayOrderRequest;
import appjava.clients.GatewayClient.GatewayOrderSnapshot;
import appjava.clients.GatewayClient.GatewaySubmitResult;
import appjava.clients.OmsClient;
import appjava.http.JsonHttpHandler;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public final class OrderService {
    private final String accountId;
    private final GatewayClient gatewayClient;
    private final OmsClient omsClient;

    public OrderService(String accountId, GatewayClient gatewayClient, OmsClient omsClient) {
        this.accountId = accountId;
        this.gatewayClient = gatewayClient;
        this.omsClient = omsClient;
    }

    public OrderView submit(OrderRequest request) {
        validate(request);
        long submittedAt = System.currentTimeMillis();
        long startNanos = System.nanoTime();
        String clientOrderId = UUID.randomUUID().toString();

        GatewaySubmitResult result = gatewayClient.submitOrder(
            new GatewayOrderRequest(
                request.symbol(),
                request.side(),
                request.type(),
                request.quantity(),
                request.price() == null ? null : Math.round(request.price()),
                request.timeInForce(),
                request.expireAt(),
                clientOrderId
            ),
            clientOrderId
        );

        long executionTimeMs = Math.round((System.nanoTime() - startNanos) / 1_000_000.0);
        String orderId = result.orderId() != null ? result.orderId() : clientOrderId;
        OrderStatus status = result.accepted() ? OrderStatus.ACCEPTED : OrderStatus.REJECTED;
        OrderView order = new OrderView(
            orderId,
            accountId,
            request.symbol(),
            request.side(),
            request.type(),
            request.quantity(),
            request.price(),
            request.timeInForce(),
            request.expireAt(),
            status,
            submittedAt,
            status == OrderStatus.FILLED ? submittedAt : null,
            (double) executionTimeMs,
            result.reason(),
            0L,
            request.quantity()
        );
        omsClient.upsertOrder(order);
        refreshOrder(orderId);
        return omsClient.fetchOrder(orderId).orElse(order);
    }

    public List<OrderView> listOrders() {
        List<OrderView> orders = omsClient.fetchOrders();
        orders.stream()
            .filter(order -> !order.status().isTerminal())
            .map(OrderView::id)
            .forEach(this::refreshOrder);
        return omsClient.fetchOrders();
    }

    public OrderView getOrder(String orderId) {
        OrderView order = omsClient.fetchOrder(orderId).orElse(null);
        if (order == null) {
            throw new JsonHttpHandler.NotFoundException("order_not_found:" + orderId);
        }
        if (!order.status().isTerminal()) {
            refreshOrder(orderId);
        }
        return omsClient.fetchOrder(orderId).orElse(order);
    }

    public void reset() {
        omsClient.reset();
    }

    public List<OrderTimelineEntry> buildTimeline(OrderView order) {
        List<OrderTimelineEntry> entries = new ArrayList<>();
        long acceptedAt = order.submittedAt() + 250L;
        long statusAt = statusEventAt(order);
        entries.add(new OrderTimelineEntry(
            "ORDER_SUBMITTED",
            order.submittedAt(),
            "注文受付",
            order.symbol() + " " + order.side() + " " + order.quantity() + "株"
        ));

        switch (order.status()) {
            case PENDING_ACCEPT -> entries.add(new OrderTimelineEntry(
                "GATEWAY_PENDING",
                acceptedAt,
                "Gateway受付待ち",
                "Gatewayで受理待ち"
            ));
            case ACCEPTED -> entries.add(new OrderTimelineEntry(
                "ORDER_ACCEPTED",
                acceptedAt,
                "OMS受付済",
                order.statusReason() == null ? "取引所受付済" : order.statusReason()
            ));
            case PARTIALLY_FILLED -> {
                entries.add(new OrderTimelineEntry(
                    "ORDER_ACCEPTED",
                    acceptedAt,
                    "OMS受付済",
                    "取引所受付済"
                ));
                entries.add(new OrderTimelineEntry(
                    "PARTIAL_FILL",
                    statusAt,
                    "一部約定",
                    order.filledQuantity() + "株約定 / 残" + order.remainingQuantity() + "株"
                ));
            }
            case FILLED -> {
                entries.add(new OrderTimelineEntry(
                    "ORDER_ACCEPTED",
                    acceptedAt,
                    "OMS受付済",
                    "取引所受付済"
                ));
                entries.add(new OrderTimelineEntry(
                    "FULL_FILL",
                    statusAt,
                    "全量約定",
                    order.quantity() + "株が約定"
                ));
            }
            case CANCEL_PENDING -> {
                entries.add(new OrderTimelineEntry(
                    "ORDER_ACCEPTED",
                    acceptedAt,
                    "OMS受付済",
                    "取引所受付済"
                ));
                entries.add(new OrderTimelineEntry(
                    "CANCEL_REQUESTED",
                    statusAt,
                    "取消送信",
                    "取消要求を取引所へ送信"
                ));
            }
            case CANCELED -> {
                entries.add(new OrderTimelineEntry(
                    "ORDER_ACCEPTED",
                    acceptedAt,
                    "OMS受付済",
                    "取引所受付済"
                ));
                entries.add(new OrderTimelineEntry(
                    "CANCEL_REQUESTED",
                    Math.max(acceptedAt + 600L, statusAt - 700L),
                    "取消送信",
                    "取消要求を取引所へ送信"
                ));
                entries.add(new OrderTimelineEntry(
                    "ORDER_CANCELED",
                    statusAt,
                    "取消完了",
                    order.statusReason() == null ? "ユーザー取消" : order.statusReason()
                ));
            }
            case EXPIRED -> {
                entries.add(new OrderTimelineEntry(
                    "ORDER_ACCEPTED",
                    acceptedAt,
                    "OMS受付済",
                    "取引所受付済"
                ));
                entries.add(new OrderTimelineEntry(
                    "ORDER_EXPIRED",
                    statusAt,
                    "失効",
                    order.statusReason() == null ? "期限切れ失効" : order.statusReason()
                ));
            }
            case REJECTED -> entries.add(new OrderTimelineEntry(
                "ORDER_REJECTED",
                statusAt,
                "注文拒否",
                order.statusReason() == null ? "拒否" : order.statusReason()
            ));
            case AMEND_PENDING -> {
                entries.add(new OrderTimelineEntry(
                    "ORDER_ACCEPTED",
                    acceptedAt,
                    "OMS受付済",
                    "取引所受付済"
                ));
                entries.add(new OrderTimelineEntry(
                    "AMEND_REQUESTED",
                    statusAt,
                    "訂正送信",
                    "訂正要求を取引所へ送信"
                ));
            }
        }
        return entries;
    }

    private long statusEventAt(OrderView order) {
        if (order.filledAt() != null) {
            return order.filledAt();
        }
        if (order.status() == OrderStatus.EXPIRED && order.expireAt() != null) {
            return Math.max(order.expireAt(), order.submittedAt() + 1_200L);
        }
        return order.submittedAt() + 1_800L;
    }

    private void refreshOrder(String orderId) {
        OrderView current = omsClient.fetchOrder(orderId).orElse(null);
        if (current == null) {
            return;
        }
        Optional<GatewayOrderSnapshot> snapshotOpt = gatewayClient.fetchOrder(orderId);
        if (snapshotOpt.isEmpty()) {
            return;
        }
        GatewayOrderSnapshot snapshot = snapshotOpt.get();
        OrderStatus status = mapStatus(snapshot.status());
        long filledQty = snapshot.filledQty();
        long remainingQty = Math.max(0L, snapshot.qty() - filledQty);
        Long filledAt = resolveFilledAt(status, snapshot.lastUpdateAt(), current.filledAt());

        OrderView updated = new OrderView(
            current.id(),
            snapshot.accountId() == null ? current.accountId() : snapshot.accountId(),
            snapshot.symbol() == null ? current.symbol() : snapshot.symbol(),
            snapshot.side() == null ? current.side() : snapshot.side(),
            snapshot.type() == null ? current.type() : snapshot.type(),
            (int) snapshot.qty(),
            resolvePrice(snapshot.price(), current.price()),
            snapshot.timeInForce() == null ? current.timeInForce() : snapshot.timeInForce(),
            snapshot.expireAt() == null ? current.expireAt() : snapshot.expireAt(),
            status,
            snapshot.acceptedAt() > 0 ? snapshot.acceptedAt() : current.submittedAt(),
            filledAt,
            current.executionTimeMs(),
            current.statusReason(),
            filledQty,
            remainingQty
        );
        omsClient.upsertOrder(updated);
    }

    private void validate(OrderRequest request) {
        if (request.symbol() == null || request.symbol().isBlank()) {
            throw new JsonHttpHandler.ValidationException("INVALID_SYMBOL");
        }
        if (request.quantity() <= 0) {
            throw new JsonHttpHandler.ValidationException("INVALID_QTY");
        }
        if ("LIMIT".equalsIgnoreCase(request.type()) && (request.price() == null || request.price() <= 0.0)) {
            throw new JsonHttpHandler.ValidationException("INVALID_PRICE");
        }
        if ("GTD".equalsIgnoreCase(request.timeInForce())) {
            if (request.expireAt() == null || request.expireAt() <= Instant.now().toEpochMilli()) {
                throw new JsonHttpHandler.ValidationException("INVALID_EXPIRE_AT");
            }
        }
    }

    private OrderStatus mapStatus(String rawStatus) {
        if (rawStatus == null) {
            return OrderStatus.ACCEPTED;
        }
        return switch (rawStatus.toUpperCase()) {
            case "PENDING_ACCEPT" -> OrderStatus.PENDING_ACCEPT;
            case "ACCEPTED", "SENT", "DURABLE" -> OrderStatus.ACCEPTED;
            case "PARTIALLY_FILLED" -> OrderStatus.PARTIALLY_FILLED;
            case "FILLED" -> OrderStatus.FILLED;
            case "CANCEL_REQUESTED", "CANCEL_PENDING" -> OrderStatus.CANCEL_PENDING;
            case "CANCELED" -> OrderStatus.CANCELED;
            case "EXPIRED" -> OrderStatus.EXPIRED;
            case "REJECTED" -> OrderStatus.REJECTED;
            case "AMEND_PENDING", "REPLACE_PENDING" -> OrderStatus.AMEND_PENDING;
            default -> OrderStatus.ACCEPTED;
        };
    }

    static Long resolveFilledAt(OrderStatus status, long snapshotLastUpdateAt, Long currentFilledAt) {
        if (status == OrderStatus.FILLED) {
            return Long.valueOf(snapshotLastUpdateAt);
        }
        return currentFilledAt;
    }

    static Double resolvePrice(Long snapshotPrice, Double currentPrice) {
        if (snapshotPrice == null) {
            return currentPrice;
        }
        return snapshotPrice.doubleValue();
    }
}
