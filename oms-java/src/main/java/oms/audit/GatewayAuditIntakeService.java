package oms.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import oms.http.JsonHttpHandler;
import oms.order.OrderEventView;
import oms.order.OrderReadModel;
import oms.order.OrderStatus;
import oms.order.OrderView;
import oms.order.ReservationView;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class GatewayAuditIntakeService {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private static final long DEFAULT_POLL_MS = 500L;

    private final OrderReadModel orderReadModel;
    private final boolean enabled;
    private final Path auditPath;
    private final Path offsetPath;
    private final String startMode;
    private final long pollMs;
    private final Object loopLock = new Object();
    private final InMemoryDeadLetterStore deadLetterStore;
    private final InMemoryPendingOrphanStore pendingOrphanStore;
    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong skipped = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong orphans = new AtomicLong();
    private final AtomicLong replays = new AtomicLong();
    private final AtomicLong currentOffset = new AtomicLong();
    private final AtomicLong lastEventAt = new AtomicLong();
    private final AtomicReference<String> state = new AtomicReference<>("IDLE");
    private final Instant startedAt = Instant.now();

    public GatewayAuditIntakeService(OrderReadModel orderReadModel) {
        this.orderReadModel = orderReadModel;
        this.enabled = parseBoolean(
            System.getProperty(
                "oms.gateway.audit.enable",
                System.getenv().getOrDefault("OMS_GATEWAY_AUDIT_ENABLE", "true")
            )
        );
        this.auditPath = resolvePath(
            System.getProperty(
                "oms.gateway.audit.path",
                System.getenv().getOrDefault("OMS_GATEWAY_AUDIT_PATH", "var/gateway/audit.log")
            )
        );
        this.offsetPath = resolvePath(
            System.getProperty(
                "oms.gateway.audit.offset.path",
                System.getenv().getOrDefault("OMS_GATEWAY_AUDIT_OFFSET_PATH", "var/java-replay/oms/audit.offset")
            )
        );
        this.startMode = System.getProperty(
            "oms.gateway.audit.start.mode",
            System.getenv().getOrDefault("OMS_GATEWAY_AUDIT_START_MODE", "tail")
        ).trim().toLowerCase();
        this.pollMs = Long.parseLong(
            System.getProperty(
                "oms.gateway.audit.poll.ms",
                System.getenv().getOrDefault("OMS_GATEWAY_AUDIT_POLL_MS", String.valueOf(DEFAULT_POLL_MS))
            )
        );
        this.deadLetterStore = new InMemoryDeadLetterStore();
        this.pendingOrphanStore = new InMemoryPendingOrphanStore();
    }

    public void start() {
        if (!enabled) {
            state.set("DISABLED");
            return;
        }
        Thread worker = new Thread(this::runLoop, "oms-java-gateway-audit");
        worker.setDaemon(true);
        worker.start();
    }

    public IntakeStatus snapshot() {
        return new IntakeStatus(
            enabled,
            state.get(),
            auditPath.toString(),
            offsetPath.toString(),
            startMode,
            startedAt,
            processed.get(),
            skipped.get(),
            duplicates.get(),
            orphans.get(),
            replays.get(),
            lastEventAt.get() == 0 ? null : lastEventAt.get(),
            currentOffset.get(),
            currentAuditSize(),
            deadLetterStore.size(),
            pendingOrphanStore.size()
        );
    }

    public List<DeadLetterEntryView> findDeadLetters(String orderId, int limit) {
        return deadLetterStore.find(orderId, limit);
    }

    public List<PendingOrphanEntryView> findPendingOrphans(String orderId, int limit) {
        return pendingOrphanStore.find(orderId, limit);
    }

    public ReplayResult replayFromStart(boolean resetState) {
        synchronized (loopLock) {
            state.set("REPLAYING");
            if (resetState) {
                orderReadModel.reset();
                deadLetterStore.reset();
                pendingOrphanStore.reset();
            }
            long offset = processAvailable(0L);
            currentOffset.set(offset);
            writeOffset(offset);
            replays.incrementAndGet();
            state.set("RUNNING");
            return new ReplayResult("REPLAYED", offset, processed.get(), skipped.get(), duplicates.get(), orphans.get());
        }
    }

    public RequeueResult requeuePending(String orderId) {
        synchronized (loopLock) {
            int reprocessed = replayPendingLocked(orderId);
            return new RequeueResult("REQUEUED", orderId, reprocessed, pendingOrphanStore.size());
        }
    }

    public ReconcileReport reconcile(String requestedAccountId) {
        List<OrderView> orders = orderReadModel.findAll().stream()
            .filter(order -> requestedAccountId == null || requestedAccountId.isBlank() || requestedAccountId.equals(order.accountId()))
            .sorted(Comparator.comparingLong(OrderView::submittedAt).reversed())
            .toList();
        long expectedReserved = 0L;
        long actualReserved = 0L;
        List<String> issues = new ArrayList<>();
        int openOrders = 0;
        for (OrderView order : orders) {
            if (!order.status().isTerminal()) {
                openOrders++;
            }
            long expected = expectedReservationAmount(order);
            long actual = orderReadModel.findReservationsByAccountId(order.accountId()).stream()
                .filter(reservation -> order.id().equals(reservation.orderId()))
                .mapToLong(ReservationView::reservedAmount)
                .sum();
            expectedReserved += expected;
            actualReserved += actual;
            if (expected != actual) {
                issues.add("reservation_mismatch:" + order.id() + ":expected=" + expected + ":actual=" + actual);
            }
            if (orderReadModel.findEventsByOrderId(order.id()).isEmpty()) {
                issues.add("missing_timeline:" + order.id());
            }
        }
        return new ReconcileReport(
            requestedAccountId,
            orders.size(),
            openOrders,
            expectedReserved,
            actualReserved,
            expectedReserved - actualReserved,
            issues
        );
    }

    private void runLoop() {
        synchronized (loopLock) {
            long initialOffset = readOffset();
            if (initialOffset == 0L && "tail".equals(startMode)) {
                initialOffset = currentAuditSize();
                writeOffset(initialOffset);
            }
            currentOffset.set(initialOffset);
        }

        while (true) {
            try {
                synchronized (loopLock) {
                    state.set("RUNNING");
                    long offset = processAvailable(currentOffset.get());
                    currentOffset.set(offset);
                    writeOffset(offset);
                }
                Thread.sleep(Math.max(50L, pollMs));
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                state.set("STOPPED");
                return;
            } catch (Exception exception) {
                state.set("ERROR");
                try {
                    Thread.sleep(Math.max(250L, pollMs));
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private long processAvailable(long startOffset) {
        if (!Files.exists(auditPath)) {
            return startOffset;
        }
        long offset = startOffset;
        long fileSize = currentAuditSize();
        if (offset > fileSize) {
            offset = 0L;
        }
        try (RandomAccessFile file = new RandomAccessFile(auditPath.toFile(), "r")) {
            file.seek(offset);
            String line;
            while ((line = file.readLine()) != null) {
                long nextOffset = file.getFilePointer();
                processLine(new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8));
                offset = nextOffset;
            }
            return offset;
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_tail_gateway_audit:" + auditPath, exception);
        }
    }

    private void processLine(String line) {
        if (line == null || line.isBlank()) {
            skipped.incrementAndGet();
            return;
        }
        GatewayAuditEvent event;
        try {
            event = OBJECT_MAPPER.readValue(line, GatewayAuditEvent.class);
        } catch (IOException exception) {
            skipped.incrementAndGet();
            deadLetterStore.append(deadLetter(
                buildEventRef(line),
                null,
                null,
                null,
                "INVALID_JSON",
                exception.getMessage(),
                line,
                0L
            ));
            return;
        }
        String eventRef = buildEventRef(line);
        if (event.orderId() != null && isDuplicate(event.orderId(), eventRef)) {
            duplicates.incrementAndGet();
            return;
        }

        handleEvent(event, eventRef, line, true);
        lastEventAt.accumulateAndGet(event.at(), Math::max);
    }

    private boolean handleEvent(GatewayAuditEvent event, String eventRef, String rawLine, boolean queueIfMissing) {
        return switch (event.type()) {
            case "OrderAccepted" -> {
                applyAccepted(event, eventRef);
                yield true;
            }
            case "ExecutionReport" -> applyExecutionReport(event, eventRef, rawLine, queueIfMissing);
            case "OrderUpdated" -> applyOrderUpdated(event, eventRef, rawLine, queueIfMissing);
            case "CancelRequested" -> applyPendingState(event, eventRef, rawLine, queueIfMissing, OrderStatus.CANCEL_PENDING, "CANCEL_REQUESTED", "取消要求");
            case "AmendRequested" -> applyAmendRequested(event, eventRef, rawLine, queueIfMissing);
            default -> {
                skipped.incrementAndGet();
                yield true;
            }
        };
    }

    private void applyAccepted(GatewayAuditEvent event, String eventRef) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, "MISSING_ORDER_ID", "orderId がありません");
            return;
        }
        JsonNode data = safeData(event.data());
        Optional<OrderView> currentOpt = orderReadModel.findById(event.orderId());
        int quantity = data.path("qty").asInt(currentOpt.map(OrderView::quantity).orElse(0));
        long filledQuantity = currentOpt.map(OrderView::filledQuantity).orElse(0L);
        Double price = data.hasNonNull("price") ? data.get("price").asDouble() : currentOpt.map(OrderView::price).orElse(null);
        Long expireAt = data.hasNonNull("expireAt") ? Long.valueOf(data.get("expireAt").asLong()) : currentOpt.map(OrderView::expireAt).orElse(null);
        OrderView next = new OrderView(
            event.orderId(),
            event.accountId(),
            textOr(data, "symbol", currentOpt.map(OrderView::symbol).orElse("UNKNOWN")),
            textOr(data, "side", currentOpt.map(OrderView::side).orElse("BUY")),
            textOr(data, "type", currentOpt.map(OrderView::type).orElse("MARKET")),
            quantity,
            price,
            textOr(data, "timeInForce", currentOpt.map(OrderView::timeInForce).orElse("GTC")),
            expireAt,
            OrderStatus.ACCEPTED,
            currentOpt.map(OrderView::submittedAt).orElse(event.at()),
            currentOpt.map(OrderView::filledAt).orElse(null),
            currentOpt.map(OrderView::executionTimeMs).orElse(null),
            currentOpt.map(OrderView::statusReason).orElse(null),
            filledQuantity,
            Math.max(0L, quantity - filledQuantity)
        );
        orderReadModel.upsert(next);
        appendEvent(next.id(), orderEvent(next.id(), "ORDER_ACCEPTED", event.at(), "OMS受付済", acceptedDetail(next), eventRef));
        syncReservation(next);
        replayPendingLocked(next.id());
        processed.incrementAndGet();
    }

    private boolean applyExecutionReport(GatewayAuditEvent event, String eventRef, String rawLine, boolean queueIfMissing) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderView current = orderReadModel.findById(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", "execution report の先に注文がありません");
        }
        JsonNode data = safeData(event.data());
        long filledTotal = data.path("filledQtyTotal").asLong(current.filledQuantity());
        long filledDelta = data.path("filledQtyDelta").asLong(Math.max(0L, filledTotal - current.filledQuantity()));
        String statusValue = textOr(data, "status", current.status().name());
        OrderStatus nextStatus = mapStatus(statusValue, current.status());
        OrderView next = new OrderView(
            current.id(),
            current.accountId(),
            current.symbol(),
            current.side(),
            current.type(),
            current.quantity(),
            current.price(),
            current.timeInForce(),
            current.expireAt(),
            nextStatus,
            current.submittedAt(),
            nextStatus == OrderStatus.FILLED ? Long.valueOf(event.at()) : current.filledAt(),
            current.executionTimeMs(),
            describeExecutionReason(nextStatus, filledDelta),
            filledTotal,
            Math.max(0L, current.quantity() - filledTotal)
        );
        orderReadModel.upsert(next);
        appendEvent(next.id(), orderEvent(
            next.id(),
            timelineType(nextStatus),
            event.at(),
            timelineLabel(nextStatus),
            executionDetail(next, filledDelta, data.hasNonNull("price") ? data.get("price").asDouble() : current.price()),
            eventRef
        ));
        syncReservation(next);
        processed.incrementAndGet();
        return true;
    }

    private boolean applyOrderUpdated(GatewayAuditEvent event, String eventRef, String rawLine, boolean queueIfMissing) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderView current = orderReadModel.findById(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", "order update の先に注文がありません");
        }
        JsonNode data = safeData(event.data());
        long filledTotal = data.path("filledQty").asLong(current.filledQuantity());
        OrderStatus nextStatus = mapStatus(textOr(data, "status", current.status().name()), current.status());
        OrderView next = new OrderView(
            current.id(),
            current.accountId(),
            current.symbol(),
            current.side(),
            current.type(),
            current.quantity(),
            current.price(),
            current.timeInForce(),
            current.expireAt(),
            nextStatus,
            current.submittedAt(),
            nextStatus == OrderStatus.FILLED ? Long.valueOf(event.at()) : current.filledAt(),
            current.executionTimeMs(),
            current.statusReason(),
            filledTotal,
            Math.max(0L, current.quantity() - filledTotal)
        );
        orderReadModel.upsert(next);
        appendEvent(next.id(), orderEvent(
            next.id(),
            "ORDER_UPDATED",
            event.at(),
            "注文状態更新",
            next.status().name() + " / 約定 " + next.filledQuantity() + "株",
            eventRef
        ));
        syncReservation(next);
        processed.incrementAndGet();
        return true;
    }

    private boolean applyPendingState(
        GatewayAuditEvent event,
        String eventRef,
        String rawLine,
        boolean queueIfMissing,
        OrderStatus nextStatus,
        String eventType,
        String label
    ) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderView current = orderReadModel.findById(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", "pending event の先に注文がありません");
        }
        OrderView next = new OrderView(
            current.id(),
            current.accountId(),
            current.symbol(),
            current.side(),
            current.type(),
            current.quantity(),
            current.price(),
            current.timeInForce(),
            current.expireAt(),
            nextStatus,
            current.submittedAt(),
            current.filledAt(),
            current.executionTimeMs(),
            current.statusReason(),
            current.filledQuantity(),
            current.remainingQuantity()
        );
        orderReadModel.upsert(next);
        appendEvent(next.id(), orderEvent(next.id(), eventType, event.at(), label, label + "を受理", eventRef));
        syncReservation(next);
        processed.incrementAndGet();
        return true;
    }

    private boolean applyAmendRequested(GatewayAuditEvent event, String eventRef, String rawLine, boolean queueIfMissing) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderView current = orderReadModel.findById(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", "amend event の先に注文がありません");
        }
        JsonNode data = safeData(event.data());
        int quantity = data.path("newQty").asInt(current.quantity());
        Double price = data.hasNonNull("newPrice") ? data.get("newPrice").asDouble() : current.price();
        OrderView next = new OrderView(
            current.id(),
            current.accountId(),
            current.symbol(),
            current.side(),
            current.type(),
            quantity,
            price,
            current.timeInForce(),
            current.expireAt(),
            OrderStatus.AMEND_PENDING,
            current.submittedAt(),
            current.filledAt(),
            current.executionTimeMs(),
            current.statusReason(),
            current.filledQuantity(),
            Math.max(0L, quantity - current.filledQuantity())
        );
        orderReadModel.upsert(next);
        appendEvent(next.id(), orderEvent(
            next.id(),
            "AMEND_REQUESTED",
            event.at(),
            "訂正要求",
            "数量 " + quantity + " / 価格 " + (price == null ? "-" : Math.round(price)),
            eventRef
        ));
        syncReservation(next);
        processed.incrementAndGet();
        return true;
    }

    private boolean isDuplicate(String orderId, String eventRef) {
        return orderReadModel.findEventsByOrderId(orderId).stream()
            .anyMatch(event -> eventRef.equals(event.eventRef()));
    }

    private void appendEvent(String orderId, OrderEventView event) {
        List<OrderEventView> events = new ArrayList<>(orderReadModel.findEventsByOrderId(orderId));
        events.add(event);
        events.sort(Comparator.comparingLong(OrderEventView::eventAt));
        orderReadModel.replaceEvents(orderId, events);
    }

    private void syncReservation(OrderView order) {
        List<ReservationView> reservations = new ArrayList<>(orderReadModel.findReservationsByAccountId(order.accountId()));
        reservations.removeIf(existing -> order.id().equals(existing.orderId()));
        if ("BUY".equalsIgnoreCase(order.side())) {
            long totalAmount = expectedReservationAmount(order);
            long reservedAmount = order.status().isTerminal() ? 0L : totalAmount;
            String status = order.status().isTerminal() ? "RELEASED" : order.status().name();
            reservations.add(new ReservationView(
                "resv-" + order.id(),
                order.accountId(),
                order.id(),
                order.symbol(),
                order.side(),
                order.remainingQuantity(),
                reservedAmount,
                Math.max(0L, totalAmount - reservedAmount),
                status,
                order.submittedAt(),
                order.status().isTerminal() && order.filledAt() != null ? order.filledAt() : Math.max(order.submittedAt(), lastEventAt.get())
            ));
        }
        reservations.sort(Comparator.comparingLong(ReservationView::updatedAt).reversed());
        orderReadModel.replaceReservations(order.accountId(), reservations);
    }

    private static long expectedReservationAmount(OrderView order) {
        if (!"BUY".equalsIgnoreCase(order.side()) || order.price() == null) {
            return 0L;
        }
        return Math.max(0L, order.remainingQuantity()) * Math.round(order.price());
    }

    private static OrderEventView orderEvent(
        String orderId,
        String eventType,
        long eventAt,
        String label,
        String detail,
        String eventRef
    ) {
        return new OrderEventView(orderId, eventType, eventAt, label, detail, "gateway-audit", eventRef);
    }

    private static OrderStatus mapStatus(String rawStatus, OrderStatus fallback) {
        if (rawStatus == null || rawStatus.isBlank()) {
            return fallback;
        }
        return switch (rawStatus.trim().toUpperCase()) {
            case "ACCEPTED", "SENT" -> OrderStatus.ACCEPTED;
            case "PARTIALLY_FILLED" -> OrderStatus.PARTIALLY_FILLED;
            case "FILLED" -> OrderStatus.FILLED;
            case "CANCELED", "CANCELLED" -> OrderStatus.CANCELED;
            case "REJECTED" -> OrderStatus.REJECTED;
            case "CANCEL_REQUESTED" -> OrderStatus.CANCEL_PENDING;
            case "AMEND_REQUESTED" -> OrderStatus.AMEND_PENDING;
            case "EXPIRED" -> OrderStatus.EXPIRED;
            default -> fallback;
        };
    }

    private static String timelineType(OrderStatus status) {
        return switch (status) {
            case PARTIALLY_FILLED -> "PARTIAL_FILL";
            case FILLED -> "FULL_FILL";
            case CANCELED -> "ORDER_CANCELED";
            case REJECTED -> "ORDER_REJECTED";
            case EXPIRED -> "ORDER_EXPIRED";
            default -> "EXECUTION_REPORT";
        };
    }

    private static String timelineLabel(OrderStatus status) {
        return switch (status) {
            case PARTIALLY_FILLED -> "一部約定";
            case FILLED -> "全量約定";
            case CANCELED -> "取消完了";
            case REJECTED -> "注文拒否";
            case EXPIRED -> "失効";
            default -> "約定通知";
        };
    }

    private static String acceptedDetail(OrderView order) {
        return order.symbol() + " " + order.side() + " " + order.quantity() + "株";
    }

    private static String executionDetail(OrderView order, long filledDelta, Double price) {
        if (order.status() == OrderStatus.PARTIALLY_FILLED || order.status() == OrderStatus.FILLED) {
            return filledDelta + "株 / 累計 " + order.filledQuantity() + "株 / 価格 " + (price == null ? "-" : Math.round(price));
        }
        return order.status().name() + " / 残 " + order.remainingQuantity() + "株";
    }

    private static String describeExecutionReason(OrderStatus status, long filledDelta) {
        if (status == OrderStatus.PARTIALLY_FILLED) {
            return "PARTIAL_FILL:" + filledDelta;
        }
        if (status == OrderStatus.FILLED) {
            return "FILLED";
        }
        if (status == OrderStatus.CANCELED) {
            return "CANCELED";
        }
        if (status == OrderStatus.REJECTED) {
            return "REJECTED";
        }
        return null;
    }

    private static JsonNode safeData(JsonNode data) {
        return data == null ? OBJECT_MAPPER.createObjectNode() : data;
    }

    private int replayPendingLocked(String orderId) {
        int reprocessed = 0;
        for (PendingOrphanEntryView entry : pendingOrphanStore.find(orderId, Integer.MAX_VALUE)) {
            GatewayAuditEvent event;
            try {
                event = OBJECT_MAPPER.readValue(entry.rawLine(), GatewayAuditEvent.class);
            } catch (IOException exception) {
                deadLetterStore.append(deadLetter(
                    entry.eventRef(),
                    entry.accountId(),
                    entry.orderId(),
                    entry.eventType(),
                    "PENDING_REPLAY_PARSE_FAILED",
                    exception.getMessage(),
                    entry.rawLine(),
                    entry.eventAt()
                ));
                pendingOrphanStore.removeByEventRef(entry.eventRef());
                continue;
            }
            if (handleEvent(event, entry.eventRef(), entry.rawLine(), false)) {
                pendingOrphanStore.removeByEventRef(entry.eventRef());
                reprocessed++;
            }
        }
        return reprocessed;
    }

    private boolean handleMissingOrder(
        GatewayAuditEvent event,
        String eventRef,
        String rawLine,
        boolean queueIfMissing,
        String reason,
        String detail
    ) {
        if (queueIfMissing) {
            orphans.incrementAndGet();
            pendingOrphanStore.append(new PendingOrphanEntryView(
                "oms-pending-" + eventRef,
                eventRef,
                event.accountId(),
                event.orderId(),
                event.type(),
                reason,
                rawLine,
                event.at(),
                System.currentTimeMillis(),
                "gateway-audit"
            ));
            return false;
        }
        return false;
    }

    private void appendOrphan(GatewayAuditEvent event, String eventRef, String reason, String detail) {
        orphans.incrementAndGet();
        deadLetterStore.append(deadLetter(
            eventRef,
            event.accountId(),
            event.orderId(),
            event.type(),
            reason,
            detail,
            safeData(event.data()).toString(),
            event.at()
        ));
    }

    private DeadLetterEntryView deadLetter(
        String eventRef,
        String accountId,
        String orderId,
        String eventType,
        String reason,
        String detail,
        String rawLine,
        long eventAt
    ) {
        return new DeadLetterEntryView(
            "oms-dlq-" + (eventRef == null ? System.nanoTime() : eventRef),
            eventRef,
            accountId,
            orderId,
            eventType,
            reason,
            detail,
            rawLine,
            eventAt,
            System.currentTimeMillis(),
            "gateway-audit"
        );
    }

    private static String textOr(JsonNode node, String fieldName, String fallback) {
        return node.hasNonNull(fieldName) ? node.get(fieldName).asText() : fallback;
    }

    private long currentAuditSize() {
        try {
            return Files.exists(auditPath) ? Files.size(auditPath) : 0L;
        } catch (IOException exception) {
            return 0L;
        }
    }

    private long readOffset() {
        try {
            if (!Files.exists(offsetPath)) {
                return 0L;
            }
            String raw = Files.readString(offsetPath).trim();
            if (raw.isEmpty()) {
                return 0L;
            }
            return Long.parseLong(raw);
        } catch (Exception exception) {
            return 0L;
        }
    }

    private void writeOffset(long offset) {
        try {
            Files.createDirectories(offsetPath.getParent());
            Files.writeString(offsetPath, Long.toString(offset), StandardCharsets.UTF_8);
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_write_oms_audit_offset:" + offsetPath, exception);
        }
    }

    private static boolean parseBoolean(String raw) {
        return raw != null && ("1".equals(raw) || "true".equalsIgnoreCase(raw));
    }

    private static Path resolvePath(String configured) {
        Path path = Path.of(configured);
        return path.isAbsolute() ? path : workspaceRoot().resolve(path).normalize();
    }

    private static Path workspaceRoot() {
        Path current = Path.of("").toAbsolutePath().normalize();
        while (current != null && !Files.exists(current.resolve("settings.gradle"))) {
            current = current.getParent();
        }
        return current != null ? current : Path.of("").toAbsolutePath().normalize();
    }

    private static String buildEventRef(String line) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return "audit-" + HexFormat.of().formatHex(digest.digest(line.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            return "audit-" + Integer.toHexString(line.hashCode());
        }
    }

    public record IntakeStatus(
        boolean enabled,
        String state,
        String auditPath,
        String offsetPath,
        String startMode,
        Instant startedAt,
        long processed,
        long skipped,
        long duplicates,
        long orphans,
        long replays,
        Long lastEventAt,
        long currentOffset,
        long currentAuditSize,
        int deadLetterCount,
        int pendingOrphanCount
    ) {
    }

    public record ReplayRequest(Boolean resetState) {
        public boolean shouldReset() {
            return resetState == null || resetState;
        }
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

    public record RequeueResult(
        String status,
        String orderId,
        int reprocessed,
        int pendingRemaining
    ) {
    }

    public record ReconcileReport(
        String accountId,
        int totalOrders,
        int openOrders,
        long expectedReservedAmount,
        long actualReservedAmount,
        long reservedGapAmount,
        List<String> issues
    ) {
    }
}
