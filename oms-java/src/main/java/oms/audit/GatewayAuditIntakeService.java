package oms.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import oms.bus.BusEventV2;
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
    private final AuditOffsetStore offsetStore;
    private final String startMode;
    private final long pollMs;
    private final Object loopLock = new Object();
    private final DeadLetterStore deadLetterStore;
    private final PendingOrphanStore pendingOrphanStore;
    private final AggregateSequenceStore aggregateSequenceStore;
    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong skipped = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong orphans = new AtomicLong();
    private final AtomicLong sequenceGaps = new AtomicLong();
    private final AtomicLong replays = new AtomicLong();
    private final AtomicLong currentOffset = new AtomicLong();
    private final AtomicLong lastEventAt = new AtomicLong();
    private final AtomicReference<String> state = new AtomicReference<>("IDLE");
    private final Instant startedAt = Instant.now();

    public GatewayAuditIntakeService(OrderReadModel orderReadModel) {
        this(
            orderReadModel,
            defaultOffsetStore(),
            new InMemoryDeadLetterStore(),
            new InMemoryPendingOrphanStore(),
            new InMemoryAggregateSequenceStore()
        );
    }

    public GatewayAuditIntakeService(OrderReadModel orderReadModel, AuditOffsetStore offsetStore) {
        this(
            orderReadModel,
            offsetStore,
            new InMemoryDeadLetterStore(),
            new InMemoryPendingOrphanStore(),
            new InMemoryAggregateSequenceStore()
        );
    }

    public GatewayAuditIntakeService(
        OrderReadModel orderReadModel,
        AuditOffsetStore offsetStore,
        DeadLetterStore deadLetterStore,
        PendingOrphanStore pendingOrphanStore,
        AggregateSequenceStore aggregateSequenceStore
    ) {
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
        this.offsetStore = offsetStore;
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
        this.deadLetterStore = deadLetterStore;
        this.pendingOrphanStore = pendingOrphanStore;
        this.aggregateSequenceStore = aggregateSequenceStore;
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
            offsetStore.describe(),
            startMode,
            startedAt,
            processed.get(),
            skipped.get(),
            duplicates.get(),
            orphans.get(),
            sequenceGaps.get(),
            replays.get(),
            lastEventAt.get() == 0 ? null : lastEventAt.get(),
            currentOffset.get(),
            currentAuditSize(),
            deadLetterStore.size(),
            pendingOrphanStore.size(),
            aggregateSequenceStore.size()
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
                aggregateSequenceStore.reset();
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

    public DeadLetterRequeueResult requeueDeadLetter(String eventRef) {
        synchronized (loopLock) {
            DeadLetterEntryView entry = deadLetterStore.findByEventRef(eventRef);
            if (entry == null) {
                return new DeadLetterRequeueResult("NOT_FOUND", eventRef, "NOT_FOUND", pendingOrphanStore.size(), deadLetterStore.size());
            }
            DecodedEnvelope envelope;
            try {
                envelope = decodeStoredEnvelope(
                    entry.eventRef(),
                    entry.rawLine(),
                    entry.accountId(),
                    entry.orderId(),
                    entry.eventType(),
                    entry.eventAt(),
                    entry.source()
                );
            } catch (IOException exception) {
                deadLetterStore.append(deadLetter(
                    entry.eventRef(),
                    entry.accountId(),
                    entry.orderId(),
                    entry.eventType(),
                    "DLQ_REPLAY_PARSE_FAILED",
                    exception.getMessage(),
                    entry.rawLine(),
                    entry.eventAt()
                ));
                return new DeadLetterRequeueResult("FAILED", eventRef, "PARSE_FAILED", pendingOrphanStore.size(), deadLetterStore.size());
            }
            IngestResult result = ingestDecodedEnvelope(envelope, true);
            boolean queuedPending = envelope.event().orderId() != null && pendingOrphanStore.find(envelope.event().orderId(), Integer.MAX_VALUE).stream()
                .anyMatch(candidate -> eventRef.equals(candidate.eventRef()));
            if (result.applied() || queuedPending || "DUPLICATE".equals(result.status())) {
                deadLetterStore.removeByEventRef(eventRef);
            }
            return new DeadLetterRequeueResult(
                result.applied() || queuedPending || "DUPLICATE".equals(result.status()) ? "REQUEUED" : "UNCHANGED",
                eventRef,
                result.applied() ? "REPROCESSED" : queuedPending ? "PENDING" : "DUPLICATE".equals(result.status()) ? "DUPLICATE" : "STILL_DLQ",
                pendingOrphanStore.size(),
                deadLetterStore.size()
            );
        }
    }

    public IngestResult ingestEvent(GatewayAuditEvent event, String eventRef, String rawLine) {
        return ingestEventInternal(event, eventRef, rawLine, null);
    }

    public IngestResult ingestSequencedEvent(
        GatewayAuditEvent event,
        String eventRef,
        String rawLine,
        String aggregateId,
        long aggregateSeq
    ) {
        return ingestEventInternal(event, eventRef, rawLine, new SequenceContext(aggregateId, aggregateSeq, "gateway-bus"));
    }

    private IngestResult ingestEventInternal(
        GatewayAuditEvent event,
        String eventRef,
        String rawLine,
        SequenceContext sequenceContext
    ) {
        synchronized (loopLock) {
            String normalizedEventRef = eventRef == null || eventRef.isBlank() ? "event-" + System.nanoTime() : eventRef;
            String normalizedRawLine = rawLine == null || rawLine.isBlank() ? serializeEvent(event) : rawLine;
            if (sequenceContext != null) {
                IngestResult sequenceResult = validateSequence(event, normalizedEventRef, normalizedRawLine, sequenceContext);
                if (sequenceResult != null) {
                    lastEventAt.accumulateAndGet(event.at(), Math::max);
                    return sequenceResult;
                }
            }
            if (event.orderId() != null && isDuplicate(event.orderId(), normalizedEventRef)) {
                duplicates.incrementAndGet();
                return new IngestResult("DUPLICATE", normalizedEventRef, event.orderId(), false);
            }
            boolean applied = handleEvent(event, normalizedEventRef, normalizedRawLine, true);
            lastEventAt.accumulateAndGet(event.at(), Math::max);
            if (applied) {
                if (sequenceContext != null) {
                    aggregateSequenceStore.markApplied(sequenceContext.aggregateId(), sequenceContext.aggregateSeq(), normalizedEventRef, event.at());
                }
                if (event.orderId() != null) {
                    replayPendingLocked(event.orderId());
                }
                return new IngestResult("APPLIED", normalizedEventRef, event.orderId(), true);
            }
            if (deadLetterStore.findByEventRef(normalizedEventRef) != null) {
                return new IngestResult("DLQ", normalizedEventRef, event.orderId(), false);
            }
            return new IngestResult("PENDING", normalizedEventRef, event.orderId(), false);
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
                .mapToLong(reservation -> Math.max(0L, reservation.reservedAmount() - reservation.releasedAmount()))
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
                exception.printStackTrace(System.err);
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
        ingestEvent(event, buildEventRef(line), line);
    }

    private boolean handleEvent(GatewayAuditEvent event, String eventRef, String rawLine, boolean queueIfMissing) {
        return switch (event.type()) {
            case "OrderAccepted" -> {
                applyAccepted(event, eventRef, rawLine);
                yield true;
            }
            case "ExecutionReport" -> applyExecutionReport(event, eventRef, rawLine, queueIfMissing);
            case "OrderUpdated" -> applyOrderUpdated(event, eventRef, rawLine, queueIfMissing);
            case "CancelRequested" -> applyPendingState(event, eventRef, rawLine, queueIfMissing, OrderStatus.CANCEL_PENDING, "CANCEL_REQUESTED", "取消要求");
            case "AmendRequested" -> applyAmendRequested(event, eventRef, rawLine, queueIfMissing);
            case "CancelRejected" -> applyCancelRejected(event, eventRef, rawLine, queueIfMissing);
            case "AmendRejected" -> applyAmendRejected(event, eventRef, rawLine, queueIfMissing);
            case "CancelAccepted" -> applyTerminalStatus(event, eventRef, rawLine, queueIfMissing, OrderStatus.CANCELED, "CANCEL_ACCEPTED", "取消完了");
            case "Expired" -> applyTerminalStatus(event, eventRef, rawLine, queueIfMissing, OrderStatus.EXPIRED, "ORDER_EXPIRED", "失効");
            default -> {
                skipped.incrementAndGet();
                yield true;
            }
        };
    }

    private void applyAccepted(GatewayAuditEvent event, String eventRef, String rawLine) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
            return;
        }
        JsonNode data = safeData(event.data());
        Optional<OrderView> currentOpt = orderReadModel.findById(event.orderId());
        int quantity = data.path("qty").asInt(currentOpt.map(OrderView::quantity).orElse(0));
        long filledQuantity = currentOpt.map(OrderView::filledQuantity).orElse(0L);
        Double price = data.hasNonNull("price") ? Double.valueOf(data.get("price").asDouble()) : currentOpt.map(OrderView::price).orElse(null);
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
        processed.incrementAndGet();
    }

    private boolean applyExecutionReport(GatewayAuditEvent event, String eventRef, String rawLine, boolean queueIfMissing) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
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
            executionDetail(next, filledDelta, data.hasNonNull("price") ? Double.valueOf(data.get("price").asDouble()) : current.price()),
            eventRef
        ));
        syncReservation(next);
        processed.incrementAndGet();
        return true;
    }

    private boolean applyOrderUpdated(GatewayAuditEvent event, String eventRef, String rawLine, boolean queueIfMissing) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
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
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
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
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderView current = orderReadModel.findById(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", "amend event の先に注文がありません");
        }
        JsonNode data = safeData(event.data());
        int quantity = data.path("newQty").asInt(current.quantity());
        Double price = data.hasNonNull("newPrice") ? Double.valueOf(data.get("newPrice").asDouble()) : current.price();
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

    private boolean applyCancelRejected(GatewayAuditEvent event, String eventRef, String rawLine, boolean queueIfMissing) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderView current = orderReadModel.findById(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", "cancel reject の先に注文がありません");
        }
        JsonNode data = safeData(event.data());
        OrderStatus nextStatus = mapStatus(textOr(data, "status", current.status().name()), current.status());
        long filledQty = data.path("filledQty").asLong(current.filledQuantity());
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
            textOr(data, "reason", current.statusReason()),
            filledQty,
            Math.max(0L, current.quantity() - filledQty)
        );
        orderReadModel.upsert(next);
        appendEvent(next.id(), orderEvent(
            next.id(),
            "CANCEL_REJECTED",
            event.at(),
            "取消拒否",
            textOr(data, "reason", "取消拒否"),
            eventRef
        ));
        syncReservation(next);
        processed.incrementAndGet();
        return true;
    }

    private boolean applyAmendRejected(GatewayAuditEvent event, String eventRef, String rawLine, boolean queueIfMissing) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderView current = orderReadModel.findById(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", "amend reject の先に注文がありません");
        }
        JsonNode data = safeData(event.data());
        int quantity = data.path("qty").asInt(current.quantity());
        Double price = data.hasNonNull("price") ? Double.valueOf(data.get("price").asDouble()) : current.price();
        OrderStatus nextStatus = mapStatus(textOr(data, "status", current.status().name()), current.status());
        long filledQty = data.path("filledQty").asLong(current.filledQuantity());
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
            nextStatus,
            current.submittedAt(),
            current.filledAt(),
            current.executionTimeMs(),
            textOr(data, "reason", current.statusReason()),
            filledQty,
            Math.max(0L, quantity - filledQty)
        );
        orderReadModel.upsert(next);
        appendEvent(next.id(), orderEvent(
            next.id(),
            "AMEND_REJECTED",
            event.at(),
            "訂正拒否",
            textOr(data, "reason", "訂正拒否"),
            eventRef
        ));
        syncReservation(next);
        processed.incrementAndGet();
        return true;
    }

    private boolean applyTerminalStatus(
        GatewayAuditEvent event,
        String eventRef,
        String rawLine,
        boolean queueIfMissing,
        OrderStatus nextStatus,
        String timelineType,
        String label
    ) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderView current = orderReadModel.findById(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", label + " の先に注文がありません");
        }
        JsonNode data = safeData(event.data());
        long filledQty = data.path("filledQty").asLong(current.filledQuantity());
        long normalizedFilledQty = Math.max(current.filledQuantity(), filledQty);
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
            textOr(data, "reason", current.statusReason()),
            normalizedFilledQty,
            Math.max(0L, current.quantity() - normalizedFilledQty)
        );
        orderReadModel.upsert(next);
        appendEvent(next.id(), orderEvent(
            next.id(),
            timelineType,
            event.at(),
            label,
            textOr(data, "reason", label),
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
        boolean advanced;
        do {
            advanced = false;
            for (PendingOrphanEntryView entry : pendingOrphanStore.find(orderId, Integer.MAX_VALUE)) {
                DecodedEnvelope envelope;
                try {
                    envelope = decodeStoredEnvelope(
                        entry.eventRef(),
                        entry.rawLine(),
                        entry.accountId(),
                        entry.orderId(),
                        entry.eventType(),
                        entry.eventAt(),
                        entry.source()
                    );
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
                IngestResult result = ingestDecodedEnvelope(envelope, false);
                if (result.applied() || "DUPLICATE".equals(result.status()) || "DLQ".equals(result.status())) {
                    pendingOrphanStore.removeByEventRef(entry.eventRef());
                    if (result.applied()) {
                        reprocessed++;
                        advanced = true;
                    }
                }
            }
        } while (advanced);
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

    private void appendOrphan(GatewayAuditEvent event, String eventRef, String rawLine, String reason, String detail) {
        orphans.incrementAndGet();
        deadLetterStore.append(deadLetter(
            eventRef,
            event.accountId(),
            event.orderId(),
            event.type(),
            reason,
            detail,
            rawLine == null || rawLine.isBlank() ? serializeEvent(event) : rawLine,
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

    private IngestResult ingestDecodedEnvelope(DecodedEnvelope envelope, boolean queueIfMissing) {
        String normalizedEventRef = envelope.eventRef() == null || envelope.eventRef().isBlank()
            ? "event-" + System.nanoTime()
            : envelope.eventRef();
        if (envelope.sequenceContext() != null) {
            IngestResult sequenceResult = validateSequence(
                envelope.event(),
                normalizedEventRef,
                envelope.rawLine(),
                envelope.sequenceContext()
            );
            if (sequenceResult != null) {
                lastEventAt.accumulateAndGet(envelope.event().at(), Math::max);
                return sequenceResult;
            }
        }
        if (envelope.event().orderId() != null && isDuplicate(envelope.event().orderId(), normalizedEventRef)) {
            duplicates.incrementAndGet();
            return new IngestResult("DUPLICATE", normalizedEventRef, envelope.event().orderId(), false);
        }
        boolean applied = handleEvent(envelope.event(), normalizedEventRef, envelope.rawLine(), queueIfMissing);
        lastEventAt.accumulateAndGet(envelope.event().at(), Math::max);
        if (applied) {
            if (envelope.sequenceContext() != null) {
                aggregateSequenceStore.markApplied(
                    envelope.sequenceContext().aggregateId(),
                    envelope.sequenceContext().aggregateSeq(),
                    normalizedEventRef,
                    envelope.event().at()
                );
            }
            return new IngestResult("APPLIED", normalizedEventRef, envelope.event().orderId(), true);
        }
        if (deadLetterStore.findByEventRef(normalizedEventRef) != null) {
            return new IngestResult("DLQ", normalizedEventRef, envelope.event().orderId(), false);
        }
        return new IngestResult("PENDING", normalizedEventRef, envelope.event().orderId(), false);
    }

    private IngestResult validateSequence(
        GatewayAuditEvent event,
        String eventRef,
        String rawLine,
        SequenceContext sequenceContext
    ) {
        if (sequenceContext.aggregateId() == null || sequenceContext.aggregateId().isBlank() || sequenceContext.aggregateSeq() <= 0L) {
            deadLetterStore.append(deadLetter(
                eventRef,
                event.accountId(),
                event.orderId(),
                event.type(),
                "INVALID_AGGREGATE_SEQUENCE",
                "aggregateId または aggregateSeq が不正です",
                rawLine,
                event.at()
            ));
            return new IngestResult("DLQ", eventRef, event.orderId(), false);
        }
        long lastApplied = aggregateSequenceStore.readLastApplied(sequenceContext.aggregateId());
        if (sequenceContext.aggregateSeq() <= lastApplied) {
            duplicates.incrementAndGet();
            return new IngestResult("DUPLICATE", eventRef, event.orderId(), false);
        }
        if (sequenceContext.aggregateSeq() > lastApplied + 1L) {
            sequenceGaps.incrementAndGet();
            pendingOrphanStore.append(new PendingOrphanEntryView(
                "oms-pending-" + eventRef,
                eventRef,
                event.accountId(),
                event.orderId(),
                event.type(),
                "SEQUENCE_GAP_EXPECTED_" + (lastApplied + 1L) + "_GOT_" + sequenceContext.aggregateSeq(),
                rawLine,
                event.at(),
                System.currentTimeMillis(),
                sequenceContext.source()
            ));
            return new IngestResult("PENDING", eventRef, event.orderId(), false);
        }
        return null;
    }

    private DecodedEnvelope decodeStoredEnvelope(
        String eventRef,
        String rawLine,
        String fallbackAccountId,
        String fallbackOrderId,
        String fallbackEventType,
        long fallbackEventAt,
        String fallbackSource
    ) throws IOException {
        JsonNode root = OBJECT_MAPPER.readTree(rawLine);
        if (root.has("schemaVersion") || root.has("aggregateSeq")) {
            BusEventV2 busEvent = OBJECT_MAPPER.treeToValue(root, BusEventV2.class);
            GatewayAuditEvent event = new GatewayAuditEvent(
                firstNonBlank(busEvent.eventType(), fallbackEventType),
                parseEpochMillis(busEvent.occurredAt(), fallbackEventAt),
                firstNonBlank(busEvent.accountId(), fallbackAccountId),
                firstNonBlank(busEvent.orderId(), firstNonBlank(busEvent.aggregateId(), fallbackOrderId)),
                busEvent.data()
            );
            return new DecodedEnvelope(
                event,
                eventRef,
                rawLine,
                new SequenceContext(
                    firstNonBlank(busEvent.aggregateId(), event.orderId()),
                    busEvent.aggregateSeq(),
                    firstNonBlank(fallbackSource, "gateway-bus")
                )
            );
        }
        GatewayAuditEvent event = OBJECT_MAPPER.treeToValue(root, GatewayAuditEvent.class);
        return new DecodedEnvelope(event, eventRef, rawLine, null);
    }

    private static String textOr(JsonNode node, String fieldName, String fallback) {
        return node.hasNonNull(fieldName) ? node.get(fieldName).asText() : fallback;
    }

    private static String serializeEvent(GatewayAuditEvent event) {
        try {
            return OBJECT_MAPPER.writeValueAsString(event);
        } catch (Exception exception) {
            return safeData(event.data()).toString();
        }
    }

    private long currentAuditSize() {
        try {
            return Files.exists(auditPath) ? Files.size(auditPath) : 0L;
        } catch (IOException exception) {
            return 0L;
        }
    }

    private long readOffset() {
        return offsetStore.readOffset();
    }

    private void writeOffset(long offset) {
        offsetStore.writeOffset(offset);
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

    private static AuditOffsetStore defaultOffsetStore() {
        Path offsetPath = resolvePath(
            System.getProperty(
                "oms.gateway.audit.offset.path",
                System.getenv().getOrDefault("OMS_GATEWAY_AUDIT_OFFSET_PATH", "var/java-replay/oms/audit.offset")
            )
        );
        return new FileAuditOffsetStore(offsetPath);
    }

    private static String buildEventRef(String line) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return "audit-" + HexFormat.of().formatHex(digest.digest(line.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            return "audit-" + Integer.toHexString(line.hashCode());
        }
    }

    private static long parseEpochMillis(String occurredAt, long fallback) {
        if (occurredAt == null || occurredAt.isBlank()) {
            return fallback;
        }
        return Instant.parse(occurredAt).toEpochMilli();
    }

    private static String firstNonBlank(String primary, String fallback) {
        return primary != null && !primary.isBlank() ? primary : fallback;
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
        long sequenceGaps,
        long replays,
        Long lastEventAt,
        long currentOffset,
        long currentAuditSize,
        int deadLetterCount,
        int pendingOrphanCount,
        int aggregateProgressCount
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

    public record DeadLetterRequeueResult(
        String status,
        String eventRef,
        String outcome,
        int pendingRemaining,
        int deadLetterRemaining
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

    public record IngestResult(
        String status,
        String eventRef,
        String orderId,
        boolean applied
    ) {
    }

    private record SequenceContext(String aggregateId, long aggregateSeq, String source) {
    }

    private record DecodedEnvelope(
        GatewayAuditEvent event,
        String eventRef,
        String rawLine,
        SequenceContext sequenceContext
    ) {
    }
}
