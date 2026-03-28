package backofficejava.audit;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.AccountOverviewView;
import backofficejava.account.FillReadModel;
import backofficejava.account.FillView;
import backofficejava.account.LedgerEntryView;
import backofficejava.account.LedgerReadModel;
import backofficejava.account.OrderProjectionState;
import backofficejava.account.OrderProjectionStateStore;
import backofficejava.account.PositionReadModel;
import backofficejava.account.PositionView;
import backofficejava.bus.BusEventV2;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class GatewayAuditIntakeService {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private static final long DEFAULT_POLL_MS = 500L;
    private static final long INITIAL_CASH = 10_000_000L;

    private final AccountOverviewReadModel accountOverviewReadModel;
    private final PositionReadModel positionReadModel;
    private final FillReadModel fillReadModel;
    private final OrderProjectionStateStore orderStateStore;
    private final LedgerReadModel ledgerReadModel;
    private final DeadLetterStore deadLetterStore;
    private final PendingOrphanStore pendingOrphanStore;
    private final AggregateSequenceStore aggregateSequenceStore;
    private final boolean enabled;
    private final Path auditPath;
    private final AuditOffsetStore offsetStore;
    private final String startMode;
    private final long pollMs;
    private final Object loopLock = new Object();
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

    public GatewayAuditIntakeService(
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel,
        OrderProjectionStateStore orderStateStore,
        LedgerReadModel ledgerReadModel
    ) {
        this(
            accountOverviewReadModel,
            positionReadModel,
            fillReadModel,
            orderStateStore,
            ledgerReadModel,
            defaultOffsetStore(),
            new InMemoryDeadLetterStore(),
            new InMemoryPendingOrphanStore(),
            new InMemoryAggregateSequenceStore()
        );
    }

    public GatewayAuditIntakeService(
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel,
        OrderProjectionStateStore orderStateStore,
        LedgerReadModel ledgerReadModel,
        AuditOffsetStore offsetStore
    ) {
        this(
            accountOverviewReadModel,
            positionReadModel,
            fillReadModel,
            orderStateStore,
            ledgerReadModel,
            offsetStore,
            new InMemoryDeadLetterStore(),
            new InMemoryPendingOrphanStore(),
            new InMemoryAggregateSequenceStore()
        );
    }

    public GatewayAuditIntakeService(
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel,
        OrderProjectionStateStore orderStateStore,
        LedgerReadModel ledgerReadModel,
        AuditOffsetStore offsetStore,
        DeadLetterStore deadLetterStore,
        PendingOrphanStore pendingOrphanStore,
        AggregateSequenceStore aggregateSequenceStore
    ) {
        this.accountOverviewReadModel = accountOverviewReadModel;
        this.positionReadModel = positionReadModel;
        this.fillReadModel = fillReadModel;
        this.orderStateStore = orderStateStore;
        this.ledgerReadModel = ledgerReadModel;
        this.enabled = parseBoolean(
            System.getProperty(
                "backoffice.gateway.audit.enable",
                System.getenv().getOrDefault("BACKOFFICE_GATEWAY_AUDIT_ENABLE", "true")
            )
        );
        this.auditPath = resolvePath(
            System.getProperty(
                "backoffice.gateway.audit.path",
                System.getenv().getOrDefault("BACKOFFICE_GATEWAY_AUDIT_PATH", "var/gateway/audit.log")
            )
        );
        this.offsetStore = offsetStore;
        this.startMode = System.getProperty(
            "backoffice.gateway.audit.start.mode",
            System.getenv().getOrDefault("BACKOFFICE_GATEWAY_AUDIT_START_MODE", "tail")
        ).trim().toLowerCase();
        this.pollMs = Long.parseLong(
            System.getProperty(
                "backoffice.gateway.audit.poll.ms",
                System.getenv().getOrDefault("BACKOFFICE_GATEWAY_AUDIT_POLL_MS", String.valueOf(DEFAULT_POLL_MS))
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
        Thread worker = new Thread(this::runLoop, "backoffice-java-gateway-audit");
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
            ledgerReadModel.find(null, null, null, Integer.MAX_VALUE, null).size(),
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
                accountOverviewReadModel.reset();
                positionReadModel.reset();
                fillReadModel.reset();
                orderStateStore.reset();
                ledgerReadModel.reset();
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
            if (ledgerReadModel.containsEventRef(normalizedEventRef)) {
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
        String accountId = requestedAccountId == null || requestedAccountId.isBlank() ? "acct_demo" : requestedAccountId;
        AccountOverviewView overview = currentOverview(accountId);
        List<OrderProjectionState> orderStates = orderStateStore.findByAccountId(accountId);
        List<PositionView> positions = positionReadModel.findByAccountId(accountId);
        List<LedgerEntryView> ledgerEntries = ledgerReadModel.findByAccountId(accountId);
        long expectedReserved = orderStates.stream().mapToLong(OrderProjectionState::reservedAmount).sum();
        long expectedCash = INITIAL_CASH + ledgerEntries.stream().mapToLong(LedgerEntryView::cashDelta).sum();
        long expectedRealizedPnl = ledgerEntries.stream().mapToLong(LedgerEntryView::realizedPnlDelta).sum();
        List<String> issues = new ArrayList<>();
        if (overview.reservedBuyingPower() != expectedReserved) {
            issues.add("reserved_mismatch:expected=" + expectedReserved + ":actual=" + overview.reservedBuyingPower());
        }
        if (overview.cashBalance() != expectedCash) {
            issues.add("cash_mismatch:expected=" + expectedCash + ":actual=" + overview.cashBalance());
        }
        if (overview.realizedPnl() != expectedRealizedPnl) {
            issues.add("realized_pnl_mismatch:expected=" + expectedRealizedPnl + ":actual=" + overview.realizedPnl());
        }
        if (overview.positionCount() != positions.size()) {
            issues.add("position_count_mismatch:expected=" + positions.size() + ":actual=" + overview.positionCount());
        }
        return new ReconcileReport(
            accountId,
            overview.cashBalance(),
            overview.availableBuyingPower(),
            overview.reservedBuyingPower(),
            overview.realizedPnl(),
            expectedCash,
            expectedReserved,
            expectedRealizedPnl,
            positions,
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
            throw new IllegalStateException("failed_to_tail_backoffice_gateway_audit:" + auditPath, exception);
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
            case "CancelRequested" -> applyPendingState(event, eventRef, rawLine, queueIfMissing, "CANCEL_PENDING", "CANCEL_REQUESTED", "取消要求");
            case "AmendRequested" -> applyAmendRequested(event, eventRef, rawLine, queueIfMissing);
            case "CancelRejected" -> applyCancelRejected(event, eventRef, rawLine, queueIfMissing);
            case "AmendRejected" -> applyAmendRejected(event, eventRef, rawLine, queueIfMissing);
            case "OrderUpdated" -> applyOrderUpdated(event, eventRef, rawLine, queueIfMissing);
            case "CancelAccepted" -> applyTerminalStatus(event, eventRef, rawLine, queueIfMissing, "CANCELED", "ORDER_CANCELED", "取消完了");
            case "Expired" -> applyTerminalStatus(event, eventRef, rawLine, queueIfMissing, "EXPIRED", "ORDER_EXPIRED", "失効");
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
        if (orderStateStore.findByOrderId(event.orderId()).isPresent()) {
            duplicates.incrementAndGet();
            return;
        }
        long quantity = data.path("qty").asLong(0L);
        long workingPrice = data.hasNonNull("price") ? data.get("price").asLong() : 0L;
        String side = textOr(data, "side", "BUY");
        String symbol = textOr(data, "symbol", "UNKNOWN");
        long reservedAmount = "BUY".equalsIgnoreCase(side) ? quantity * workingPrice : 0L;
        orderStateStore.upsert(new OrderProjectionState(
            event.orderId(),
            event.accountId(),
            symbol,
            side,
            quantity,
            workingPrice,
            event.at(),
            event.at(),
            "ACCEPTED",
            0L,
            reservedAmount
        ));
        applyOverviewDelta(event.accountId(), 0L, reservedAmount, 0L);
        appendLedger(new LedgerEntryView(
            "ledger-" + event.orderId() + "-accepted",
            eventRef,
            event.accountId(),
            event.orderId(),
            "ORDER_ACCEPTED",
            symbol,
            side,
            0L,
            0L,
            reservedAmount,
            0L,
            symbol + " " + side + " " + quantity + "株 受付",
            event.at(),
            "gateway-audit"
        ));
        processed.incrementAndGet();
    }

    private boolean applyPendingState(
        GatewayAuditEvent event,
        String eventRef,
        String rawLine,
        boolean queueIfMissing,
        String nextStatus,
        String eventType,
        String detail
    ) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderProjectionState current = orderStateStore.findByOrderId(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", "pending event の先に注文がありません");
        }
        orderStateStore.upsert(new OrderProjectionState(
            current.orderId(),
            current.accountId(),
            current.symbol(),
            current.side(),
            current.quantity(),
            current.workingPrice(),
            current.submittedAt(),
            event.at(),
            nextStatus,
            current.filledQuantity(),
            current.reservedAmount()
        ));
        appendLedger(new LedgerEntryView(
            "ledger-" + current.orderId() + "-" + eventType.toLowerCase(),
            eventRef,
            current.accountId(),
            current.orderId(),
            eventType,
            current.symbol(),
            current.side(),
            0L,
            0L,
            0L,
            0L,
            detail,
            event.at(),
            "gateway-audit"
        ));
        processed.incrementAndGet();
        return true;
    }

    private boolean applyAmendRequested(GatewayAuditEvent event, String eventRef, String rawLine, boolean queueIfMissing) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderProjectionState current = orderStateStore.findByOrderId(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", "amend event の先に注文がありません");
        }
        JsonNode data = safeData(event.data());
        long quantity = data.path("newQty").asLong(current.quantity());
        long workingPrice = data.hasNonNull("newPrice") ? data.get("newPrice").asLong() : current.workingPrice();
        long remainingQuantity = Math.max(0L, quantity - current.filledQuantity());
        long nextReserved = "BUY".equalsIgnoreCase(current.side()) ? remainingQuantity * workingPrice : 0L;
        long reservedDelta = nextReserved - current.reservedAmount();
        orderStateStore.upsert(new OrderProjectionState(
            current.orderId(),
            current.accountId(),
            current.symbol(),
            current.side(),
            quantity,
            workingPrice,
            current.submittedAt(),
            event.at(),
            "AMEND_PENDING",
            current.filledQuantity(),
            nextReserved
        ));
        applyOverviewDelta(current.accountId(), 0L, reservedDelta, 0L);
        appendLedger(new LedgerEntryView(
            "ledger-" + current.orderId() + "-amend",
            eventRef,
            current.accountId(),
            current.orderId(),
            "AMEND_REQUESTED",
            current.symbol(),
            current.side(),
            0L,
            0L,
            reservedDelta,
            0L,
            "数量 " + quantity + " / 価格 " + workingPrice,
            event.at(),
            "gateway-audit"
        ));
        processed.incrementAndGet();
        return true;
    }

    private boolean applyCancelRejected(GatewayAuditEvent event, String eventRef, String rawLine, boolean queueIfMissing) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderProjectionState current = orderStateStore.findByOrderId(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", "cancel reject の先に注文がありません");
        }
        JsonNode data = safeData(event.data());
        String nextStatus = normalizedStatus(textOr(data, "status", current.status()));
        long filledQty = data.path("filledQty").asLong(current.filledQuantity());
        orderStateStore.upsert(new OrderProjectionState(
            current.orderId(),
            current.accountId(),
            current.symbol(),
            current.side(),
            current.quantity(),
            current.workingPrice(),
            current.submittedAt(),
            event.at(),
            nextStatus,
            Math.max(current.filledQuantity(), filledQty),
            current.reservedAmount()
        ));
        appendLedger(new LedgerEntryView(
            "ledger-" + current.orderId() + "-cancel-rejected",
            eventRef,
            current.accountId(),
            current.orderId(),
            "CANCEL_REJECTED",
            current.symbol(),
            current.side(),
            0L,
            0L,
            0L,
            0L,
            textOr(data, "reason", "取消拒否"),
            event.at(),
            "gateway-audit"
        ));
        processed.incrementAndGet();
        return true;
    }

    private boolean applyAmendRejected(GatewayAuditEvent event, String eventRef, String rawLine, boolean queueIfMissing) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderProjectionState current = orderStateStore.findByOrderId(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", "amend reject の先に注文がありません");
        }
        JsonNode data = safeData(event.data());
        long quantity = data.path("qty").asLong(current.quantity());
        long workingPrice = data.hasNonNull("price") ? data.get("price").asLong() : current.workingPrice();
        long filledQty = data.path("filledQty").asLong(current.filledQuantity());
        long remainingQty = Math.max(0L, quantity - filledQty);
        long nextReserved = "BUY".equalsIgnoreCase(current.side()) ? remainingQty * workingPrice : 0L;
        long reservedDelta = nextReserved - current.reservedAmount();
        orderStateStore.upsert(new OrderProjectionState(
            current.orderId(),
            current.accountId(),
            current.symbol(),
            current.side(),
            quantity,
            workingPrice,
            current.submittedAt(),
            event.at(),
            normalizedStatus(textOr(data, "status", current.status())),
            Math.max(current.filledQuantity(), filledQty),
            nextReserved
        ));
        applyOverviewDelta(current.accountId(), 0L, reservedDelta, 0L);
        appendLedger(new LedgerEntryView(
            "ledger-" + current.orderId() + "-amend-rejected",
            eventRef,
            current.accountId(),
            current.orderId(),
            "AMEND_REJECTED",
            current.symbol(),
            current.side(),
            0L,
            0L,
            reservedDelta,
            0L,
            textOr(data, "reason", "訂正拒否"),
            event.at(),
            "gateway-audit"
        ));
        processed.incrementAndGet();
        return true;
    }

    private boolean applyExecutionReport(GatewayAuditEvent event, String eventRef, String rawLine, boolean queueIfMissing) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderProjectionState current = orderStateStore.findByOrderId(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", "execution report の先に注文がありません");
        }
        JsonNode data = safeData(event.data());
        long filledTotal = data.path("filledQtyTotal").asLong(current.filledQuantity());
        if (filledTotal < current.filledQuantity()) {
            duplicates.incrementAndGet();
            return true;
        }
        long filledDelta = Math.max(0L, filledTotal - current.filledQuantity());
        String status = textOr(data, "status", current.status());
        long fillPrice = data.hasNonNull("price") ? Math.round(data.get("price").asDouble()) : current.workingPrice();
        long signedQuantityDelta = "BUY".equalsIgnoreCase(current.side()) ? filledDelta : -filledDelta;
        long cashDelta = "BUY".equalsIgnoreCase(current.side()) ? -(filledDelta * fillPrice) : filledDelta * fillPrice;

        PositionMutation positionMutation = updatePositions(current.accountId(), current.symbol(), current.side(), filledDelta, fillPrice);
        long realizedPnlDelta = positionMutation.realizedPnlDelta();

        long remainingQuantity = Math.max(0L, current.quantity() - filledTotal);
        long nextReserved = "BUY".equalsIgnoreCase(current.side())
            ? (isTerminalStatus(status) ? 0L : remainingQuantity * current.workingPrice())
            : 0L;
        long reservedDelta = nextReserved - current.reservedAmount();

        if (filledDelta > 0L) {
            appendFill(current, filledTotal, filledDelta, fillPrice, event.at());
        }
        applyOverviewDelta(current.accountId(), cashDelta, reservedDelta, realizedPnlDelta);
        orderStateStore.upsert(new OrderProjectionState(
            current.orderId(),
            current.accountId(),
            current.symbol(),
            current.side(),
            current.quantity(),
            current.workingPrice(),
            current.submittedAt(),
            event.at(),
            normalizedStatus(status),
            filledTotal,
            nextReserved
        ));
        appendLedger(new LedgerEntryView(
            "ledger-" + current.orderId() + "-" + event.at(),
            eventRef,
            current.accountId(),
            current.orderId(),
            normalizedEventType(status),
            current.symbol(),
            current.side(),
            signedQuantityDelta,
            cashDelta,
            reservedDelta,
            realizedPnlDelta,
            normalizedStatus(status) + " / delta=" + filledDelta + " / total=" + filledTotal,
            event.at(),
            "gateway-audit"
        ));
        processed.incrementAndGet();
        return true;
    }

    private boolean applyOrderUpdated(GatewayAuditEvent event, String eventRef, String rawLine, boolean queueIfMissing) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderProjectionState current = orderStateStore.findByOrderId(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", "order update の先に注文がありません");
        }
        JsonNode data = safeData(event.data());
        long filledTotal = data.path("filledQty").asLong(current.filledQuantity());
        String status = normalizedStatus(textOr(data, "status", current.status()));
        orderStateStore.upsert(new OrderProjectionState(
            current.orderId(),
            current.accountId(),
            current.symbol(),
            current.side(),
            current.quantity(),
            current.workingPrice(),
            current.submittedAt(),
            event.at(),
            status,
            Math.max(current.filledQuantity(), filledTotal),
            current.reservedAmount()
        ));
        appendLedger(new LedgerEntryView(
            "ledger-" + current.orderId() + "-updated-" + event.at(),
            eventRef,
            current.accountId(),
            current.orderId(),
            "ORDER_UPDATED",
            current.symbol(),
            current.side(),
            0L,
            0L,
            0L,
            0L,
            status + " / filled=" + filledTotal,
            event.at(),
            "gateway-audit"
        ));
        processed.incrementAndGet();
        return true;
    }

    private boolean applyTerminalStatus(
        GatewayAuditEvent event,
        String eventRef,
        String rawLine,
        boolean queueIfMissing,
        String nextStatus,
        String eventType,
        String detail
    ) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, rawLine, "MISSING_ORDER_ID", "orderId がありません");
            return false;
        }
        OrderProjectionState current = orderStateStore.findByOrderId(event.orderId()).orElse(null);
        if (current == null) {
            return handleMissingOrder(event, eventRef, rawLine, queueIfMissing, "ORDER_NOT_FOUND", detail + " の先に注文がありません");
        }
        JsonNode data = safeData(event.data());
        long filledQty = Math.max(current.filledQuantity(), data.path("filledQty").asLong(current.filledQuantity()));
        long remainingQuantity = Math.max(0L, current.quantity() - filledQty);
        long nextReserved = "BUY".equalsIgnoreCase(current.side()) && !isTerminalStatus(nextStatus)
            ? remainingQuantity * current.workingPrice()
            : 0L;
        long reservedDelta = nextReserved - current.reservedAmount();
        orderStateStore.upsert(new OrderProjectionState(
            current.orderId(),
            current.accountId(),
            current.symbol(),
            current.side(),
            current.quantity(),
            current.workingPrice(),
            current.submittedAt(),
            event.at(),
            normalizedStatus(nextStatus),
            filledQty,
            nextReserved
        ));
        applyOverviewDelta(current.accountId(), 0L, reservedDelta, 0L);
        appendLedger(new LedgerEntryView(
            "ledger-" + current.orderId() + "-" + eventType.toLowerCase() + "-" + event.at(),
            eventRef,
            current.accountId(),
            current.orderId(),
            eventType,
            current.symbol(),
            current.side(),
            0L,
            0L,
            reservedDelta,
            0L,
            textOr(data, "reason", detail),
            event.at(),
            "gateway-audit"
        ));
        processed.incrementAndGet();
        return true;
    }

    private PositionMutation updatePositions(String accountId, String symbol, String side, long deltaQty, long fillPrice) {
        if (deltaQty <= 0L) {
            return new PositionMutation(positionReadModel.findByAccountId(accountId), 0L);
        }
        List<PositionView> positions = new ArrayList<>(positionReadModel.findByAccountId(accountId));
        PositionView current = positions.stream()
            .filter(position -> symbol.equals(position.symbol()))
            .findFirst()
            .orElse(null);
        positions.removeIf(position -> symbol.equals(position.symbol()));
        long realizedPnlDelta = 0L;
        if ("BUY".equalsIgnoreCase(side)) {
            long currentQty = current == null ? 0L : current.netQty();
            double currentAvg = current == null ? 0.0 : current.avgPrice();
            long nextQty = currentQty + deltaQty;
            double nextAvg = nextQty > 0 ? ((currentQty * currentAvg) + (deltaQty * fillPrice)) / nextQty : 0.0;
            positions.add(new PositionView(accountId, symbol, nextQty, round2(nextAvg)));
        } else {
            long currentQty = current == null ? 0L : current.netQty();
            double currentAvg = current == null ? 0.0 : current.avgPrice();
            long nextQty = Math.max(0L, currentQty - deltaQty);
            realizedPnlDelta = Math.round((fillPrice - currentAvg) * Math.min(currentQty, deltaQty));
            if (nextQty > 0L) {
                positions.add(new PositionView(accountId, symbol, nextQty, round2(currentAvg)));
            }
        }
        positions.sort(Comparator.comparing(PositionView::symbol));
        positionReadModel.replacePositions(accountId, positions);
        return new PositionMutation(positions, realizedPnlDelta);
    }

    private void appendFill(OrderProjectionState orderState, long filledTotal, long filledDelta, long fillPrice, long filledAt) {
        List<FillView> fills = new ArrayList<>(fillReadModel.findByOrderId(orderState.orderId()));
        fills.add(new FillView(
            "fill-" + orderState.orderId() + "-" + filledTotal,
            orderState.orderId(),
            orderState.accountId(),
            orderState.symbol(),
            orderState.side(),
            filledDelta,
            fillPrice,
            filledDelta * fillPrice,
            "TAKER",
            filledAt
        ));
        fills.sort(Comparator.comparingLong(FillView::filledAt));
        fillReadModel.replaceFills(orderState.orderId(), fills);
    }

    private void applyOverviewDelta(String accountId, long cashDelta, long reservedDelta, long realizedPnlDelta) {
        AccountOverviewView current = currentOverview(accountId);
        long nextCash = current.cashBalance() + cashDelta;
        long nextReserved = Math.max(0L, current.reservedBuyingPower() + reservedDelta);
        int positionCount = positionReadModel.findByAccountId(accountId).size();
        accountOverviewReadModel.upsert(new AccountOverviewView(
            accountId,
            nextCash,
            nextCash - nextReserved,
            nextReserved,
            positionCount,
            current.realizedPnl() + realizedPnlDelta,
            Instant.ofEpochMilli(Math.max(System.currentTimeMillis(), lastEventAt.get()))
        ));
    }

    private AccountOverviewView currentOverview(String accountId) {
        return accountOverviewReadModel.findByAccountId(accountId).orElseGet(() ->
            new AccountOverviewView(accountId, INITIAL_CASH, INITIAL_CASH, 0L, 0, 0L, Instant.now())
        );
    }

    private void appendLedger(LedgerEntryView entry) {
        if (!ledgerReadModel.containsEventRef(entry.eventRef())) {
            ledgerReadModel.append(entry);
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
                "backoffice-pending-" + eventRef,
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
            "backoffice-dlq-" + (eventRef == null ? System.nanoTime() : eventRef),
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
        if (ledgerReadModel.containsEventRef(normalizedEventRef)) {
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
            if (queueIfMissing && envelope.event().orderId() != null) {
                replayPendingLocked(envelope.event().orderId());
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
                "backoffice-pending-" + eventRef,
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

    private static boolean isTerminalStatus(String status) {
        String normalized = normalizedStatus(status);
        return "FILLED".equals(normalized) || "CANCELED".equals(normalized) || "REJECTED".equals(normalized) || "EXPIRED".equals(normalized);
    }

    private static String normalizedStatus(String status) {
        if (status == null) {
            return "ACCEPTED";
        }
        return switch (status.trim().toUpperCase()) {
            case "SENT", "ACCEPTED" -> "ACCEPTED";
            case "CANCELLED" -> "CANCELED";
            default -> status.trim().toUpperCase();
        };
    }

    private static String normalizedEventType(String status) {
        return switch (normalizedStatus(status)) {
            case "PARTIALLY_FILLED" -> "PARTIAL_FILL";
            case "FILLED" -> "FULL_FILL";
            case "CANCELED" -> "ORDER_CANCELED";
            case "REJECTED" -> "ORDER_REJECTED";
            default -> "EXECUTION_REPORT";
        };
    }

    private static boolean parseBoolean(String raw) {
        return raw != null && ("1".equals(raw) || "true".equalsIgnoreCase(raw));
    }

    private static Path resolvePath(String configured) {
        Path path = Path.of(configured);
        return path.isAbsolute() ? path : workspaceRoot().resolve(path).normalize();
    }

    private static AuditOffsetStore defaultOffsetStore() {
        Path offsetPath = resolvePath(
            System.getProperty(
                "backoffice.gateway.audit.offset.path",
                System.getenv().getOrDefault("BACKOFFICE_GATEWAY_AUDIT_OFFSET_PATH", "var/java-replay/backoffice/audit.offset")
            )
        );
        return new FileAuditOffsetStore(offsetPath);
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

    private static long parseEpochMillis(String occurredAt, long fallback) {
        if (occurredAt == null || occurredAt.isBlank()) {
            return fallback;
        }
        return Instant.parse(occurredAt).toEpochMilli();
    }

    private static String firstNonBlank(String primary, String fallback) {
        return primary != null && !primary.isBlank() ? primary : fallback;
    }

    private static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
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
        int ledgerEntryCount,
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
        long cashBalance,
        long availableBuyingPower,
        long reservedBuyingPower,
        long realizedPnl,
        long expectedCashBalance,
        long expectedReservedBuyingPower,
        long expectedRealizedPnl,
        List<PositionView> positions,
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

    private record PositionMutation(List<PositionView> positions, long realizedPnlDelta) {
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
