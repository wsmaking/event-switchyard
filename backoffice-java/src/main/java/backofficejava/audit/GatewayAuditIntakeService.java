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
    private final InMemoryDeadLetterStore deadLetterStore;
    private final boolean enabled;
    private final Path auditPath;
    private final Path offsetPath;
    private final String startMode;
    private final long pollMs;
    private final Object loopLock = new Object();
    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong skipped = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong orphans = new AtomicLong();
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
        this.offsetPath = resolvePath(
            System.getProperty(
                "backoffice.gateway.audit.offset.path",
                System.getenv().getOrDefault("BACKOFFICE_GATEWAY_AUDIT_OFFSET_PATH", "var/java-replay/backoffice/audit.offset")
            )
        );
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
        this.deadLetterStore = new InMemoryDeadLetterStore();
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
            ledgerReadModel.find(null, null, null, Integer.MAX_VALUE, null).size(),
            deadLetterStore.size()
        );
    }

    public List<DeadLetterEntryView> findDeadLetters(String orderId, int limit) {
        return deadLetterStore.find(orderId, limit);
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
            }
            long offset = processAvailable(0L);
            currentOffset.set(offset);
            writeOffset(offset);
            replays.incrementAndGet();
            state.set("RUNNING");
            return new ReplayResult("REPLAYED", offset, processed.get(), skipped.get(), duplicates.get(), orphans.get());
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
        String eventRef = buildEventRef(line);
        if (ledgerReadModel.containsEventRef(eventRef)) {
            duplicates.incrementAndGet();
            return;
        }

        switch (event.type()) {
            case "OrderAccepted" -> applyAccepted(event, eventRef);
            case "ExecutionReport" -> applyExecutionReport(event, eventRef);
            case "CancelRequested" -> applyPendingState(event, eventRef, "CANCEL_PENDING", "CANCEL_REQUESTED", "取消要求");
            case "AmendRequested" -> applyAmendRequested(event, eventRef);
            case "OrderUpdated" -> applyOrderUpdated(event, eventRef);
            default -> skipped.incrementAndGet();
        }
        lastEventAt.accumulateAndGet(event.at(), Math::max);
    }

    private void applyAccepted(GatewayAuditEvent event, String eventRef) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, "MISSING_ORDER_ID", "orderId がありません");
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

    private void applyPendingState(
        GatewayAuditEvent event,
        String eventRef,
        String nextStatus,
        String eventType,
        String detail
    ) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, "MISSING_ORDER_ID", "orderId がありません");
            return;
        }
        OrderProjectionState current = orderStateStore.findByOrderId(event.orderId()).orElse(null);
        if (current == null) {
            appendOrphan(event, eventRef, "ORDER_NOT_FOUND", "pending event の先に注文がありません");
            return;
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
    }

    private void applyAmendRequested(GatewayAuditEvent event, String eventRef) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, "MISSING_ORDER_ID", "orderId がありません");
            return;
        }
        OrderProjectionState current = orderStateStore.findByOrderId(event.orderId()).orElse(null);
        if (current == null) {
            appendOrphan(event, eventRef, "ORDER_NOT_FOUND", "amend event の先に注文がありません");
            return;
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
    }

    private void applyExecutionReport(GatewayAuditEvent event, String eventRef) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, "MISSING_ORDER_ID", "orderId がありません");
            return;
        }
        OrderProjectionState current = orderStateStore.findByOrderId(event.orderId()).orElse(null);
        if (current == null) {
            appendOrphan(event, eventRef, "ORDER_NOT_FOUND", "execution report の先に注文がありません");
            return;
        }
        JsonNode data = safeData(event.data());
        long filledTotal = data.path("filledQtyTotal").asLong(current.filledQuantity());
        if (filledTotal < current.filledQuantity()) {
            duplicates.incrementAndGet();
            return;
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
    }

    private void applyOrderUpdated(GatewayAuditEvent event, String eventRef) {
        if (event.orderId() == null) {
            appendOrphan(event, eventRef, "MISSING_ORDER_ID", "orderId がありません");
            return;
        }
        OrderProjectionState current = orderStateStore.findByOrderId(event.orderId()).orElse(null);
        if (current == null) {
            appendOrphan(event, eventRef, "ORDER_NOT_FOUND", "order update の先に注文がありません");
            return;
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
            throw new IllegalStateException("failed_to_write_backoffice_audit_offset:" + offsetPath, exception);
        }
    }

    private static JsonNode safeData(JsonNode data) {
        return data == null ? OBJECT_MAPPER.createObjectNode() : data;
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

    private static String textOr(JsonNode node, String fieldName, String fallback) {
        return node.hasNonNull(fieldName) ? node.get(fieldName).asText() : fallback;
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
        long replays,
        Long lastEventAt,
        long currentOffset,
        long currentAuditSize,
        int ledgerEntryCount,
        int deadLetterCount
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

    private record PositionMutation(List<PositionView> positions, long realizedPnlDelta) {
    }
}
