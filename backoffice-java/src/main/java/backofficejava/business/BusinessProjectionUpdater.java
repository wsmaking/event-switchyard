package backofficejava.business;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.AccountOverviewView;
import backofficejava.account.FillReadModel;
import backofficejava.account.FillView;
import backofficejava.account.OrderProjectionState;
import backofficejava.account.OrderProjectionStateStore;
import backofficejava.account.PositionReadModel;
import backofficejava.account.PositionView;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public final class BusinessProjectionUpdater {
    private static final DateTimeFormatter DATE_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm", Locale.US).withZone(ZoneId.of("Asia/Tokyo"));

    private final AccountOverviewReadModel accountOverviewReadModel;
    private final PositionReadModel positionReadModel;
    private final FillReadModel fillReadModel;
    private final OrderProjectionStateStore orderProjectionStateStore;
    private final ExecutionPackageReadModel executionPackageReadModel;
    private final ParentExecutionStateReadModel parentExecutionStateReadModel;
    private final AllocationStateReadModel allocationStateReadModel;
    private final PostTradePackageReadModel postTradePackageReadModel;
    private final SettlementProjectionReadModel settlementProjectionReadModel;
    private final StatementProjectionReadModel statementProjectionReadModel;
    private final RiskSnapshotReadModel riskSnapshotReadModel;
    private final SettlementExceptionWorkflowReadModel settlementExceptionWorkflowReadModel;
    private final CorporateActionWorkflowReadModel corporateActionWorkflowReadModel;
    private final MarginProjectionReadModel marginProjectionReadModel;
    private final ScenarioEvaluationHistoryReadModel scenarioEvaluationHistoryReadModel;
    private final BacktestHistoryReadModel backtestHistoryReadModel;
    private final AccountHierarchyReadModel accountHierarchyReadModel;
    private final OperatorControlStateReadModel operatorControlStateReadModel;

    public BusinessProjectionUpdater(
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel,
        OrderProjectionStateStore orderProjectionStateStore,
        ExecutionPackageReadModel executionPackageReadModel,
        ParentExecutionStateReadModel parentExecutionStateReadModel,
        AllocationStateReadModel allocationStateReadModel,
        PostTradePackageReadModel postTradePackageReadModel,
        SettlementProjectionReadModel settlementProjectionReadModel,
        StatementProjectionReadModel statementProjectionReadModel,
        RiskSnapshotReadModel riskSnapshotReadModel,
        SettlementExceptionWorkflowReadModel settlementExceptionWorkflowReadModel,
        CorporateActionWorkflowReadModel corporateActionWorkflowReadModel,
        MarginProjectionReadModel marginProjectionReadModel,
        ScenarioEvaluationHistoryReadModel scenarioEvaluationHistoryReadModel,
        BacktestHistoryReadModel backtestHistoryReadModel,
        AccountHierarchyReadModel accountHierarchyReadModel,
        OperatorControlStateReadModel operatorControlStateReadModel
    ) {
        this.accountOverviewReadModel = accountOverviewReadModel;
        this.positionReadModel = positionReadModel;
        this.fillReadModel = fillReadModel;
        this.orderProjectionStateStore = orderProjectionStateStore;
        this.executionPackageReadModel = executionPackageReadModel;
        this.parentExecutionStateReadModel = parentExecutionStateReadModel;
        this.allocationStateReadModel = allocationStateReadModel;
        this.postTradePackageReadModel = postTradePackageReadModel;
        this.settlementProjectionReadModel = settlementProjectionReadModel;
        this.statementProjectionReadModel = statementProjectionReadModel;
        this.riskSnapshotReadModel = riskSnapshotReadModel;
        this.settlementExceptionWorkflowReadModel = settlementExceptionWorkflowReadModel;
        this.corporateActionWorkflowReadModel = corporateActionWorkflowReadModel;
        this.marginProjectionReadModel = marginProjectionReadModel;
        this.scenarioEvaluationHistoryReadModel = scenarioEvaluationHistoryReadModel;
        this.backtestHistoryReadModel = backtestHistoryReadModel;
        this.accountHierarchyReadModel = accountHierarchyReadModel;
        this.operatorControlStateReadModel = operatorControlStateReadModel;
    }

    public void refresh(String accountId, String orderId, long eventAt) {
        refreshRisk(accountId, eventAt);
        if (orderId == null || orderId.isBlank()) {
            return;
        }
        Optional<OrderProjectionState> orderOptional = orderProjectionStateStore.findByOrderId(orderId);
        if (orderOptional.isEmpty()) {
            return;
        }
        OrderProjectionState order = orderOptional.get();
        List<FillView> fills = fillReadModel.findByOrderId(orderId);
        double averageFillPrice = averageFillPrice(fills, order.workingPrice());
        long filledQuantity = order.filledQuantity();
        long remainingQuantity = Math.max(0L, order.quantity() - filledQuantity);
        long executedNotional = fills.stream().mapToLong(FillView::notional).sum();
        String symbolName = symbolName(order.symbol());
        double benchmarkPrice = referencePrice(order, averageFillPrice);

        ParentExecutionStateView parentExecutionState = buildParentExecutionState(order, fills, benchmarkPrice, averageFillPrice, remainingQuantity, eventAt);
        AllocationStateView allocationState = buildAllocationState(order, fills, averageFillPrice, eventAt);
        SettlementProjectionView settlementProjection = buildSettlementProjection(order, fills, executedNotional, averageFillPrice, eventAt);
        StatementProjectionView statementProjection = buildStatementProjection(order, filledQuantity, averageFillPrice, executedNotional, settlementProjection, eventAt);
        SettlementExceptionWorkflowView settlementExceptionWorkflow = buildSettlementExceptionWorkflow(order, executedNotional, settlementProjection, eventAt);
        CorporateActionWorkflowView corporateActionWorkflow = buildCorporateActionWorkflow(order, filledQuantity, eventAt);
        OperatorControlStateView operatorControlState = buildOperatorControlState(order, parentExecutionState, allocationState, settlementExceptionWorkflow, eventAt);

        executionPackageReadModel.upsert(buildExecutionPackage(order, symbolName, benchmarkPrice, averageFillPrice, fills, allocationState, eventAt));
        parentExecutionStateReadModel.upsert(parentExecutionState);
        allocationStateReadModel.upsert(allocationState);
        postTradePackageReadModel.upsert(buildPostTradePackage(order, symbolName, averageFillPrice, executedNotional, settlementProjection, statementProjection, eventAt));
        settlementProjectionReadModel.upsert(settlementProjection);
        statementProjectionReadModel.upsert(statementProjection);
        settlementExceptionWorkflowReadModel.upsert(settlementExceptionWorkflow);
        corporateActionWorkflowReadModel.upsert(corporateActionWorkflow);
        accountHierarchyReadModel.upsert(buildAccountHierarchy(order, eventAt));
        operatorControlStateReadModel.upsert(operatorControlState);
    }

    public void reset() {
        executionPackageReadModel.reset();
        parentExecutionStateReadModel.reset();
        allocationStateReadModel.reset();
        postTradePackageReadModel.reset();
        settlementProjectionReadModel.reset();
        statementProjectionReadModel.reset();
        riskSnapshotReadModel.reset();
        settlementExceptionWorkflowReadModel.reset();
        corporateActionWorkflowReadModel.reset();
        marginProjectionReadModel.reset();
        scenarioEvaluationHistoryReadModel.reset();
        backtestHistoryReadModel.reset();
        accountHierarchyReadModel.reset();
        operatorControlStateReadModel.reset();
    }

    private ExecutionPackageView buildExecutionPackage(
        OrderProjectionState order,
        String symbolName,
        double benchmarkPrice,
        double averageFillPrice,
        List<FillView> fills,
        AllocationStateView allocationState,
        long eventAt
    ) {
        return new ExecutionPackageView(
            eventAt,
            order.orderId(),
            order.accountId(),
            order.symbol(),
            symbolName,
            "benchmark-aware " + order.side().toLowerCase(Locale.US) + " execution",
            List.of(
                new ExecutionPackageView.ExecutionStyleView(
                    "POV",
                    "flow と liquidity に追随",
                    "visible liquidity が薄いときに participation を抑える",
                    "child scheduling と venue pacing を分けて管理する",
                    List.of("arrival benchmark は守りやすい", "completion certainty は下がる")
                ),
                new ExecutionPackageView.ExecutionStyleView(
                    "care order",
                    "block / information-sensitive execution",
                    "sales-trader の説明責任を残す",
                    "operator ack と escalation が増える",
                    List.of("client explanation が厚い", "automation 度は下がる")
                )
            ),
            new ExecutionPackageView.ParentOrderPlanView(
                order.side(),
                order.quantity(),
                benchmarkPrice,
                targetParticipationPercent(order),
                scheduleWindowMinutes(order),
                chosenStyle(order),
                List.of("full sweep は slippage が読みにくい", "静的 TWAP は venue state を無視しやすい")
            ),
            buildChildOrders(order, fills, benchmarkPrice, averageFillPrice),
            new ExecutionPackageView.AllocationPlanView(
                "block-average-book",
                averageFillPrice,
                allocationState.allocatedQuantity(),
                allocationState.books().stream()
                    .map(book -> new ExecutionPackageView.AllocationSliceView(
                        book.book(),
                        book.allocatedQuantity(),
                        order.quantity() == 0L ? 0.0 : round2((book.allocatedQuantity() * 100.0) / Math.max(1L, order.quantity())),
                        book.rationale()
                    ))
                    .toList(),
                settlementNote(order.status(), allocationState.allocationStatus()),
                List.of(
                    "allocation 合計数量と filled quantity を一致させる",
                    "partial fill 中は statement release を止める",
                    "book hierarchy と operator permission を一致させる"
                )
            ),
            List.of(
                "arrival benchmark=" + formatPrice(benchmarkPrice),
                "avg fill=" + formatPrice(averageFillPrice),
                "fills=" + fills.size(),
                "allocation=" + allocationState.allocationStatus()
            )
        );
    }

    private ParentExecutionStateView buildParentExecutionState(
        OrderProjectionState order,
        List<FillView> fills,
        double benchmarkPrice,
        double averageFillPrice,
        long remainingQuantity,
        long eventAt
    ) {
        long firstSlice = Math.max(1L, Math.round(order.quantity() * 0.4));
        long secondSlice = Math.max(1L, Math.round(order.quantity() * 0.35));
        long thirdSlice = Math.max(0L, order.quantity() - firstSlice - secondSlice);
        long sliceOneExec = Math.min(order.filledQuantity(), firstSlice);
        long sliceTwoExec = Math.min(Math.max(0L, order.filledQuantity() - sliceOneExec), secondSlice);
        long sliceThreeExec = Math.min(Math.max(0L, order.filledQuantity() - sliceOneExec - sliceTwoExec), thirdSlice);
        return new ParentExecutionStateView(
            eventAt,
            order.orderId(),
            order.accountId(),
            order.symbol(),
            order.status(),
            chosenStyle(order),
            order.quantity(),
            order.filledQuantity(),
            remainingQuantity,
            targetParticipationPercent(order),
            actualParticipationPercent(order),
            scheduleWindowMinutes(order) + "m window",
            List.of(
                childState(order, "child-1", "open auction / lit", firstSlice, sliceOneExec, benchmarkPrice, averageFillPrice),
                childState(order, "child-2", "continuous book", secondSlice, sliceTwoExec, benchmarkPrice, averageFillPrice),
                childState(order, "child-3", "care / completion", thirdSlice, sliceThreeExec, benchmarkPrice, averageFillPrice)
            ),
            List.of(
                "remaining=" + remainingQuantity,
                fills.isEmpty() ? "fill 未着" : "fill quality を allocation 前に固定",
                order.status().contains("PENDING") ? "operator escalation を準備" : "steady-state"
            )
        );
    }

    private ParentExecutionStateView.ChildExecutionStateView childState(
        OrderProjectionState order,
        String childId,
        String venueIntent,
        long plannedQuantity,
        long executedQuantity,
        double benchmarkPrice,
        double averageFillPrice
    ) {
        long remaining = Math.max(0L, plannedQuantity - executedQuantity);
        String state = remaining == 0L ? "DONE" : order.status().contains("PENDING") ? "WAITING" : executedQuantity > 0L ? "WORKING" : "QUEUED";
        double basisPrice = averageFillPrice > 0.0 ? averageFillPrice : benchmarkPrice;
        double slippage = benchmarkPrice <= 0.0 ? 0.0 : round2(((basisPrice - benchmarkPrice) / benchmarkPrice) * ("BUY".equalsIgnoreCase(order.side()) ? 10_000.0 : -10_000.0));
        return new ParentExecutionStateView.ChildExecutionStateView(
            childId,
            state,
            venueIntent,
            plannedQuantity,
            executedQuantity,
            remaining,
            benchmarkPrice,
            basisPrice,
            slippage,
            remaining == 0L ? "done" : state.equals("WAITING") ? "await venue/session" : "continue slicing"
        );
    }

    private AllocationStateView buildAllocationState(
        OrderProjectionState order,
        List<FillView> fills,
        double averageFillPrice,
        long eventAt
    ) {
        long filledQuantity = order.filledQuantity();
        long bookOne = Math.round(filledQuantity * 0.5);
        long bookTwo = Math.round(filledQuantity * 0.3);
        long bookThree = Math.max(0L, filledQuantity - bookOne - bookTwo);
        String status = filledQuantity == 0L
            ? "PENDING"
            : isTerminal(order.status())
            ? "ALLOCATED"
            : "WORKING";
        return new AllocationStateView(
            eventAt,
            order.orderId(),
            order.accountId(),
            order.symbol(),
            status,
            filledQuantity,
            Math.max(0L, order.quantity() - filledQuantity),
            averageFillPrice,
            List.of(
                new AllocationStateView.BookAllocationView("japan-long-only", bookOne, bookOne, filledQuantity == 0L ? "PLANNED" : "READY", "benchmark tracking book"),
                new AllocationStateView.BookAllocationView("event-driven", bookTwo, bookTwo, filledQuantity == 0L ? "PLANNED" : "READY", "event capture sleeve"),
                new AllocationStateView.BookAllocationView("multi-strat", bookThree, bookThree, filledQuantity == 0L ? "PLANNED" : "READY", "completion / liquidity recycle")
            ),
            List.of(
                "allocation total=" + filledQuantity,
                isTerminal(order.status()) ? "statement release 候補" : "execution 完了前は release 保留",
                order.status().equals("REJECTED") ? "no allocation on reject" : "book ratio を固定"
            )
        );
    }

    private PostTradePackageView buildPostTradePackage(
        OrderProjectionState order,
        String symbolName,
        double averageFillPrice,
        long executedNotional,
        SettlementProjectionView settlementProjection,
        StatementProjectionView statementProjection,
        long eventAt
    ) {
        long commission = Math.round(executedNotional * 0.0008);
        long exchangeFee = Math.round(executedNotional * 0.00015);
        long taxes = "BUY".equalsIgnoreCase(order.side()) ? 0L : Math.round(executedNotional * 0.002);
        long netCashMovement = "BUY".equalsIgnoreCase(order.side())
            ? -(executedNotional + commission + exchangeFee + taxes)
            : executedNotional - commission - exchangeFee - taxes;
        return new PostTradePackageView(
            eventAt,
            order.orderId(),
            order.accountId(),
            order.symbol(),
            symbolName,
            order.status(),
            List.of(
                new PostTradePackageView.PostTradeStageView("Execution", "front office", "fill capture", order.filledQuantity() + " / " + order.quantity(), "約定と配賦は別の説明責任"),
                new PostTradePackageView.PostTradeStageView("Allocation", "middle office", "book assignment", order.filledQuantity() == 0L ? "pending" : "book average ready", "block average を statement より先に固める"),
                new PostTradePackageView.PostTradeStageView("Settlement", "operations", "cash/securities leg close", settlementProjection.settlementStatus(), "trade date と settle date を分ける"),
                new PostTradePackageView.PostTradeStageView("Books & Records", "finance-control", "confirm / statement", statementProjection.statementStatus(), "final-out と statement は同義ではない")
            ),
            new PostTradePackageView.FeeBreakdownView(
                executedNotional,
                commission,
                exchangeFee,
                taxes,
                netCashMovement,
                List.of("国内 cash equities 想定の簡略 fee table", "tax は sell side のみ簡略反映")
            ),
            new PostTradePackageView.StatementPreviewView(
                order.accountId(),
                order.symbol(),
                symbolName,
                order.filledQuantity(),
                averageFillPrice,
                settlementProjection.settlementDateLabel(),
                String.format(Locale.US, "%,d", netCashMovement),
                List.of("statement は confirm reference を持つ", "settlement exception がある間は release しない")
            ),
            List.of(
                new PostTradePackageView.SettlementCheckView("trade vs settle", "T+0 / T+2 を混同しない", settlementProjection.tradeDateLabel() + " -> " + settlementProjection.settlementDateLabel()),
                new PostTradePackageView.SettlementCheckView("cash movement", "fee/tax を net cash に落とす", String.format(Locale.US, "%,d", netCashMovement)),
                new PostTradePackageView.SettlementCheckView("books close", "statement release 前に exception を解消", statementProjection.statementStatus())
            ),
            List.of(
                new PostTradePackageView.CorporateActionHookView("Dividend", "権利落ち後に評価損益説明が変わる", "ledger / statement note を更新"),
                new PostTradePackageView.CorporateActionHookView("Stock split", "数量と avg price の説明が変わる", "position restatement と statement continuity を守る")
            )
        );
    }

    private SettlementProjectionView buildSettlementProjection(
        OrderProjectionState order,
        List<FillView> fills,
        long executedNotional,
        double averageFillPrice,
        long eventAt
    ) {
        long filledQuantity = order.filledQuantity();
        String settlementStatus = filledQuantity == 0L
            ? "NO_TRADE"
            : isTerminal(order.status())
            ? "READY"
            : "WORKING";
        String cashLeg = filledQuantity == 0L ? "NOT_APPLICABLE" : isTerminal(order.status()) ? "PENDING_RELEASE" : "BOOKING";
        String securitiesLeg = filledQuantity == 0L ? "NOT_APPLICABLE" : isTerminal(order.status()) ? "PENDING_DELIVERY" : "BOOKING";
        List<String> flags = new ArrayList<>();
        if (filledQuantity > 0L && !isTerminal(order.status())) {
            flags.add("PARTIAL_FILL_OPEN");
        }
        if (fills.size() > 1) {
            flags.add("MULTI_FILL_AVERAGE_PRICE");
        }
        if (averageFillPrice == 0.0 && filledQuantity > 0L) {
            flags.add("PRICE_MISSING_FALLBACK");
        }
        return new SettlementProjectionView(
            eventAt,
            order.orderId(),
            order.accountId(),
            order.symbol(),
            settlementStatus,
            formatTimestamp(eventAt),
            formatTimestamp(eventAt + (2L * 24L * 60L * 60L * 1000L)),
            executedNotional,
            "BUY".equalsIgnoreCase(order.side()) ? -executedNotional : executedNotional,
            filledQuantity,
            cashLeg,
            securitiesLeg,
            flags,
            settlementStatus.equals("READY") ? "release to operations queue" : settlementStatus.equals("NO_TRADE") ? "wait for fill" : "continue trade capture"
        );
    }

    private StatementProjectionView buildStatementProjection(
        OrderProjectionState order,
        long filledQuantity,
        double averageFillPrice,
        long executedNotional,
        SettlementProjectionView settlementProjection,
        long eventAt
    ) {
        String status = filledQuantity == 0L
            ? "NO_TRADE"
            : settlementProjection.exceptionFlags().isEmpty() && "READY".equals(settlementProjection.settlementStatus())
            ? "READY"
            : "HELD";
        return new StatementProjectionView(
            eventAt,
            order.orderId(),
            order.accountId(),
            order.symbol(),
            status,
            "CONF-" + shortId(order.orderId()),
            "STMT-" + shortId(order.orderId()),
            order.side() + " " + filledQuantity + " " + order.symbol() + " @ " + formatPrice(averageFillPrice),
            List.of(
                new StatementProjectionView.StatementLineView("Account", order.accountId(), "statement recipient"),
                new StatementProjectionView.StatementLineView("Trade", order.side() + " " + filledQuantity + " " + symbolName(order.symbol()), "booked quantity"),
                new StatementProjectionView.StatementLineView("Average Price", formatPrice(averageFillPrice), "multi-fill average"),
                new StatementProjectionView.StatementLineView("Gross Notional", String.format(Locale.US, "%,d", executedNotional), "before fee / tax")
            ),
            List.of(
                "confirm reference は allocation / books close と対応付ける",
                status.equals("READY") ? "release ready" : "hold until settlement workflow clear"
            )
        );
    }

    private SettlementExceptionWorkflowView buildSettlementExceptionWorkflow(
        OrderProjectionState order,
        long executedNotional,
        SettlementProjectionView settlementProjection,
        long eventAt
    ) {
        boolean partialOpen = order.filledQuantity() > 0L && !isTerminal(order.status());
        String workflowStatus = partialOpen ? "WATCH" : settlementProjection.settlementStatus().equals("READY") ? "CLEAR" : "IDLE";
        String exceptionType = partialOpen ? "PARTIAL_FILL_RECON" : "NONE";
        return new SettlementExceptionWorkflowView(
            eventAt,
            order.orderId(),
            order.accountId(),
            order.symbol(),
            workflowStatus,
            exceptionType,
            partialOpen ? "allocation / statement" : "none",
            partialOpen ? "T+0" : "none",
            partialOpen ? "fill closed but parent still open" : "none",
            partialOpen ? "middle-office" : "back-office",
            partialOpen ? "next allocation cycle" : "not required",
            partialOpen ? Math.round(executedNotional * 0.01) : 0L,
            partialOpen ? Math.max(0L, order.quantity() - order.filledQuantity()) : 0L,
            partialOpen,
            partialOpen ? "0-1d" : "none",
            partialOpen ? "reconcile allocation before statement release" : "no exception",
            partialOpen ? List.of("cash break watch", "residual quantity watch") : List.of("no break"),
            partialOpen ? List.of("statement hold", "books release requires operator ack") : List.of("books release permitted"),
            partialOpen ? List.of("partial fill remains open", "watch cancel/correct risk") : List.of("steady-state")
        );
    }

    private CorporateActionWorkflowView buildCorporateActionWorkflow(OrderProjectionState order, long filledQuantity, long eventAt) {
        String workflowStatus = filledQuantity == 0L ? "IDLE" : "WATCH";
        return new CorporateActionWorkflowView(
            eventAt,
            order.orderId(),
            order.accountId(),
            order.symbol(),
            "Dividend entitlement watch",
            workflowStatus,
            formatTimestamp(eventAt + (10L * 24L * 60L * 60L * 1000L)),
            formatTimestamp(eventAt + (13L * 24L * 60L * 60L * 1000L)),
            filledQuantity == 0L ? "no position" : "ex-date after fill changes customer explanation",
            filledQuantity == 0L ? "none" : "position valuation / ledger note update",
            filledQuantity == 0L ? "none" : "statement note append on ex-date",
            filledQuantity == 0L ? "not required" : "preserve avg price continuity around ex-date",
            filledQuantity == 0L ? "none" : "watch CA calendar before statement cycle",
            filledQuantity == 0L ? List.of("no action") : List.of("do not rewrite historical fill", "apply CA through books and records flow")
        );
    }

    private AccountHierarchyView buildAccountHierarchy(OrderProjectionState order, long eventAt) {
        return new AccountHierarchyView(
            eventAt,
            order.accountId(),
            "switchyard client alpha",
            "Switchyard Securities Japan",
            "JP",
            "cash-equities",
            chosenStyle(order),
            "demo-clearing-broker",
            "demo-custodian",
            List.of(
                new AccountHierarchyView.BookHierarchyView("Switchyard Japan Fund", "japan-long-only", "acct-jp-long-01", "desk-trader-a", "TSE/JASDEC", "benchmark tracking"),
                new AccountHierarchyView.BookHierarchyView("Switchyard Event Fund", "event-driven", "acct-event-02", "desk-trader-b", "TSE/JASDEC", "event capture"),
                new AccountHierarchyView.BookHierarchyView("Switchyard Multi Strategy", "multi-strat", "acct-ms-03", "desk-trader-c", "TSE/JASDEC", "liquidity recycle")
            ),
            List.of(
                new AccountHierarchyView.PermissionGrantView("trader", "order-entry", List.of("submit", "cancel", "request care"), false, "新規リスクを持つ操作"),
                new AccountHierarchyView.PermissionGrantView("middle-office", "allocation-settlement", List.of("approve allocation", "ack settlement exception", "release statement"), true, "books and records を閉じる"),
                new AccountHierarchyView.PermissionGrantView("risk-control", "limit-override", List.of("reduce-only", "deny new risk", "force margin review"), true, "limit breach 時の制御")
            ),
            List.of(
                "client -> legal entity -> desk -> fund -> book -> sub-account",
                "allocation と statement は同じ hierarchy で説明する",
                "role permissions を order status と分離して残す"
            ),
            List.of(
                "allocation ratio と hierarchy を突き合わせる",
                "custodian leg と statement recipient を混同しない"
            )
        );
    }

    private OperatorControlStateView buildOperatorControlState(
        OrderProjectionState order,
        ParentExecutionStateView parentExecutionState,
        AllocationStateView allocationState,
        SettlementExceptionWorkflowView settlementExceptionWorkflow,
        long eventAt
    ) {
        String workflowState = switch (order.status()) {
            case "PARTIALLY_FILLED" -> "AWAITING_ALLOCATION_ACK";
            case "REJECTED" -> "RISK_REVIEW";
            case "CANCELED", "EXPIRED" -> "RELEASE_RECORDED";
            default -> "CLEAR";
        };
        return new OperatorControlStateView(
            eventAt,
            order.orderId(),
            order.accountId(),
            workflowState,
            order.status().equals("REJECTED") ? "HIGH" : order.status().equals("PARTIALLY_FILLED") ? "MEDIUM" : "LOW",
            List.of(
                new OperatorControlStateView.ApprovalRequirementView("allocation approval", "middle-office", allocationState.allocationStatus().equals("PENDING") ? "CLEAR" : "REQUIRED", "配賦数量と parent fill 総量の一致確認", "block average と statement draft を更新する"),
                new OperatorControlStateView.ApprovalRequirementView("risk acknowledgement", "risk-control", order.status().equals("REJECTED") ? "REQUIRED" : "CLEAR", "deny 理由と entitlement 影響を残す", "operator log に判断理由を残す"),
                new OperatorControlStateView.ApprovalRequirementView("books release", "back-office", settlementExceptionWorkflow.workflowStatus().equals("CLEAR") ? "CLEAR" : "REQUIRED", "books and records を閉じる前に exception を解消", "statement / settlement を解放する")
            ),
            List.of(
                new OperatorControlStateView.OperatorAcknowledgementView("desk-trader-a", "arrival benchmark fixed", "T+0 09:00", "parent benchmark を固定"),
                new OperatorControlStateView.OperatorAcknowledgementView("middle-office", "allocation sanity check", "T+0 15:35", allocationState.allocationStatus()),
                new OperatorControlStateView.OperatorAcknowledgementView("back-office", "books release review", "T+1 08:10", settlementExceptionWorkflow.workflowStatus())
            ),
            List.of("submit child order", "cancel remaining", "request care escalation", "ack settlement workflow"),
            List.of(
                order.status().equals("REJECTED") ? "release new risk" : "none",
                settlementExceptionWorkflow.workflowStatus().equals("CLEAR") ? "none" : "statement release"
            ),
            List.of(
                "workflow=" + workflowState,
                "parentStatus=" + parentExecutionState.parentStatus(),
                "settlementWorkflow=" + settlementExceptionWorkflow.workflowStatus()
            )
        );
    }

    private void refreshRisk(String accountId, long eventAt) {
        List<PositionView> positions = new ArrayList<>(positionReadModel.findByAccountId(accountId));
        positions.sort(Comparator.comparing(PositionView::symbol));
        AccountOverviewView overview = currentOverview(accountId);
        double marketValue = positions.stream()
            .mapToDouble(position -> Math.abs(position.netQty() * position.avgPrice()))
            .sum();
        List<RiskSnapshotView.ConcentrationMetricView> concentration = positions.stream()
            .map(position -> {
                double exposure = Math.abs(position.netQty() * position.avgPrice());
                double weight = marketValue == 0.0 ? 0.0 : round2((exposure / marketValue) * 100.0);
                return new RiskSnapshotView.ConcentrationMetricView(position.symbol(), symbolName(position.symbol()), round2(exposure), weight, weight >= 35.0 ? "single-name watch" : "within concentration budget");
            })
            .toList();
        List<RiskSnapshotView.LiquidityMetricView> liquidity = positions.stream()
            .map(position -> {
                long visibleTop = Math.max(500L, Math.min(25_000L, Math.abs(position.netQty()) / 4L + 2_000L));
                double participation = visibleTop == 0L ? 0.0 : round2((Math.abs(position.netQty()) * 100.0) / visibleTop);
                double exitDays = round2(Math.max(0.25, Math.abs(position.netQty()) / 12_000.0));
                return new RiskSnapshotView.LiquidityMetricView(position.symbol(), symbolName(position.symbol()), position.netQty(), visibleTop, participation, exitDays, exitDays > 3.0 ? "liquidity add-on" : "normal liquidation window");
            })
            .toList();
        List<RiskSnapshotView.FactorExposureView> factorExposures = buildFactorExposures(concentration, liquidity);
        List<RiskSnapshotView.LiquidityBucketView> liquidityBuckets = List.of(
            new RiskSnapshotView.LiquidityBucketView("0-1 day", round2(sumExposure(liquidity, 0.0, 1.0)), 1.0, "routine unwind"),
            new RiskSnapshotView.LiquidityBucketView("1-3 day", round2(sumExposure(liquidity, 1.0, 3.0)), 3.0, "watch liquidity usage"),
            new RiskSnapshotView.LiquidityBucketView("3d+", round2(sumExposure(liquidity, 3.0, Double.MAX_VALUE)), 5.0, "add liquidity buffer")
        );
        List<RiskSnapshotView.LimitBreachView> limitBreaches = buildLimitBreaches(concentration, liquidityBuckets);
        RiskSnapshotView snapshot = new RiskSnapshotView(
            eventAt,
            accountId,
            round2(marketValue),
            overview.cashBalance(),
            concentration,
            factorExposures,
            liquidity,
            liquidityBuckets,
            scenarioLibrary(),
            buildBacktestingPreview(positions),
            modelBoundaries(),
            governanceChecks(limitBreaches),
            limitBreaches,
            marginAlerts(limitBreaches, liquidityBuckets)
        );
        riskSnapshotReadModel.upsert(snapshot);
        MarginProjectionView marginProjection = buildMarginProjection(snapshot, overview, eventAt);
        marginProjectionReadModel.upsert(marginProjection);
        scenarioEvaluationHistoryReadModel.upsert(buildScenarioHistory(snapshot, eventAt));
        backtestHistoryReadModel.upsert(buildBacktestHistory(snapshot, eventAt));
    }

    private MarginProjectionView buildMarginProjection(RiskSnapshotView snapshot, AccountOverviewView overview, long eventAt) {
        double liquidityAddOn = snapshot.liquidityBuckets().stream()
            .filter(bucket -> bucket.stressedExitDays() > 3.0)
            .mapToDouble(bucket -> bucket.grossExposure() * 0.12)
            .sum();
        double concentrationAddOn = snapshot.concentration().stream()
            .filter(metric -> metric.weightPercent() >= 35.0)
            .mapToDouble(metric -> metric.exposure() * 0.1)
            .sum();
        double marginUsed = round2((snapshot.marketValue() * 0.18) + liquidityAddOn + concentrationAddOn + overview.reservedBuyingPower());
        double marginLimit = round2(Math.max(1_500_000.0, overview.availableBuyingPower() + overview.reservedBuyingPower() + 750_000.0));
        double utilization = marginLimit == 0.0 ? 0.0 : round2((marginUsed / marginLimit) * 100.0);
        String breachStatus = utilization >= 95.0 ? "BREACH" : utilization >= 80.0 ? "WATCH" : "WITHIN_LIMIT";
        List<String> breachedLimits = new ArrayList<>();
        if (utilization >= 80.0) {
            breachedLimits.add("portfolio margin utilization " + utilization + "%");
        }
        snapshot.limitBreaches().stream()
            .filter(limit -> !"CLEAR".equals(limit.state()))
            .map(limit -> limit.limitName() + " " + limit.state())
            .forEach(breachedLimits::add);
        return new MarginProjectionView(
            eventAt,
            snapshot.accountId(),
            "educational portfolio margin proxy",
            marginLimit,
            marginUsed,
            utilization,
            breachStatus,
            breachedLimits,
            breachStatus.equals("BREACH")
                ? List.of("reduce-only に切り替える", "scenario stress を更新する")
                : breachStatus.equals("WATCH")
                ? List.of("new risk 前に headroom を確認する", "liquidity bucket を再確認する")
                : List.of("margin headroom あり"),
            List.of(
                "reservation は order-level、margin は portfolio-level",
                "clearer rule book は簡略化している",
                "feed stale 時は valuation guard を広げる"
            ),
            List.of(
                overview.reservedBuyingPower() > 0L ? "working order reservation が headroom を圧迫" : "reservation は軽微",
                liquidityAddOn > 0.0 ? "liquidity add-on を適用" : "liquidity add-on は軽微",
                concentrationAddOn > 0.0 ? "single-name concentration add-on を適用" : "concentration add-on なし"
            ),
            breachStatus.equals("BREACH") ? "next 15m" : "next 1h"
        );
    }

    private ScenarioEvaluationHistoryView buildScenarioHistory(RiskSnapshotView snapshot, long eventAt) {
        return new ScenarioEvaluationHistoryView(
            eventAt,
            snapshot.accountId(),
            formatTimestamp(eventAt),
            snapshot.limitBreaches().stream().anyMatch(limit -> !"CLEAR".equals(limit.state())) ? "WATCH" : "APPROVED",
            "risk-sim-v3",
            List.of("risk lead approval", "control note attached"),
            snapshot.scenarioLibrary().stream()
                .limit(3)
                .map(entry -> new ScenarioEvaluationHistoryView.ScenarioEvaluationEntryView(
                    entry.title(),
                    entry.shock(),
                    round2(-snapshot.marketValue() * scenarioShock(entry.id())),
                    entry.focus()
                ))
                .toList()
        );
    }

    private BacktestHistoryView buildBacktestHistory(RiskSnapshotView snapshot, long eventAt) {
        return new BacktestHistoryView(
            eventAt,
            snapshot.accountId(),
            "直近 8 bucket",
            "board top / fill average / liquidity bucket",
            snapshot.backtesting().breachRatePercent(),
            List.of("holiday session は除外", "cross-venue fragmentation は未反映"),
            snapshot.backtesting().samples().stream()
                .map(sample -> new BacktestHistoryView.BacktestHistoryPointView(
                    sample.label(),
                    sample.pnl(),
                    sample.breached(),
                    sample.breached() ? "loss limit 超過サンプル" : "想定範囲内"
                ))
                .toList()
        );
    }

    private RiskSnapshotView.BacktestingPreviewView buildBacktestingPreview(List<PositionView> positions) {
        List<RiskSnapshotView.BacktestSampleView> samples = new ArrayList<>();
        double gross = positions.stream().mapToDouble(position -> Math.abs(position.netQty() * position.avgPrice())).sum();
        for (int index = 0; index < 8; index++) {
            double pnl = round2((gross * 0.012) - (index * gross * 0.01));
            boolean breached = pnl < -(gross * 0.015);
            samples.add(new RiskSnapshotView.BacktestSampleView("t-" + index, pnl, breached));
        }
        double breachRate = samples.isEmpty() ? 0.0 : round2((samples.stream().filter(RiskSnapshotView.BacktestSampleView::breached).count() * 100.0) / samples.size());
        double averageTailLoss = round2(samples.stream()
            .filter(RiskSnapshotView.BacktestSampleView::breached)
            .mapToDouble(RiskSnapshotView.BacktestSampleView::pnl)
            .average()
            .orElse(0.0));
        return new RiskSnapshotView.BacktestingPreviewView(
            samples.size(),
            breachRate,
            averageTailLoss,
            "historical replay bucket based preview",
            samples
        );
    }

    private List<RiskSnapshotView.FactorExposureView> buildFactorExposures(
        List<RiskSnapshotView.ConcentrationMetricView> concentration,
        List<RiskSnapshotView.LiquidityMetricView> liquidity
    ) {
        double singleName = concentration.stream().mapToDouble(RiskSnapshotView.ConcentrationMetricView::weightPercent).max().orElse(0.0);
        double liquidityStress = liquidity.stream().mapToDouble(metric -> metric.estimatedDaysToExit() * 12.0).sum();
        double cyclicality = concentration.stream().mapToDouble(metric -> metric.exposure() * 0.18).sum();
        return List.of(
            factor("single-name", singleName, 35.0, "one-name concentration"),
            factor("liquidity", liquidityStress, 50.0, "exit day stress"),
            factor("beta", cyclicality, 125_000_000.0, "cash equity beta proxy")
        );
    }

    private RiskSnapshotView.FactorExposureView factor(String name, double exposure, double limit, String note) {
        double utilization = limit == 0.0 ? 0.0 : round2((exposure / limit) * 100.0);
        return new RiskSnapshotView.FactorExposureView(name, round2(exposure), limit, utilization, note);
    }

    private List<RiskSnapshotView.LimitBreachView> buildLimitBreaches(
        List<RiskSnapshotView.ConcentrationMetricView> concentration,
        List<RiskSnapshotView.LiquidityBucketView> liquidityBuckets
    ) {
        List<RiskSnapshotView.LimitBreachView> values = new ArrayList<>();
        double maxWeight = concentration.stream().mapToDouble(RiskSnapshotView.ConcentrationMetricView::weightPercent).max().orElse(0.0);
        values.add(new RiskSnapshotView.LimitBreachView(
            "single-name concentration",
            maxWeight >= 45.0 ? "HIGH" : maxWeight >= 35.0 ? "MEDIUM" : "LOW",
            maxWeight >= 45.0 ? "BREACH" : maxWeight >= 35.0 ? "WATCH" : "CLEAR",
            maxWeight >= 35.0 ? "reduce or hedge concentration" : "no action"
        ));
        double stressedExposure = liquidityBuckets.stream()
            .filter(bucket -> bucket.stressedExitDays() > 3.0)
            .mapToDouble(RiskSnapshotView.LiquidityBucketView::grossExposure)
            .sum();
        values.add(new RiskSnapshotView.LimitBreachView(
            "liquidity exit bucket",
            stressedExposure > 2_500_000.0 ? "MEDIUM" : "LOW",
            stressedExposure > 2_500_000.0 ? "WATCH" : "CLEAR",
            stressedExposure > 2_500_000.0 ? "reduce liquidation horizon or add buffer" : "within policy"
        ));
        return values;
    }

    private List<RiskSnapshotView.GovernanceCheckView> governanceChecks(List<RiskSnapshotView.LimitBreachView> breaches) {
        boolean requiresEscalation = breaches.stream().anyMatch(breach -> !"CLEAR".equals(breach.state()));
        return List.of(
            new RiskSnapshotView.GovernanceCheckView("model boundary review", "CLEAR", "risk-control", "simplified assumptions acknowledged"),
            new RiskSnapshotView.GovernanceCheckView("limit escalation", requiresEscalation ? "WATCH" : "CLEAR", "desk-risk", requiresEscalation ? "limit breach workflow を更新" : "no escalation"),
            new RiskSnapshotView.GovernanceCheckView("feed stale guard", "CLEAR", "operations", "valuation guard handled in ops layer")
        );
    }

    private List<String> marginAlerts(List<RiskSnapshotView.LimitBreachView> breaches, List<RiskSnapshotView.LiquidityBucketView> buckets) {
        List<String> alerts = new ArrayList<>();
        breaches.stream()
            .filter(breach -> !"CLEAR".equals(breach.state()))
            .map(breach -> breach.limitName() + " " + breach.state())
            .forEach(alerts::add);
        buckets.stream()
            .filter(bucket -> bucket.stressedExitDays() > 3.0)
            .map(bucket -> bucket.bucket() + " requires liquidity add-on")
            .forEach(alerts::add);
        return alerts.isEmpty() ? List.of("margin alert なし") : alerts;
    }

    private List<RiskSnapshotView.ScenarioLibraryEntryView> scenarioLibrary() {
        return List.of(
            new RiskSnapshotView.ScenarioLibraryEntryView("single-name-gap", "single-name gap", "equity", "-12%", "one-name shock の説明軸", "concentration"),
            new RiskSnapshotView.ScenarioLibraryEntryView("market-beta-down", "market beta down", "equity", "-5%", "gross / net の影響を見る", "market beta"),
            new RiskSnapshotView.ScenarioLibraryEntryView("spread-widening", "spread widening", "liquidity", "+40% spread", "execution quality と liquidity を同時に見る", "liquidity")
        );
    }

    private List<RiskSnapshotView.ModelBoundaryView> modelBoundaries() {
        return List.of(
            new RiskSnapshotView.ModelBoundaryView("Portfolio margin proxy", "reservation と margin を混同しない", "cash equity exposure, liquidity add-on", "clearer rule book, offsets"),
            new RiskSnapshotView.ModelBoundaryView("Backtesting preview", "breach rate は教育用の近似", "recent risk buckets", "full historical market data"),
            new RiskSnapshotView.ModelBoundaryView("Scenario library", "stress rationale を固定する", "named shocks and notes", "desk-specific bespoke stress")
        );
    }

    private AccountOverviewView currentOverview(String accountId) {
        return accountOverviewReadModel.findByAccountId(accountId)
            .orElseGet(() -> new AccountOverviewView(accountId, 10_000_000L, 10_000_000L, 0L, 0, 0L, Instant.EPOCH));
    }

    private List<ExecutionPackageView.ChildOrderSliceView> buildChildOrders(
        OrderProjectionState order,
        List<FillView> fills,
        double benchmarkPrice,
        double averageFillPrice
    ) {
        long total = Math.max(1L, order.quantity());
        long first = Math.max(1L, Math.round(total * 0.4));
        long second = Math.max(1L, Math.round(total * 0.35));
        long third = Math.max(0L, total - first - second);
        double slippage = benchmarkPrice <= 0.0 ? 0.0 : round2(((averageFillPrice - benchmarkPrice) / benchmarkPrice) * ("BUY".equalsIgnoreCase(order.side()) ? 10_000.0 : -10_000.0));
        return List.of(
            childOrder("slice-1", "open auction / passive", first, benchmarkPrice, averageFillPrice, slippage, "09:00-09:20"),
            childOrder("slice-2", "continuous lit / POV", second, benchmarkPrice, averageFillPrice, slippage, "09:20-10:30"),
            childOrder("slice-3", "care completion", third, benchmarkPrice, averageFillPrice, slippage, fills.size() > 1 ? "multi-fill window" : "completion window")
        );
    }

    private ExecutionPackageView.ChildOrderSliceView childOrder(
        String id,
        String venueIntent,
        long plannedQuantity,
        double benchmarkPrice,
        double expectedFillPrice,
        double slippageBps,
        String bucket
    ) {
        return new ExecutionPackageView.ChildOrderSliceView(
            id,
            venueIntent,
            plannedQuantity,
            benchmarkPrice,
            expectedFillPrice,
            slippageBps,
            bucket,
            slippageBps > 15.0 ? "aggressive completion は benchmark を壊しやすい" : "passive slice で spread を温存する"
        );
    }

    private double actualParticipationPercent(OrderProjectionState order) {
        if (order.quantity() <= 0L) {
            return 0.0;
        }
        return round2((order.filledQuantity() * 100.0) / order.quantity());
    }

    private double targetParticipationPercent(OrderProjectionState order) {
        return order.quantity() >= 5_000L ? 12.5 : order.quantity() >= 1_000L ? 18.0 : 24.0;
    }

    private int scheduleWindowMinutes(OrderProjectionState order) {
        return order.quantity() >= 5_000L ? 90 : order.quantity() >= 1_000L ? 45 : 20;
    }

    private String chosenStyle(OrderProjectionState order) {
        return order.quantity() >= 5_000L ? "POV" : order.quantity() >= 1_000L ? "VWAP" : "care order";
    }

    private String settlementNote(String orderStatus, String allocationStatus) {
        if ("REJECTED".equals(orderStatus)) {
            return "reject は allocation を発生させない";
        }
        if ("ALLOCATED".equals(allocationStatus)) {
            return "book average を statement / settlement に引き継ぐ";
        }
        return "allocation 確定前は statement hold";
    }

    private double averageFillPrice(List<FillView> fills, long fallback) {
        long totalQuantity = fills.stream().mapToLong(FillView::quantity).sum();
        double totalNotional = fills.stream().mapToDouble(fill -> fill.price() * fill.quantity()).sum();
        if (totalQuantity == 0L) {
            return fallback <= 0L ? inferReferencePrice("UNKNOWN") : fallback;
        }
        return round2(totalNotional / totalQuantity);
    }

    private double referencePrice(OrderProjectionState order, double averageFillPrice) {
        if (order.workingPrice() > 0L) {
            return order.workingPrice();
        }
        if (averageFillPrice > 0.0) {
            return averageFillPrice;
        }
        return inferReferencePrice(order.symbol());
    }

    private double inferReferencePrice(String symbol) {
        return switch (symbol) {
            case "7203" -> 2765.0;
            case "6758" -> 13240.0;
            case "8306" -> 1823.0;
            default -> 1000.0;
        };
    }

    private String symbolName(String symbol) {
        return switch (symbol) {
            case "7203" -> "Toyota Motor";
            case "6758" -> "Sony Group";
            case "8306" -> "Mitsubishi UFJ";
            default -> symbol;
        };
    }

    private boolean isTerminal(String status) {
        return "FILLED".equals(status) || "REJECTED".equals(status) || "CANCELED".equals(status) || "EXPIRED".equals(status);
    }

    private double scenarioShock(String id) {
        return switch (id) {
            case "single-name-gap" -> 0.12;
            case "market-beta-down" -> 0.05;
            case "spread-widening" -> 0.008;
            default -> 0.02;
        };
    }

    private double sumExposure(List<RiskSnapshotView.LiquidityMetricView> liquidity, double minDaysInclusive, double maxDaysExclusive) {
        return liquidity.stream()
            .filter(metric -> metric.estimatedDaysToExit() >= minDaysInclusive && metric.estimatedDaysToExit() < maxDaysExclusive)
            .mapToDouble(metric -> Math.abs(metric.positionQuantity() * metric.positionQuantity() * 0.01))
            .sum();
    }

    private String shortId(String orderId) {
        if (orderId == null || orderId.isBlank()) {
            return "UNKNOWN";
        }
        return orderId.length() <= 8 ? orderId.toUpperCase(Locale.US) : orderId.substring(orderId.length() - 8).toUpperCase(Locale.US);
    }

    private String formatTimestamp(long timestamp) {
        return DATE_FORMATTER.format(Instant.ofEpochMilli(timestamp));
    }

    private String formatPrice(double price) {
        return String.format(Locale.US, "%.2f", price);
    }

    private double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
