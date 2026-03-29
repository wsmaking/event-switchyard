package backofficejava.http;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.AccountOverviewView;
import backofficejava.account.FillReadModel;
import backofficejava.account.FillView;
import backofficejava.account.LedgerReadModel;
import backofficejava.account.LedgerEntryView;
import backofficejava.account.OrderProjectionState;
import backofficejava.account.OrderProjectionStateStore;
import backofficejava.account.PositionReadModel;
import backofficejava.account.PositionView;
import backofficejava.business.AllocationStateReadModel;
import backofficejava.business.AllocationStateView;
import backofficejava.business.ExecutionPackageReadModel;
import backofficejava.business.ExecutionPackageView;
import backofficejava.business.ParentExecutionStateReadModel;
import backofficejava.business.ParentExecutionStateView;
import backofficejava.business.PostTradePackageReadModel;
import backofficejava.business.PostTradePackageView;
import backofficejava.business.SettlementProjectionReadModel;
import backofficejava.business.SettlementProjectionView;
import backofficejava.business.StatementProjectionReadModel;
import backofficejava.business.StatementProjectionView;
import backofficejava.business.RiskSnapshotReadModel;
import backofficejava.business.RiskSnapshotView;
import backofficejava.business.SettlementExceptionWorkflowReadModel;
import backofficejava.business.SettlementExceptionWorkflowView;
import backofficejava.business.CorporateActionWorkflowReadModel;
import backofficejava.business.CorporateActionWorkflowView;
import backofficejava.business.MarginProjectionReadModel;
import backofficejava.business.MarginProjectionView;
import backofficejava.business.ScenarioEvaluationHistoryReadModel;
import backofficejava.business.ScenarioEvaluationHistoryView;
import backofficejava.business.BacktestHistoryReadModel;
import backofficejava.business.BacktestHistoryView;
import backofficejava.business.AccountHierarchyReadModel;
import backofficejava.business.AccountHierarchyView;
import backofficejava.business.OperatorControlStateReadModel;
import backofficejava.business.OperatorControlStateView;
import com.sun.net.httpserver.HttpExchange;

import java.util.List;

public final class BackOfficeInternalHttpHandler extends JsonHttpHandler {
    public BackOfficeInternalHttpHandler(
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel,
        OrderProjectionStateStore orderProjectionStateStore,
        LedgerReadModel ledgerReadModel,
        ExecutionPackageReadModel executionPackageReadModel,
        PostTradePackageReadModel postTradePackageReadModel,
        ParentExecutionStateReadModel parentExecutionStateReadModel,
        AllocationStateReadModel allocationStateReadModel,
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
        super(exchange -> route(
            exchange,
            accountOverviewReadModel,
            positionReadModel,
            fillReadModel,
            orderProjectionStateStore,
            ledgerReadModel,
            executionPackageReadModel,
            postTradePackageReadModel,
            parentExecutionStateReadModel,
            allocationStateReadModel,
            settlementProjectionReadModel,
            statementProjectionReadModel,
            riskSnapshotReadModel,
            settlementExceptionWorkflowReadModel,
            corporateActionWorkflowReadModel,
            marginProjectionReadModel,
            scenarioEvaluationHistoryReadModel,
            backtestHistoryReadModel,
            accountHierarchyReadModel,
            operatorControlStateReadModel
        ));
    }

    private static JsonResponse route(
        HttpExchange exchange,
        AccountOverviewReadModel accountOverviewReadModel,
        PositionReadModel positionReadModel,
        FillReadModel fillReadModel,
        OrderProjectionStateStore orderProjectionStateStore,
        LedgerReadModel ledgerReadModel,
        ExecutionPackageReadModel executionPackageReadModel,
        PostTradePackageReadModel postTradePackageReadModel,
        ParentExecutionStateReadModel parentExecutionStateReadModel,
        AllocationStateReadModel allocationStateReadModel,
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
    ) throws Exception {
        String path = exchange.getRequestURI().getPath();
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/accounts/upsert".equals(path)) {
            AccountOverviewView request = readJson(exchange, AccountOverviewView.class);
            accountOverviewReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/positions/replace".equals(path)) {
            ReplacePositionsRequest request = readJson(exchange, ReplacePositionsRequest.class);
            positionReadModel.replacePositions(request.accountId(), request.positions());
            return JsonResponse.ok(new ReplacePositionsResponse("REPLACED", request.positions().size()));
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/fills/replace".equals(path)) {
            ReplaceFillsRequest request = readJson(exchange, ReplaceFillsRequest.class);
            fillReadModel.replaceFills(request.orderId(), request.fills());
            return JsonResponse.ok(new ReplaceFillsResponse("REPLACED", request.fills().size()));
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/orders/state/upsert".equals(path)) {
            OrderProjectionState request = readJson(exchange, OrderProjectionState.class);
            orderProjectionStateStore.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/ledger/replace".equals(path)) {
            ReplaceLedgerRequest request = readJson(exchange, ReplaceLedgerRequest.class);
            ledgerReadModel.reset();
            request.entries().forEach(ledgerReadModel::append);
            return JsonResponse.ok(new ReplaceLedgerResponse("REPLACED", request.entries().size()));
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/execution-package/upsert".equals(path)) {
            ExecutionPackageView request = readJson(exchange, ExecutionPackageView.class);
            executionPackageReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/post-trade-package/upsert".equals(path)) {
            PostTradePackageView request = readJson(exchange, PostTradePackageView.class);
            postTradePackageReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/parent-execution-state/upsert".equals(path)) {
            ParentExecutionStateView request = readJson(exchange, ParentExecutionStateView.class);
            parentExecutionStateReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/allocation-state/upsert".equals(path)) {
            AllocationStateView request = readJson(exchange, AllocationStateView.class);
            allocationStateReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/settlement-projection/upsert".equals(path)) {
            SettlementProjectionView request = readJson(exchange, SettlementProjectionView.class);
            settlementProjectionReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/statement-projection/upsert".equals(path)) {
            StatementProjectionView request = readJson(exchange, StatementProjectionView.class);
            statementProjectionReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/risk-snapshot/upsert".equals(path)) {
            RiskSnapshotView request = readJson(exchange, RiskSnapshotView.class);
            riskSnapshotReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/settlement-exception-workflow/upsert".equals(path)) {
            SettlementExceptionWorkflowView request = readJson(exchange, SettlementExceptionWorkflowView.class);
            settlementExceptionWorkflowReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/corporate-action-workflow/upsert".equals(path)) {
            CorporateActionWorkflowView request = readJson(exchange, CorporateActionWorkflowView.class);
            corporateActionWorkflowReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/margin-projection/upsert".equals(path)) {
            MarginProjectionView request = readJson(exchange, MarginProjectionView.class);
            marginProjectionReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/scenario-evaluation-history/upsert".equals(path)) {
            ScenarioEvaluationHistoryView request = readJson(exchange, ScenarioEvaluationHistoryView.class);
            scenarioEvaluationHistoryReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/backtest-history/upsert".equals(path)) {
            BacktestHistoryView request = readJson(exchange, BacktestHistoryView.class);
            backtestHistoryReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/account-hierarchy/upsert".equals(path)) {
            AccountHierarchyView request = readJson(exchange, AccountHierarchyView.class);
            accountHierarchyReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/business/operator-control-state/upsert".equals(path)) {
            OperatorControlStateView request = readJson(exchange, OperatorControlStateView.class);
            operatorControlStateReadModel.upsert(request);
            return JsonResponse.ok(request);
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && "/internal/reset".equals(path)) {
            accountOverviewReadModel.reset();
            positionReadModel.reset();
            fillReadModel.reset();
            orderProjectionStateStore.reset();
            ledgerReadModel.reset();
            executionPackageReadModel.reset();
            postTradePackageReadModel.reset();
            parentExecutionStateReadModel.reset();
            allocationStateReadModel.reset();
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
            return JsonResponse.ok(new ResetResponse("RESET"));
        }
        throw new NotFoundException("route_not_found:" + path);
    }

    public record ReplacePositionsRequest(String accountId, List<PositionView> positions) {
    }

    public record ReplacePositionsResponse(String status, int count) {
    }

    public record ReplaceFillsRequest(String orderId, List<FillView> fills) {
    }

    public record ReplaceFillsResponse(String status, int count) {
    }

    public record ReplaceLedgerRequest(List<LedgerEntryView> entries) {
    }

    public record ReplaceLedgerResponse(String status, int count) {
    }

    public record ResetResponse(String status) {
    }
}
