package backofficejava.http;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.FillReadModel;
import backofficejava.account.LedgerReadModel;
import backofficejava.account.OrderProjectionStateStore;
import backofficejava.account.PositionReadModel;
import backofficejava.business.AllocationStateReadModel;
import backofficejava.business.ExecutionPackageReadModel;
import backofficejava.business.ParentExecutionStateReadModel;
import backofficejava.business.PostTradePackageReadModel;
import backofficejava.business.SettlementProjectionReadModel;
import backofficejava.business.StatementProjectionReadModel;
import backofficejava.business.RiskSnapshotReadModel;
import backofficejava.business.SettlementExceptionWorkflowReadModel;
import backofficejava.business.CorporateActionWorkflowReadModel;
import backofficejava.business.MarginProjectionReadModel;
import backofficejava.business.ScenarioEvaluationHistoryReadModel;
import backofficejava.business.BacktestHistoryReadModel;
import com.sun.net.httpserver.HttpExchange;

public final class DemoResetHttpHandler extends JsonHttpHandler {
    public DemoResetHttpHandler(
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
        BacktestHistoryReadModel backtestHistoryReadModel
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
            backtestHistoryReadModel
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
        BacktestHistoryReadModel backtestHistoryReadModel
    ) {
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            return new JsonResponse(204, new ResetResponse("OK"));
        }
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            throw new MethodNotAllowedException(exchange.getRequestMethod());
        }
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
        return JsonResponse.ok(new ResetResponse("RESET"));
    }

    public record ResetResponse(String status) {
    }
}
