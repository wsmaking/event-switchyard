package backofficejava;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.FillReadModel;
import backofficejava.account.LedgerReadModel;
import backofficejava.account.OrderProjectionStateStore;
import backofficejava.account.PositionReadModel;
import backofficejava.audit.GatewayAuditIntakeService;
import backofficejava.bus.BusEventIntakeService;
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
import backofficejava.http.BackOfficeHttpServer;
import backofficejava.persistence.BackOfficeRuntime;
import backofficejava.persistence.BackOfficeStoreFactory;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getProperty("backoffice.http.port", "18082"));
        String accountId = System.getProperty("backoffice.account.id", System.getenv().getOrDefault("ACCOUNT_ID", "acct_demo"));
        BackOfficeRuntime runtime = BackOfficeStoreFactory.create(accountId);
        AccountOverviewReadModel accountOverviewReadModel = runtime.accountOverviewReadModel();
        PositionReadModel positionReadModel = runtime.positionReadModel();
        FillReadModel fillReadModel = runtime.fillReadModel();
        OrderProjectionStateStore orderProjectionStateStore = runtime.orderProjectionStateStore();
        LedgerReadModel ledgerReadModel = runtime.ledgerReadModel();
        ExecutionPackageReadModel executionPackageReadModel = runtime.executionPackageReadModel();
        PostTradePackageReadModel postTradePackageReadModel = runtime.postTradePackageReadModel();
        ParentExecutionStateReadModel parentExecutionStateReadModel = runtime.parentExecutionStateReadModel();
        AllocationStateReadModel allocationStateReadModel = runtime.allocationStateReadModel();
        SettlementProjectionReadModel settlementProjectionReadModel = runtime.settlementProjectionReadModel();
        StatementProjectionReadModel statementProjectionReadModel = runtime.statementProjectionReadModel();
        RiskSnapshotReadModel riskSnapshotReadModel = runtime.riskSnapshotReadModel();
        SettlementExceptionWorkflowReadModel settlementExceptionWorkflowReadModel = runtime.settlementExceptionWorkflowReadModel();
        CorporateActionWorkflowReadModel corporateActionWorkflowReadModel = runtime.corporateActionWorkflowReadModel();
        MarginProjectionReadModel marginProjectionReadModel = runtime.marginProjectionReadModel();
        ScenarioEvaluationHistoryReadModel scenarioEvaluationHistoryReadModel = runtime.scenarioEvaluationHistoryReadModel();
        BacktestHistoryReadModel backtestHistoryReadModel = runtime.backtestHistoryReadModel();
        GatewayAuditIntakeService intakeService = new GatewayAuditIntakeService(
            accountOverviewReadModel,
            positionReadModel,
            fillReadModel,
            orderProjectionStateStore,
            ledgerReadModel,
            runtime.auditOffsetStore(),
            runtime.deadLetterStore(),
            runtime.pendingOrphanStore(),
            runtime.aggregateSequenceStore()
        );
        BusEventIntakeService busEventIntakeService = new BusEventIntakeService(intakeService);
        BackOfficeHttpServer server = new BackOfficeHttpServer(
            port,
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
            intakeService,
            busEventIntakeService
        );
        server.start();
        intakeService.start();
        busEventIntakeService.start();
        System.out.println("backoffice-java store mode=" + runtime.storeMode());
    }
}
