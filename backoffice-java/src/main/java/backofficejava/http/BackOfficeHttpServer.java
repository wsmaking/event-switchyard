package backofficejava.http;

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
import backofficejava.business.AccountHierarchyReadModel;
import backofficejava.business.OperatorControlStateReadModel;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public final class BackOfficeHttpServer {
    private final int port;
    private final AccountOverviewReadModel accountOverviewReadModel;
    private final PositionReadModel positionReadModel;
    private final FillReadModel fillReadModel;
    private final OrderProjectionStateStore orderProjectionStateStore;
    private final LedgerReadModel ledgerReadModel;
    private final ExecutionPackageReadModel executionPackageReadModel;
    private final PostTradePackageReadModel postTradePackageReadModel;
    private final ParentExecutionStateReadModel parentExecutionStateReadModel;
    private final AllocationStateReadModel allocationStateReadModel;
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
    private final GatewayAuditIntakeService intakeService;
    private final BusEventIntakeService busEventIntakeService;

    public BackOfficeHttpServer(
        int port,
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
        OperatorControlStateReadModel operatorControlStateReadModel,
        GatewayAuditIntakeService intakeService,
        BusEventIntakeService busEventIntakeService
    ) {
        this.port = port;
        this.accountOverviewReadModel = accountOverviewReadModel;
        this.positionReadModel = positionReadModel;
        this.fillReadModel = fillReadModel;
        this.orderProjectionStateStore = orderProjectionStateStore;
        this.ledgerReadModel = ledgerReadModel;
        this.executionPackageReadModel = executionPackageReadModel;
        this.postTradePackageReadModel = postTradePackageReadModel;
        this.parentExecutionStateReadModel = parentExecutionStateReadModel;
        this.allocationStateReadModel = allocationStateReadModel;
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
        this.intakeService = intakeService;
        this.busEventIntakeService = busEventIntakeService;
    }

    public void start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", new JsonHttpHandler(exchange ->
            JsonHttpHandler.JsonResponse.ok(new HealthResponse("UP", "backoffice-java"))
        ));
        server.createContext("/stats", new BackOfficeStatsHttpHandler(intakeService));
        server.createContext("/reconcile", new BackOfficeReconcileHttpHandler(intakeService));
        server.createContext("/orphans", new BackOfficeOrphanHttpHandler(intakeService));
        server.createContext("/orphans/pending", new BackOfficePendingOrphanHttpHandler(intakeService));
        server.createContext("/accounts", new AccountOverviewHttpHandler(accountOverviewReadModel));
        server.createContext("/positions", new PositionHttpHandler(positionReadModel));
        server.createContext("/fills", new FillHttpHandler(fillReadModel));
        server.createContext("/ledger", new LedgerHttpHandler(ledgerReadModel));
        server.createContext("/business/execution-package", new ExecutionPackageHttpHandler(executionPackageReadModel));
        server.createContext("/business/post-trade-package", new PostTradePackageHttpHandler(postTradePackageReadModel));
        server.createContext("/business/parent-execution-state", new ParentExecutionStateHttpHandler(parentExecutionStateReadModel));
        server.createContext("/business/allocation-state", new AllocationStateHttpHandler(allocationStateReadModel));
        server.createContext("/business/settlement-projection", new SettlementProjectionHttpHandler(settlementProjectionReadModel));
        server.createContext("/business/statement-projection", new StatementProjectionHttpHandler(statementProjectionReadModel));
        server.createContext("/business/risk-snapshot", new RiskSnapshotHttpHandler(riskSnapshotReadModel));
        server.createContext("/business/settlement-exception-workflow", new SettlementExceptionWorkflowHttpHandler(settlementExceptionWorkflowReadModel));
        server.createContext("/business/corporate-action-workflow", new CorporateActionWorkflowHttpHandler(corporateActionWorkflowReadModel));
        server.createContext("/business/margin-projection", new MarginProjectionHttpHandler(marginProjectionReadModel));
        server.createContext("/business/scenario-evaluation-history", new ScenarioEvaluationHistoryHttpHandler(scenarioEvaluationHistoryReadModel));
        server.createContext("/business/backtest-history", new BacktestHistoryHttpHandler(backtestHistoryReadModel));
        server.createContext("/business/account-hierarchy", new AccountHierarchyHttpHandler(accountHierarchyReadModel));
        server.createContext("/business/operator-control-state", new OperatorControlStateHttpHandler(operatorControlStateReadModel));
        server.createContext(
            "/demo/reset",
            new DemoResetHttpHandler(
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
            )
        );
        server.createContext(
            "/internal",
            new BackOfficeInternalHttpHandler(
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
            )
        );
        server.createContext("/internal/audit", new BackOfficeInternalAuditHttpHandler(intakeService));
        server.createContext("/internal/bus", new BackOfficeInternalBusHttpHandler(busEventIntakeService));
        server.createContext("/internal/orphans", new BackOfficeInternalOrphanHttpHandler(intakeService));
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
        System.out.println("backoffice-java listening on http://localhost:" + port);
    }

    public record HealthResponse(String status, String service) {
    }
}
