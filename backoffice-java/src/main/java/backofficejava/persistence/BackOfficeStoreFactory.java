package backofficejava.persistence;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.FillReadModel;
import backofficejava.account.InMemoryAccountOverviewReadModel;
import backofficejava.account.InMemoryFillReadModel;
import backofficejava.account.InMemoryLedgerReadModel;
import backofficejava.account.InMemoryOrderProjectionStateStore;
import backofficejava.account.InMemoryPositionReadModel;
import backofficejava.account.JdbcBackOfficeStore;
import backofficejava.account.LedgerReadModel;
import backofficejava.account.OrderProjectionStateStore;
import backofficejava.account.PositionReadModel;
import backofficejava.audit.AggregateSequenceStore;
import backofficejava.audit.AuditOffsetStore;
import backofficejava.audit.DeadLetterStore;
import backofficejava.audit.FileAuditOffsetStore;
import backofficejava.audit.InMemoryAggregateSequenceStore;
import backofficejava.audit.InMemoryDeadLetterStore;
import backofficejava.audit.InMemoryPendingOrphanStore;
import backofficejava.audit.JdbcAggregateSequenceStore;
import backofficejava.audit.JdbcAuditOffsetStore;
import backofficejava.audit.JdbcDeadLetterStore;
import backofficejava.audit.JdbcPendingOrphanStore;
import backofficejava.audit.PendingOrphanStore;
import backofficejava.business.ExecutionPackageReadModel;
import backofficejava.business.AllocationStateReadModel;
import backofficejava.business.InMemoryExecutionPackageReadModel;
import backofficejava.business.InMemoryAllocationStateReadModel;
import backofficejava.business.InMemoryParentExecutionStateReadModel;
import backofficejava.business.InMemoryPostTradePackageReadModel;
import backofficejava.business.InMemorySettlementProjectionReadModel;
import backofficejava.business.InMemoryStatementProjectionReadModel;
import backofficejava.business.InMemoryRiskSnapshotReadModel;
import backofficejava.business.JdbcBusinessPackageStore;
import backofficejava.business.ParentExecutionStateReadModel;
import backofficejava.business.PostTradePackageReadModel;
import backofficejava.business.SettlementProjectionReadModel;
import backofficejava.business.StatementProjectionReadModel;
import backofficejava.business.RiskSnapshotReadModel;

import java.nio.file.Files;
import java.nio.file.Path;

public final class BackOfficeStoreFactory {
    private BackOfficeStoreFactory() {
    }

    public static BackOfficeRuntime create(String accountId) {
        String storeMode = System.getenv().getOrDefault("BACKOFFICE_STORE_MODE", "memory").trim().toLowerCase();
        if ("postgres".equals(storeMode)) {
            JdbcConnectionFactory connectionFactory = JdbcConnectionFactory.fromEnvironment("BACKOFFICE", "backoffice");
            SqlMigrationRunner.run(
                connectionFactory,
                "db/migration/V1__backoffice_schema.sql",
                "db/migration/V2__backoffice_orphan_state.sql",
                "db/migration/V3__backoffice_aggregate_progress.sql",
                "db/migration/V4__backoffice_business_packages.sql",
                "db/migration/V5__backoffice_institutional_states.sql",
                "db/migration/V6__backoffice_post_trade_projections.sql",
                "db/migration/V7__backoffice_risk_snapshots.sql"
            );
            JdbcBackOfficeStore store = new JdbcBackOfficeStore(connectionFactory, accountId);
            JdbcBusinessPackageStore businessPackageStore = new JdbcBusinessPackageStore(connectionFactory);
            AuditOffsetStore offsetStore = new JdbcAuditOffsetStore(connectionFactory, "gateway-audit");
            DeadLetterStore deadLetterStore = new JdbcDeadLetterStore(connectionFactory);
            PendingOrphanStore pendingOrphanStore = new JdbcPendingOrphanStore(connectionFactory);
            AggregateSequenceStore aggregateSequenceStore = new JdbcAggregateSequenceStore(connectionFactory);
            return new BackOfficeRuntime(
                store.accountOverviewReadModel(),
                store.positionReadModel(),
                store.fillReadModel(),
                store.orderProjectionStateStore(),
                store.ledgerReadModel(),
                businessPackageStore.executionPackageReadModel(),
                businessPackageStore.postTradePackageReadModel(),
                businessPackageStore.parentExecutionStateReadModel(),
                businessPackageStore.allocationStateReadModel(),
                businessPackageStore.settlementProjectionReadModel(),
                businessPackageStore.statementProjectionReadModel(),
                businessPackageStore.riskSnapshotReadModel(),
                offsetStore,
                deadLetterStore,
                pendingOrphanStore,
                aggregateSequenceStore,
                "postgres"
            );
        }

        AccountOverviewReadModel accountOverviewReadModel = new InMemoryAccountOverviewReadModel(accountId);
        PositionReadModel positionReadModel = new InMemoryPositionReadModel(accountId);
        FillReadModel fillReadModel = new InMemoryFillReadModel();
        OrderProjectionStateStore orderProjectionStateStore = new InMemoryOrderProjectionStateStore();
        LedgerReadModel ledgerReadModel = new InMemoryLedgerReadModel();
        ExecutionPackageReadModel executionPackageReadModel = new InMemoryExecutionPackageReadModel();
        PostTradePackageReadModel postTradePackageReadModel = new InMemoryPostTradePackageReadModel();
        ParentExecutionStateReadModel parentExecutionStateReadModel = new InMemoryParentExecutionStateReadModel();
        AllocationStateReadModel allocationStateReadModel = new InMemoryAllocationStateReadModel();
        SettlementProjectionReadModel settlementProjectionReadModel = new InMemorySettlementProjectionReadModel();
        StatementProjectionReadModel statementProjectionReadModel = new InMemoryStatementProjectionReadModel();
        RiskSnapshotReadModel riskSnapshotReadModel = new InMemoryRiskSnapshotReadModel();
        Path offsetPath = resolvePath(
            System.getProperty(
                "backoffice.gateway.audit.offset.path",
                System.getenv().getOrDefault("BACKOFFICE_GATEWAY_AUDIT_OFFSET_PATH", "var/java-replay/backoffice/audit.offset")
            )
        );
        return new BackOfficeRuntime(
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
            new FileAuditOffsetStore(offsetPath),
            new InMemoryDeadLetterStore(),
            new InMemoryPendingOrphanStore(),
            new InMemoryAggregateSequenceStore(),
            "memory"
        );
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
}
