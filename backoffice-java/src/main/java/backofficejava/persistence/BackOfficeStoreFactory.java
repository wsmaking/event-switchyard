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
import backofficejava.audit.AuditOffsetStore;
import backofficejava.audit.DeadLetterStore;
import backofficejava.audit.FileAuditOffsetStore;
import backofficejava.audit.InMemoryDeadLetterStore;
import backofficejava.audit.InMemoryPendingOrphanStore;
import backofficejava.audit.JdbcAuditOffsetStore;
import backofficejava.audit.JdbcDeadLetterStore;
import backofficejava.audit.JdbcPendingOrphanStore;
import backofficejava.audit.PendingOrphanStore;

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
                "db/migration/V2__backoffice_orphan_state.sql"
            );
            JdbcBackOfficeStore store = new JdbcBackOfficeStore(connectionFactory, accountId);
            AuditOffsetStore offsetStore = new JdbcAuditOffsetStore(connectionFactory, "gateway-audit");
            DeadLetterStore deadLetterStore = new JdbcDeadLetterStore(connectionFactory);
            PendingOrphanStore pendingOrphanStore = new JdbcPendingOrphanStore(connectionFactory);
            return new BackOfficeRuntime(
                store.accountOverviewReadModel(),
                store.positionReadModel(),
                store.fillReadModel(),
                store.orderProjectionStateStore(),
                store.ledgerReadModel(),
                offsetStore,
                deadLetterStore,
                pendingOrphanStore,
                "postgres"
            );
        }

        AccountOverviewReadModel accountOverviewReadModel = new InMemoryAccountOverviewReadModel(accountId);
        PositionReadModel positionReadModel = new InMemoryPositionReadModel(accountId);
        FillReadModel fillReadModel = new InMemoryFillReadModel();
        OrderProjectionStateStore orderProjectionStateStore = new InMemoryOrderProjectionStateStore();
        LedgerReadModel ledgerReadModel = new InMemoryLedgerReadModel();
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
            new FileAuditOffsetStore(offsetPath),
            new InMemoryDeadLetterStore(),
            new InMemoryPendingOrphanStore(),
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
