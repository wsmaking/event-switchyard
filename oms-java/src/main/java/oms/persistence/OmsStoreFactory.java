package oms.persistence;

import oms.audit.AuditOffsetStore;
import oms.audit.AggregateSequenceStore;
import oms.audit.DeadLetterStore;
import oms.audit.FileAuditOffsetStore;
import oms.audit.InMemoryAggregateSequenceStore;
import oms.audit.InMemoryDeadLetterStore;
import oms.audit.InMemoryPendingOrphanStore;
import oms.audit.JdbcAggregateSequenceStore;
import oms.audit.JdbcAuditOffsetStore;
import oms.audit.JdbcDeadLetterStore;
import oms.audit.JdbcPendingOrphanStore;
import oms.audit.PendingOrphanStore;
import oms.order.InMemoryOrderReadModel;
import oms.order.JdbcOrderReadModel;
import oms.order.OrderReadModel;

import java.nio.file.Files;
import java.nio.file.Path;

public final class OmsStoreFactory {
    private OmsStoreFactory() {
    }

    public static OmsRuntime create(String accountId) {
        String storeMode = System.getenv().getOrDefault("OMS_STORE_MODE", "memory").trim().toLowerCase();
        if ("postgres".equals(storeMode)) {
            JdbcConnectionFactory connectionFactory = JdbcConnectionFactory.fromEnvironment("OMS", "oms");
            SqlMigrationRunner.run(
                connectionFactory,
                "db/migration/V1__oms_schema.sql",
                "db/migration/V2__oms_orphan_state.sql",
                "db/migration/V3__oms_aggregate_progress.sql"
            );
            OrderReadModel orderReadModel = new JdbcOrderReadModel(connectionFactory);
            AuditOffsetStore offsetStore = new JdbcAuditOffsetStore(connectionFactory, "gateway-audit");
            DeadLetterStore deadLetterStore = new JdbcDeadLetterStore(connectionFactory);
            PendingOrphanStore pendingOrphanStore = new JdbcPendingOrphanStore(connectionFactory);
            AggregateSequenceStore aggregateSequenceStore = new JdbcAggregateSequenceStore(connectionFactory);
            return new OmsRuntime(orderReadModel, offsetStore, deadLetterStore, pendingOrphanStore, aggregateSequenceStore, "postgres");
        }

        Path offsetPath = resolvePath(
            System.getProperty(
                "oms.gateway.audit.offset.path",
                System.getenv().getOrDefault("OMS_GATEWAY_AUDIT_OFFSET_PATH", "var/java-replay/oms/audit.offset")
            )
        );
        return new OmsRuntime(
            new InMemoryOrderReadModel(accountId),
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
