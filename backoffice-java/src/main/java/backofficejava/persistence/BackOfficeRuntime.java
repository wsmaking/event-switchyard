package backofficejava.persistence;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.FillReadModel;
import backofficejava.account.LedgerReadModel;
import backofficejava.account.OrderProjectionStateStore;
import backofficejava.account.PositionReadModel;
import backofficejava.audit.AggregateSequenceStore;
import backofficejava.audit.AuditOffsetStore;
import backofficejava.audit.DeadLetterStore;
import backofficejava.audit.PendingOrphanStore;
import backofficejava.business.ExecutionPackageReadModel;
import backofficejava.business.PostTradePackageReadModel;

public record BackOfficeRuntime(
    AccountOverviewReadModel accountOverviewReadModel,
    PositionReadModel positionReadModel,
    FillReadModel fillReadModel,
    OrderProjectionStateStore orderProjectionStateStore,
    LedgerReadModel ledgerReadModel,
    ExecutionPackageReadModel executionPackageReadModel,
    PostTradePackageReadModel postTradePackageReadModel,
    AuditOffsetStore auditOffsetStore,
    DeadLetterStore deadLetterStore,
    PendingOrphanStore pendingOrphanStore,
    AggregateSequenceStore aggregateSequenceStore,
    String storeMode
) {
}
