package backofficejava.persistence;

import backofficejava.account.AccountOverviewReadModel;
import backofficejava.account.FillReadModel;
import backofficejava.account.LedgerReadModel;
import backofficejava.account.OrderProjectionStateStore;
import backofficejava.account.PositionReadModel;
import backofficejava.audit.AuditOffsetStore;

public record BackOfficeRuntime(
    AccountOverviewReadModel accountOverviewReadModel,
    PositionReadModel positionReadModel,
    FillReadModel fillReadModel,
    OrderProjectionStateStore orderProjectionStateStore,
    LedgerReadModel ledgerReadModel,
    AuditOffsetStore auditOffsetStore,
    String storeMode
) {
}
