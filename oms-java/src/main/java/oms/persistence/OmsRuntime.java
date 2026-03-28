package oms.persistence;

import oms.audit.AuditOffsetStore;
import oms.audit.DeadLetterStore;
import oms.audit.PendingOrphanStore;
import oms.order.OrderReadModel;

public record OmsRuntime(
    OrderReadModel orderReadModel,
    AuditOffsetStore auditOffsetStore,
    DeadLetterStore deadLetterStore,
    PendingOrphanStore pendingOrphanStore,
    String storeMode
) {
}
