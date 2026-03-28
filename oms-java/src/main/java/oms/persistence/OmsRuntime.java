package oms.persistence;

import oms.audit.AuditOffsetStore;
import oms.order.OrderReadModel;

public record OmsRuntime(
    OrderReadModel orderReadModel,
    AuditOffsetStore auditOffsetStore,
    String storeMode
) {
}
