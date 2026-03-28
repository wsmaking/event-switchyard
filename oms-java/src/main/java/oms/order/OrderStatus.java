package oms.order;

public enum OrderStatus {
    PENDING_ACCEPT,
    ACCEPTED,
    REJECTED,
    PARTIALLY_FILLED,
    FILLED,
    CANCEL_PENDING,
    CANCELED,
    EXPIRED,
    AMEND_PENDING;

    public boolean isTerminal() {
        return this == REJECTED || this == FILLED || this == CANCELED || this == EXPIRED;
    }
}
