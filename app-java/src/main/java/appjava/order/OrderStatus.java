package appjava.order;

public enum OrderStatus {
    PENDING_ACCEPT,
    ACCEPTED,
    PARTIALLY_FILLED,
    FILLED,
    CANCEL_PENDING,
    CANCELED,
    EXPIRED,
    REJECTED,
    AMEND_PENDING;

    public boolean isTerminal() {
        return this == FILLED || this == CANCELED || this == EXPIRED || this == REJECTED;
    }
}
