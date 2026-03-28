package appjava.order;

public record BalanceEffectView(
    long cashDelta,
    long availableBuyingPowerDelta,
    long reservedBuyingPowerDelta,
    long realizedPnlDelta
) {
}
