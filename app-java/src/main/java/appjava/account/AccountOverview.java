package appjava.account;

public record AccountOverview(
    String accountId,
    long cashBalance,
    long availableBuyingPower,
    long reservedBuyingPower,
    int positionCount,
    long realizedPnl,
    String updatedAt
) {
    public static AccountOverview empty(String accountId) {
        return new AccountOverview(accountId, 0L, 0L, 0L, 0, 0L, null);
    }
}
