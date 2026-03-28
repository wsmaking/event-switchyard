package backofficejava.account;

import java.time.Instant;

public record AccountOverviewView(
    String accountId,
    long cashBalance,
    long availableBuyingPower,
    long reservedBuyingPower,
    int positionCount,
    long realizedPnl,
    Instant updatedAt
) {
}
