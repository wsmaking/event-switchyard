package appjava.order;

import appjava.account.AccountOverview;
import appjava.position.UiPosition;

import java.util.List;

public record OrderFinalOut(
    OrderView order,
    AccountOverview accountOverview,
    List<UiPosition> positions,
    List<OrderTimelineEntry> timeline
) {
}
