package appjava.order;

import appjava.account.AccountOverview;
import appjava.position.UiPosition;

import java.util.List;

public record OrderFinalOut(
    OrderView order,
    AccountOverview accountOverview,
    BalanceEffectView balanceEffect,
    ExecutionQualityView executionQuality,
    List<ReservationView> reservations,
    List<FillView> fills,
    List<UiPosition> positions,
    List<OrderTimelineEntry> timeline,
    List<OrderEventView> rawEvents
) {
}
