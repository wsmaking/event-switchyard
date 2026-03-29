package backofficejava.business;

import java.util.List;

public record BacktestHistoryView(
    long generatedAt,
    String accountId,
    String windowLabel,
    double breachRatePercent,
    List<BacktestHistoryPointView> history
) {
    public record BacktestHistoryPointView(
        String label,
        double pnl,
        boolean breached,
        String note
    ) {
    }
}
