package backofficejava.business;

import java.util.List;

public record BacktestHistoryView(
    long generatedAt,
    String accountId,
    String windowLabel,
    String coverageLabel,
    double breachRatePercent,
    List<String> exceptions,
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
