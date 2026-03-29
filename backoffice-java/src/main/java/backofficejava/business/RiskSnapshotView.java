package backofficejava.business;

import java.util.List;

public record RiskSnapshotView(
    long generatedAt,
    String accountId,
    double marketValue,
    double cashBalance,
    List<ConcentrationMetricView> concentration,
    List<LiquidityMetricView> liquidity,
    List<ScenarioLibraryEntryView> scenarioLibrary,
    BacktestingPreviewView backtesting,
    List<ModelBoundaryView> modelBoundaries,
    List<String> marginAlerts
) {
    public record ConcentrationMetricView(
        String symbol,
        String symbolName,
        double exposure,
        double weightPercent,
        String note
    ) {
    }

    public record LiquidityMetricView(
        String symbol,
        String symbolName,
        long positionQuantity,
        long visibleTopOfBookQuantity,
        double participationPercent,
        double estimatedDaysToExit,
        String note
    ) {
    }

    public record ScenarioLibraryEntryView(
        String id,
        String title,
        String category,
        String shock,
        String rationale,
        String focus
    ) {
    }

    public record BacktestingPreviewView(
        int observationCount,
        double breachRatePercent,
        double averageTailLoss,
        String note,
        List<BacktestSampleView> samples
    ) {
    }

    public record BacktestSampleView(
        String label,
        double pnl,
        boolean breached
    ) {
    }

    public record ModelBoundaryView(
        String title,
        String whyItMatters,
        String whatIncluded,
        String whatExcluded
    ) {
    }
}
