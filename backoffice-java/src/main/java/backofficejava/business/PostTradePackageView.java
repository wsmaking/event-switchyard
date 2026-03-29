package backofficejava.business;

import java.util.List;

public record PostTradePackageView(
    long generatedAt,
    String orderId,
    String accountId,
    String symbol,
    String symbolName,
    String orderStatus,
    List<PostTradeStageView> stages,
    FeeBreakdownView feeBreakdown,
    StatementPreviewView statementPreview,
    List<SettlementCheckView> settlementChecks,
    List<CorporateActionHookView> corporateActionHooks
) {
    public record PostTradeStageView(
        String name,
        String owner,
        String purpose,
        String currentView,
        String whyItMatters
    ) {
    }

    public record FeeBreakdownView(
        long grossNotional,
        long commission,
        long exchangeFee,
        long taxes,
        long netCashMovement,
        List<String> assumptions
    ) {
    }

    public record StatementPreviewView(
        String accountId,
        String symbol,
        String symbolName,
        long settledQuantity,
        double averagePrice,
        String settlementDateLabel,
        String netCashMovementLabel,
        List<String> notes
    ) {
    }

    public record SettlementCheckView(
        String title,
        String rule,
        String currentValue
    ) {
    }

    public record CorporateActionHookView(
        String name,
        String businessImpact,
        String systemImpact
    ) {
    }
}
