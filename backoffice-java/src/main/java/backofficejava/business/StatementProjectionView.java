package backofficejava.business;

import java.util.List;

public record StatementProjectionView(
    long generatedAt,
    String orderId,
    String accountId,
    String symbol,
    String statementStatus,
    String confirmReference,
    String statementReference,
    String customerFacingSummary,
    List<StatementLineView> lines,
    List<String> controls
) {
    public record StatementLineView(
        String label,
        String value,
        String note
    ) {
    }
}
