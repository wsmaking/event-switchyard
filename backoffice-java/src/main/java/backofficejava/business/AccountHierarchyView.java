package backofficejava.business;

import java.util.List;

public record AccountHierarchyView(
    long generatedAt,
    String accountId,
    String clientName,
    String legalEntity,
    String region,
    String desk,
    String strategy,
    String clearingBroker,
    String custodian,
    List<BookHierarchyView> books,
    List<PermissionGrantView> permissions,
    List<String> reportingLines,
    List<String> controlChecks
) {
    public record BookHierarchyView(
        String fund,
        String book,
        String subAccount,
        String trader,
        String settlementLocation,
        String mandate
    ) {
    }

    public record PermissionGrantView(
        String role,
        String scope,
        List<String> actions,
        boolean approvalRequired,
        String note
    ) {
    }
}
