package backofficejava.business;

import java.util.List;

public record MarginProjectionView(
    long generatedAt,
    String accountId,
    String methodology,
    double marginLimit,
    double marginUsed,
    double utilizationPercent,
    String breachStatus,
    List<String> breachedLimits,
    List<String> requiredActions,
    List<String> modelNotes
) {
}
