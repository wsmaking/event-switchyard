package backofficejava.audit;

import com.fasterxml.jackson.databind.JsonNode;

public record GatewayAuditEvent(
    String type,
    long at,
    String accountId,
    String orderId,
    JsonNode data
) {
}
