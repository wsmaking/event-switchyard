package oms.bus;

import com.fasterxml.jackson.databind.JsonNode;

public record BusEventV2(
    String eventId,
    String eventType,
    int schemaVersion,
    String sourceSystem,
    String aggregateId,
    long aggregateSeq,
    String occurredAt,
    String ingestedAt,
    String accountId,
    String orderId,
    String venueOrderId,
    String correlationId,
    String causationId,
    JsonNode data
) {
}
