package oms.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import oms.order.InMemoryOrderReadModel;
import oms.order.OrderReadModel;
import oms.order.OrderStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GatewayAuditIntakeServiceTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @TempDir
    Path tempDir;

    @Test
    void replayBuildsFilledOrderAndReleasedReservation() throws Exception {
        Path auditPath = tempDir.resolve("audit.log");
        Files.writeString(auditPath, """
            {"type":"OrderAccepted","at":1711600000000,"accountId":"acct_demo","orderId":"ord_test_1","data":{"symbol":"7203","side":"BUY","type":"LIMIT","qty":100,"price":2800,"timeInForce":"GTC","expireAt":null,"clientOrderId":"cli-1"}}
            {"type":"ExecutionReport","at":1711600001000,"accountId":"acct_demo","orderId":"ord_test_1","data":{"status":"PARTIALLY_FILLED","filledQtyDelta":40,"filledQtyTotal":40,"price":2800}}
            {"type":"ExecutionReport","at":1711600002000,"accountId":"acct_demo","orderId":"ord_test_1","data":{"status":"FILLED","filledQtyDelta":60,"filledQtyTotal":100,"price":2795}}
            {"type":"OrderUpdated","at":1711600002001,"accountId":"acct_demo","orderId":"ord_test_1","data":{"status":"FILLED","filledQty":100}}
            """);

        Path statePath = tempDir.resolve("oms-state.json");
        Path offsetPath = tempDir.resolve("oms.offset");
        Path aggregateSequencePath = tempDir.resolve("oms-aggregate-sequence.json");
        System.setProperty("oms.state.path", statePath.toString());
        System.setProperty("oms.gateway.audit.path", auditPath.toString());
        System.setProperty("oms.gateway.audit.offset.path", offsetPath.toString());
        System.setProperty("oms.aggregate.sequence.path", aggregateSequencePath.toString());
        System.setProperty("oms.gateway.audit.enable", "true");
        System.setProperty("oms.gateway.audit.start.mode", "tail");

        try {
            OrderReadModel readModel = new InMemoryOrderReadModel("acct_demo");
            GatewayAuditIntakeService service = new GatewayAuditIntakeService(readModel);
            GatewayAuditIntakeService.ReplayResult replayResult = service.replayFromStart(true);

            assertNotNull(replayResult);
            assertEquals("REPLAYED", replayResult.status());

            var order = readModel.findById("ord_test_1").orElseThrow();
            assertEquals(OrderStatus.FILLED, order.status());
            assertEquals(100L, order.filledQuantity());
            assertEquals(0L, order.remainingQuantity());
            assertEquals(4, readModel.findEventsByOrderId("ord_test_1").size());
            assertEquals(0L, readModel.findReservationsByAccountId("acct_demo").getFirst().reservedAmount());
        } finally {
            System.clearProperty("oms.state.path");
            System.clearProperty("oms.gateway.audit.path");
            System.clearProperty("oms.gateway.audit.offset.path");
            System.clearProperty("oms.aggregate.sequence.path");
            System.clearProperty("oms.gateway.audit.enable");
            System.clearProperty("oms.gateway.audit.start.mode");
        }
    }

    @Test
    void replayConsumesPendingExecutionReportAfterAcceptance() throws Exception {
        Path auditPath = tempDir.resolve("audit-pending.log");
        Files.writeString(auditPath, """
            {"type":"ExecutionReport","at":1711600001000,"accountId":"acct_demo","orderId":"ord_test_pending","data":{"status":"FILLED","filledQtyDelta":100,"filledQtyTotal":100,"price":2800}}
            {"type":"OrderAccepted","at":1711600002000,"accountId":"acct_demo","orderId":"ord_test_pending","data":{"symbol":"7203","side":"BUY","type":"LIMIT","qty":100,"price":2800,"timeInForce":"GTC","expireAt":null,"clientOrderId":"cli-pending"}}
            {"type":"OrderUpdated","at":1711600002001,"accountId":"acct_demo","orderId":"ord_test_pending","data":{"status":"FILLED","filledQty":100}}
            """);

        System.setProperty("oms.state.path", tempDir.resolve("oms-pending-state.json").toString());
        System.setProperty("oms.gateway.audit.path", auditPath.toString());
        System.setProperty("oms.gateway.audit.offset.path", tempDir.resolve("oms-pending.offset").toString());
        System.setProperty("oms.pending.orphan.path", tempDir.resolve("oms-pending-orphans.json").toString());
        System.setProperty("oms.aggregate.sequence.path", tempDir.resolve("oms-pending-aggregate-sequence.json").toString());
        System.setProperty("oms.gateway.audit.enable", "true");
        System.setProperty("oms.gateway.audit.start.mode", "tail");

        try {
            OrderReadModel readModel = new InMemoryOrderReadModel("acct_demo");
            GatewayAuditIntakeService service = new GatewayAuditIntakeService(readModel);
            service.replayFromStart(true);

            var order = readModel.findById("ord_test_pending").orElseThrow();
            assertEquals(OrderStatus.FILLED, order.status());
            assertEquals(100L, order.filledQuantity());
            assertEquals(0, service.snapshot().pendingOrphanCount());
        } finally {
            System.clearProperty("oms.state.path");
            System.clearProperty("oms.gateway.audit.path");
            System.clearProperty("oms.gateway.audit.offset.path");
            System.clearProperty("oms.pending.orphan.path");
            System.clearProperty("oms.aggregate.sequence.path");
            System.clearProperty("oms.gateway.audit.enable");
            System.clearProperty("oms.gateway.audit.start.mode");
        }
    }

    @Test
    void busSequenceGapReplaysPendingExecutionAfterAcceptance() {
        System.setProperty("oms.state.path", tempDir.resolve("oms-bus-state.json").toString());
        System.setProperty("oms.pending.orphan.path", tempDir.resolve("oms-bus-pending.json").toString());
        System.setProperty("oms.aggregate.sequence.path", tempDir.resolve("oms-bus-aggregate-sequence.json").toString());

        try {
            OrderReadModel readModel = new InMemoryOrderReadModel("acct_demo");
            GatewayAuditIntakeService service = new GatewayAuditIntakeService(readModel);

            GatewayAuditEvent executionReport = new GatewayAuditEvent(
                "ExecutionReport",
                1711600001000L,
                "acct_demo",
                "ord_bus_1",
                OBJECT_MAPPER.readTree("""
                    {"status":"FILLED","filledQtyDelta":100,"filledQtyTotal":100,"price":2800}
                    """)
            );
            GatewayAuditIntakeService.IngestResult pending = service.ingestSequencedEvent(
                executionReport,
                "evt-bus-2",
                """
                {"eventId":"evt-bus-2","eventType":"ExecutionReport","schemaVersion":2,"sourceSystem":"gateway-rust","aggregateId":"ord_bus_1","aggregateSeq":2,"occurredAt":"2024-03-28T00:00:01Z","ingestedAt":"2024-03-28T00:00:01Z","accountId":"acct_demo","orderId":"ord_bus_1","venueOrderId":"venue-1","correlationId":"corr-1","causationId":"evt-bus-1","data":{"status":"FILLED","filledQtyDelta":100,"filledQtyTotal":100,"price":2800}}
                """,
                "ord_bus_1",
                2L
            );
            assertEquals("PENDING", pending.status());
            assertEquals(1, service.snapshot().pendingOrphanCount());

            GatewayAuditEvent accepted = new GatewayAuditEvent(
                "OrderAccepted",
                1711600000000L,
                "acct_demo",
                "ord_bus_1",
                OBJECT_MAPPER.readTree("""
                    {"symbol":"7203","side":"BUY","type":"LIMIT","qty":100,"price":2800,"timeInForce":"GTC","expireAt":null,"clientOrderId":"cli-bus"}
                    """)
            );
            GatewayAuditIntakeService.IngestResult applied = service.ingestSequencedEvent(
                accepted,
                "evt-bus-1",
                """
                {"eventId":"evt-bus-1","eventType":"OrderAccepted","schemaVersion":2,"sourceSystem":"gateway-rust","aggregateId":"ord_bus_1","aggregateSeq":1,"occurredAt":"2024-03-28T00:00:00Z","ingestedAt":"2024-03-28T00:00:00Z","accountId":"acct_demo","orderId":"ord_bus_1","venueOrderId":"venue-1","correlationId":"corr-1","causationId":"","data":{"symbol":"7203","side":"BUY","type":"LIMIT","qty":100,"price":2800,"timeInForce":"GTC","expireAt":null,"clientOrderId":"cli-bus"}}
                """,
                "ord_bus_1",
                1L
            );
            assertEquals("APPLIED", applied.status());

            var order = readModel.findById("ord_bus_1").orElseThrow();
            assertEquals(OrderStatus.FILLED, order.status());
            assertEquals(100L, order.filledQuantity());
            assertEquals(0, service.snapshot().pendingOrphanCount());
            assertEquals(1L, service.snapshot().sequenceGaps());
            assertEquals(1, service.snapshot().aggregateProgressCount());
        } catch (Exception exception) {
            throw new AssertionError(exception);
        } finally {
            System.clearProperty("oms.state.path");
            System.clearProperty("oms.pending.orphan.path");
            System.clearProperty("oms.aggregate.sequence.path");
        }
    }
}
