package backofficejava.audit;

import backofficejava.account.InMemoryAccountOverviewReadModel;
import backofficejava.account.InMemoryFillReadModel;
import backofficejava.account.InMemoryLedgerReadModel;
import backofficejava.account.InMemoryOrderProjectionStateStore;
import backofficejava.account.InMemoryPositionReadModel;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    void replayBuildsLedgerBalancesAndPositions() throws Exception {
        Path auditPath = tempDir.resolve("audit.log");
        Files.writeString(auditPath, """
            {"type":"OrderAccepted","at":1711600000000,"accountId":"acct_demo","orderId":"ord_test_1","data":{"symbol":"7203","side":"BUY","type":"LIMIT","qty":100,"price":2800,"timeInForce":"GTC","expireAt":null,"clientOrderId":"cli-1"}}
            {"type":"ExecutionReport","at":1711600001000,"accountId":"acct_demo","orderId":"ord_test_1","data":{"status":"PARTIALLY_FILLED","filledQtyDelta":40,"filledQtyTotal":40,"price":2800}}
            {"type":"ExecutionReport","at":1711600002000,"accountId":"acct_demo","orderId":"ord_test_1","data":{"status":"FILLED","filledQtyDelta":60,"filledQtyTotal":100,"price":2795}}
            {"type":"OrderUpdated","at":1711600002001,"accountId":"acct_demo","orderId":"ord_test_1","data":{"status":"FILLED","filledQty":100}}
            """);

        System.setProperty("backoffice.accounts.path", tempDir.resolve("accounts.json").toString());
        System.setProperty("backoffice.positions.path", tempDir.resolve("positions.json").toString());
        System.setProperty("backoffice.fills.path", tempDir.resolve("fills.json").toString());
        System.setProperty("backoffice.ledger.path", tempDir.resolve("ledger.json").toString());
        System.setProperty("backoffice.order.state.path", tempDir.resolve("orders.json").toString());
        System.setProperty("backoffice.gateway.audit.path", auditPath.toString());
        System.setProperty("backoffice.gateway.audit.offset.path", tempDir.resolve("audit.offset").toString());
        System.setProperty("backoffice.aggregate.sequence.path", tempDir.resolve("aggregate-sequence.json").toString());
        System.setProperty("backoffice.gateway.audit.enable", "true");
        System.setProperty("backoffice.gateway.audit.start.mode", "tail");

        try {
            var accountReadModel = new InMemoryAccountOverviewReadModel("acct_demo");
            var positionReadModel = new InMemoryPositionReadModel("acct_demo");
            var fillReadModel = new InMemoryFillReadModel();
            var orderStateStore = new InMemoryOrderProjectionStateStore();
            var ledgerReadModel = new InMemoryLedgerReadModel();
            GatewayAuditIntakeService service = new GatewayAuditIntakeService(
                accountReadModel,
                positionReadModel,
                fillReadModel,
                orderStateStore,
                ledgerReadModel
            );

            GatewayAuditIntakeService.ReplayResult replayResult = service.replayFromStart(true);
            assertNotNull(replayResult);
            assertEquals("REPLAYED", replayResult.status());

            var overview = accountReadModel.findByAccountId("acct_demo").orElseThrow();
            assertEquals(9_720_300L, overview.cashBalance());
            assertEquals(0L, overview.reservedBuyingPower());
            assertEquals(9_720_300L, overview.availableBuyingPower());

            var positions = positionReadModel.findByAccountId("acct_demo");
            assertEquals(1, positions.size());
            assertEquals(100L, positions.getFirst().netQty());
            assertEquals(2797.0, positions.getFirst().avgPrice());

            var fills = fillReadModel.findByOrderId("ord_test_1");
            assertEquals(2, fills.size());
            assertEquals(4, ledgerReadModel.findByAccountId("acct_demo").size());
        } finally {
            System.clearProperty("backoffice.accounts.path");
            System.clearProperty("backoffice.positions.path");
            System.clearProperty("backoffice.fills.path");
            System.clearProperty("backoffice.ledger.path");
            System.clearProperty("backoffice.order.state.path");
            System.clearProperty("backoffice.gateway.audit.path");
            System.clearProperty("backoffice.gateway.audit.offset.path");
            System.clearProperty("backoffice.aggregate.sequence.path");
            System.clearProperty("backoffice.gateway.audit.enable");
            System.clearProperty("backoffice.gateway.audit.start.mode");
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

        System.setProperty("backoffice.accounts.path", tempDir.resolve("pending-accounts.json").toString());
        System.setProperty("backoffice.positions.path", tempDir.resolve("pending-positions.json").toString());
        System.setProperty("backoffice.fills.path", tempDir.resolve("pending-fills.json").toString());
        System.setProperty("backoffice.ledger.path", tempDir.resolve("pending-ledger.json").toString());
        System.setProperty("backoffice.order.state.path", tempDir.resolve("pending-orders.json").toString());
        System.setProperty("backoffice.pending.orphan.path", tempDir.resolve("pending-orphans.json").toString());
        System.setProperty("backoffice.gateway.audit.path", auditPath.toString());
        System.setProperty("backoffice.gateway.audit.offset.path", tempDir.resolve("pending-audit.offset").toString());
        System.setProperty("backoffice.aggregate.sequence.path", tempDir.resolve("pending-aggregate-sequence.json").toString());
        System.setProperty("backoffice.gateway.audit.enable", "true");
        System.setProperty("backoffice.gateway.audit.start.mode", "tail");

        try {
            var accountReadModel = new InMemoryAccountOverviewReadModel("acct_demo");
            var positionReadModel = new InMemoryPositionReadModel("acct_demo");
            var fillReadModel = new InMemoryFillReadModel();
            var orderStateStore = new InMemoryOrderProjectionStateStore();
            var ledgerReadModel = new InMemoryLedgerReadModel();
            GatewayAuditIntakeService service = new GatewayAuditIntakeService(
                accountReadModel,
                positionReadModel,
                fillReadModel,
                orderStateStore,
                ledgerReadModel
            );

            service.replayFromStart(true);

            assertEquals(0, service.snapshot().pendingOrphanCount());
            assertEquals(1, positionReadModel.findByAccountId("acct_demo").size());
            assertEquals(1, fillReadModel.findByOrderId("ord_test_pending").size());
        } finally {
            System.clearProperty("backoffice.accounts.path");
            System.clearProperty("backoffice.positions.path");
            System.clearProperty("backoffice.fills.path");
            System.clearProperty("backoffice.ledger.path");
            System.clearProperty("backoffice.order.state.path");
            System.clearProperty("backoffice.pending.orphan.path");
            System.clearProperty("backoffice.gateway.audit.path");
            System.clearProperty("backoffice.gateway.audit.offset.path");
            System.clearProperty("backoffice.aggregate.sequence.path");
            System.clearProperty("backoffice.gateway.audit.enable");
            System.clearProperty("backoffice.gateway.audit.start.mode");
        }
    }

    @Test
    void busSequenceGapReplaysPendingExecutionAfterAcceptance() {
        System.setProperty("backoffice.accounts.path", tempDir.resolve("bus-accounts.json").toString());
        System.setProperty("backoffice.positions.path", tempDir.resolve("bus-positions.json").toString());
        System.setProperty("backoffice.fills.path", tempDir.resolve("bus-fills.json").toString());
        System.setProperty("backoffice.ledger.path", tempDir.resolve("bus-ledger.json").toString());
        System.setProperty("backoffice.order.state.path", tempDir.resolve("bus-orders.json").toString());
        System.setProperty("backoffice.pending.orphan.path", tempDir.resolve("bus-pending-orphans.json").toString());
        System.setProperty("backoffice.aggregate.sequence.path", tempDir.resolve("bus-aggregate-sequence.json").toString());

        try {
            var accountReadModel = new InMemoryAccountOverviewReadModel("acct_demo");
            var positionReadModel = new InMemoryPositionReadModel("acct_demo");
            var fillReadModel = new InMemoryFillReadModel();
            var orderStateStore = new InMemoryOrderProjectionStateStore();
            var ledgerReadModel = new InMemoryLedgerReadModel();
            GatewayAuditIntakeService service = new GatewayAuditIntakeService(
                accountReadModel,
                positionReadModel,
                fillReadModel,
                orderStateStore,
                ledgerReadModel
            );

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
                "evt-bo-bus-2",
                """
                {"eventId":"evt-bo-bus-2","eventType":"ExecutionReport","schemaVersion":2,"sourceSystem":"gateway-rust","aggregateId":"ord_bus_1","aggregateSeq":2,"occurredAt":"2024-03-28T00:00:01Z","ingestedAt":"2024-03-28T00:00:01Z","accountId":"acct_demo","orderId":"ord_bus_1","venueOrderId":"venue-1","correlationId":"corr-1","causationId":"evt-bo-bus-1","data":{"status":"FILLED","filledQtyDelta":100,"filledQtyTotal":100,"price":2800}}
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
                "evt-bo-bus-1",
                """
                {"eventId":"evt-bo-bus-1","eventType":"OrderAccepted","schemaVersion":2,"sourceSystem":"gateway-rust","aggregateId":"ord_bus_1","aggregateSeq":1,"occurredAt":"2024-03-28T00:00:00Z","ingestedAt":"2024-03-28T00:00:00Z","accountId":"acct_demo","orderId":"ord_bus_1","venueOrderId":"venue-1","correlationId":"corr-1","causationId":"","data":{"symbol":"7203","side":"BUY","type":"LIMIT","qty":100,"price":2800,"timeInForce":"GTC","expireAt":null,"clientOrderId":"cli-bus"}}
                """,
                "ord_bus_1",
                1L
            );
            assertEquals("APPLIED", applied.status());

            assertEquals(0, service.snapshot().pendingOrphanCount());
            assertEquals(1L, service.snapshot().sequenceGaps());
            assertEquals(1, service.snapshot().aggregateProgressCount());
            assertEquals(1, positionReadModel.findByAccountId("acct_demo").size());
            assertEquals(1, fillReadModel.findByOrderId("ord_bus_1").size());
        } catch (Exception exception) {
            throw new AssertionError(exception);
        } finally {
            System.clearProperty("backoffice.accounts.path");
            System.clearProperty("backoffice.positions.path");
            System.clearProperty("backoffice.fills.path");
            System.clearProperty("backoffice.ledger.path");
            System.clearProperty("backoffice.order.state.path");
            System.clearProperty("backoffice.pending.orphan.path");
            System.clearProperty("backoffice.aggregate.sequence.path");
        }
    }
}
