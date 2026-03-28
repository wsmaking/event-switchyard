package oms.audit;

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
        System.setProperty("oms.state.path", statePath.toString());
        System.setProperty("oms.gateway.audit.path", auditPath.toString());
        System.setProperty("oms.gateway.audit.offset.path", offsetPath.toString());
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
            System.clearProperty("oms.gateway.audit.enable");
            System.clearProperty("oms.gateway.audit.start.mode");
        }
    }
}
