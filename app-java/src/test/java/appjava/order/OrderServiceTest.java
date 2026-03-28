package appjava.order;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

final class OrderServiceTest {
    @Test
    void resolveFilledAtPreservesNullForNonFilledOrders() {
        assertNull(OrderService.resolveFilledAt(OrderStatus.ACCEPTED, 1_700_000_000_000L, null));
    }

    @Test
    void resolveFilledAtUsesSnapshotTimestampForFilledOrders() {
        assertEquals(Long.valueOf(1_700_000_000_000L), OrderService.resolveFilledAt(OrderStatus.FILLED, 1_700_000_000_000L, null));
    }

    @Test
    void resolvePricePreservesNullForMarketOrders() {
        assertNull(OrderService.resolvePrice(null, null));
    }

    @Test
    void resolvePriceUsesSnapshotPriceWhenPresent() {
        assertEquals(Double.valueOf(13500.0), OrderService.resolvePrice(13_500L, null));
    }
}
