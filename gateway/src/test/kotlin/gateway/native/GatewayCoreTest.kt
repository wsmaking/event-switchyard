package gateway.native

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable

/**
 * GatewayCore (Rust FFI) のテスト
 *
 * 実行方法:
 * ```bash
 * # Rustライブラリをビルド
 * cd gateway-core && cargo build --release
 *
 * # 環境変数を設定してテスト実行
 * GATEWAY_CORE_LIB_PATH=$(pwd)/gateway-core/target/release/libgateway_core.dylib \
 *   ./gradlew :gateway:test --tests "gateway.native.GatewayCoreTest"
 * ```
 */
@EnabledIfEnvironmentVariable(named = "GATEWAY_CORE_LIB_PATH", matches = ".+")
class GatewayCoreTest {

    companion object {
        @JvmStatic
        @BeforeAll
        fun setup() {
            val result = GatewayCore.init(65536)
            println("GatewayCore.init() = $result")
        }
    }

    @Test
    fun `test process order accepted`() {
        val result = GatewayCore.processOrder(
            orderId = 1L,
            accountId = 100L,
            symbol = "AAPL",
            side = 1, // buy
            qty = 100,
            price = 15000L,
            timestampNs = System.nanoTime()
        )
        assertEquals(GatewayCore.ResultCode.ACCEPTED, result)
    }

    @Test
    fun `test process order rejected max qty`() {
        val result = GatewayCore.processOrder(
            orderId = 2L,
            accountId = 100L,
            symbol = "AAPL",
            side = 1,
            qty = 100_000, // デフォルト上限10,000を超過
            price = 15000L,
            timestampNs = System.nanoTime()
        )
        assertEquals(GatewayCore.ResultCode.REJECTED_MAX_QTY, result)
    }

    @Test
    fun `test pop order`() {
        // 注文を投入
        GatewayCore.processOrder(
            orderId = 3L,
            accountId = 200L,
            symbol = "GOOG",
            side = 2, // sell
            qty = 50,
            price = 28000L,
            timestampNs = 12345L
        )

        // キューから取り出し
        val popped = GatewayCore.popOrder()
        assertNotNull(popped)
        assertEquals(3L, popped!!.orderId)
        assertEquals(200L, popped.accountId)
        assertEquals("GOOG", popped.symbolString())
        assertEquals(2.toByte(), popped.side)
        assertEquals(50, popped.qty)
        assertEquals(28000L, popped.price)
    }

    @Test
    fun `test queue operations`() {
        val initialLen = GatewayCore.queueLen()

        // 10件投入
        repeat(10) { i ->
            GatewayCore.processOrder(
                orderId = 100L + i,
                accountId = 1L,
                symbol = "TEST",
                side = 1,
                qty = 10,
                price = 1000L,
                timestampNs = System.nanoTime()
            )
        }

        assertEquals(initialLen + 10, GatewayCore.queueLen())

        // 全部取り出し
        repeat(10) {
            assertNotNull(GatewayCore.popOrder())
        }

        assertEquals(initialLen, GatewayCore.queueLen())
    }

    @Test
    fun `test latency metrics`() {
        GatewayCore.latencyReset()

        // 1000件処理
        repeat(1000) { i ->
            GatewayCore.processOrder(
                orderId = 1000L + i,
                accountId = 1L,
                symbol = "BENCH",
                side = 1,
                qty = 10,
                price = 1000L,
                timestampNs = System.nanoTime()
            )
        }

        // キューをクリア
        while (GatewayCore.popOrder() != null) { }

        val count = GatewayCore.latencyCount()
        val p50 = GatewayCore.latencyP50()
        val p99 = GatewayCore.latencyP99()
        val max = GatewayCore.latencyMax()

        println("Rust FFI Latency (1000 orders):")
        println("  count: $count")
        println("  p50: ${p50}ns")
        println("  p99: ${p99}ns")
        println("  max: ${max}ns")

        assertTrue(count >= 1000)
        assertTrue(p50 > 0)
        assertTrue(p99 >= p50)
    }
}
