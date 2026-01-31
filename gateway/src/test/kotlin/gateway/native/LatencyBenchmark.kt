package gateway.native

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import java.util.concurrent.ArrayBlockingQueue

/**
 * Kotlin (JVM) vs Rust (FFI) レイテンシ比較ベンチマーク
 *
 * 3方式を比較:
 * 1. Kotlin純正 (ArrayBlockingQueue)
 * 2. Rust FFI + JNA
 * 3. Rust FFI + Panama (Foreign Function & Memory API)
 *
 * 実行方法:
 * ```bash
 * GATEWAY_CORE_LIB_PATH=$(pwd)/gateway-core/target/release/libgateway_core.dylib \
 *   ./gradlew :gateway:test --tests "gateway.native.LatencyBenchmark" \
 *   --jvmargs="--enable-native-access=ALL-UNNAMED"
 * ```
 */
@EnabledIfEnvironmentVariable(named = "GATEWAY_CORE_LIB_PATH", matches = ".+")
class LatencyBenchmark {

    companion object {
        private const val WARMUP_COUNT = 10_000
        private const val BENCH_COUNT = 100_000

        // Panama初期化済みフラグ（JNAと別インスタンスなので別途初期化が必要）
        private var panamaInitialized = false

        @JvmStatic
        @BeforeAll
        fun setup() {
            GatewayCore.init(65536)
            // Panama版は遅延初期化（テスト実行時にフラグで制御）
        }
    }

    /**
     * Kotlin純正実装のベンチマーク
     *
     * 計測範囲: リスクチェック + キュー投入（ArrayBlockingQueue）
     */
    @Test
    fun `benchmark Kotlin native`() {
        data class Order(
            val orderId: Long,
            val accountId: Long,
            val symbol: String,
            val side: Byte,
            val qty: Int,
            val price: Long,
            val timestampNs: Long
        )

        // シンプルなリスクチェック
        fun checkRisk(order: Order): Boolean {
            if (order.qty > 10_000) return false
            if (order.price * order.qty > 1_000_000_000L) return false
            return true
        }

        val queue = ArrayBlockingQueue<Order>(65536)
        val latencies = LongArray(BENCH_COUNT)

        // ウォームアップ
        repeat(WARMUP_COUNT) { i ->
            val order = Order(i.toLong(), 1, "AAPL", 1, 100, 15000, System.nanoTime())
            checkRisk(order)
            queue.offer(order)
            queue.poll()
        }

        // 計測
        repeat(BENCH_COUNT) { i ->
            val start = System.nanoTime()

            val order = Order(i.toLong(), 1, "AAPL", 1, 100, 15000, start)
            checkRisk(order)
            queue.offer(order)

            val elapsed = System.nanoTime() - start
            latencies[i] = elapsed
            queue.poll()
        }

        latencies.sort()
        val p50 = latencies[BENCH_COUNT / 2]
        val p99 = latencies[(BENCH_COUNT * 0.99).toInt()]
        val max = latencies[BENCH_COUNT - 1]
        val mean = latencies.average().toLong()

        println()
        println("=" .repeat(60))
        println("Kotlin (JVM) - ArrayBlockingQueue + リスクチェック")
        println("=" .repeat(60))
        println("  サンプル数: $BENCH_COUNT")
        println("  平均: ${mean}ns")
        println("  p50:  ${p50}ns")
        println("  p99:  ${p99}ns")
        println("  最大: ${max}ns")
        println()
    }

    /**
     * Rust FFI 実装のベンチマーク
     *
     * 計測範囲: Rust側のリスクチェック + キュー投入
     * ※ JNAのオーバーヘッドを含む
     */
    @Test
    fun `benchmark Rust FFI`() {
        GatewayCore.latencyReset()
        val latencies = LongArray(BENCH_COUNT)

        // ウォームアップ
        repeat(WARMUP_COUNT) { i ->
            GatewayCore.processOrder(
                i.toLong(), 1, "AAPL", 1, 100, 15000, System.nanoTime()
            )
            GatewayCore.popOrder()
        }

        GatewayCore.latencyReset()

        // 計測（Kotlin側でも計測）
        repeat(BENCH_COUNT) { i ->
            val start = System.nanoTime()

            GatewayCore.processOrder(
                i.toLong(), 1, "AAPL", 1, 100, 15000, start
            )

            val elapsed = System.nanoTime() - start
            latencies[i] = elapsed
            GatewayCore.popOrder()
        }

        latencies.sort()
        val p50Kotlin = latencies[BENCH_COUNT / 2]
        val p99Kotlin = latencies[(BENCH_COUNT * 0.99).toInt()]
        val maxKotlin = latencies[BENCH_COUNT - 1]
        val meanKotlin = latencies.average().toLong()

        // Rust内部のレイテンシ（JNAオーバーヘッド除外）
        val p50Rust = GatewayCore.latencyP50()
        val p99Rust = GatewayCore.latencyP99()
        val maxRust = GatewayCore.latencyMax()

        println()
        println("=" .repeat(60))
        println("Rust (FFI) - crossbeam + リスクチェック")
        println("=" .repeat(60))
        println("【Kotlin側計測（JNAオーバーヘッド込み）】")
        println("  サンプル数: $BENCH_COUNT")
        println("  平均: ${meanKotlin}ns")
        println("  p50:  ${p50Kotlin}ns")
        println("  p99:  ${p99Kotlin}ns")
        println("  最大: ${maxKotlin}ns")
        println()
        println("【Rust内部計測（JNAオーバーヘッド除外）】")
        println("  p50:  ${p50Rust}ns")
        println("  p99:  ${p99Rust}ns")
        println("  最大: ${maxRust}ns")
        println()
        println("【JNAオーバーヘッド推定】")
        println("  p50:  約${p50Kotlin - p50Rust}ns")
        println("  p99:  約${p99Kotlin - p99Rust}ns")
        println()
    }

    /**
     * Rust FFI + Panama 実装のベンチマーク
     *
     * 計測範囲: Rust側のリスクチェック + キュー投入
     * ※ Panamaのオーバーヘッドを含む（JNAより大幅に低い想定）
     *
     * 注意: JNA版と同じRustライブラリを使うため、グローバル状態を共有する
     *       そのため、このテストは単独で実行するか、JNA版の後に実行する必要がある
     */
    @Test
    fun `benchmark Rust FFI Panama`() {
        // Panama版はJNA版と同じライブラリを使うので、初期化は不要
        // （JNA版のsetup()で既に初期化済み）
        GatewayCorePanama.latencyReset()
        val latencies = LongArray(BENCH_COUNT)

        // ウォームアップ
        repeat(WARMUP_COUNT) { i ->
            GatewayCorePanama.processOrder(
                i.toLong(), 1, "AAPL", 1, 100, 15000, System.nanoTime()
            )
            GatewayCorePanama.popOrder()
        }

        GatewayCorePanama.latencyReset()

        // 計測（Kotlin側でも計測）
        repeat(BENCH_COUNT) { i ->
            val start = System.nanoTime()

            GatewayCorePanama.processOrder(
                i.toLong(), 1, "AAPL", 1, 100, 15000, start
            )

            val elapsed = System.nanoTime() - start
            latencies[i] = elapsed
            GatewayCorePanama.popOrder()
        }

        latencies.sort()
        val p50Kotlin = latencies[BENCH_COUNT / 2]
        val p99Kotlin = latencies[(BENCH_COUNT * 0.99).toInt()]
        val maxKotlin = latencies[BENCH_COUNT - 1]
        val meanKotlin = latencies.average().toLong()

        // Rust内部のレイテンシ（Panamaオーバーヘッド除外）
        val p50Rust = GatewayCorePanama.latencyP50()
        val p99Rust = GatewayCorePanama.latencyP99()
        val maxRust = GatewayCorePanama.latencyMax()

        println()
        println("=" .repeat(60))
        println("Rust (FFI) + Panama - crossbeam + リスクチェック")
        println("=" .repeat(60))
        println("【Kotlin側計測（Panamaオーバーヘッド込み）】")
        println("  サンプル数: $BENCH_COUNT")
        println("  平均: ${meanKotlin}ns")
        println("  p50:  ${p50Kotlin}ns")
        println("  p99:  ${p99Kotlin}ns")
        println("  最大: ${maxKotlin}ns")
        println()
        println("【Rust内部計測（Panamaオーバーヘッド除外）】")
        println("  p50:  ${p50Rust}ns")
        println("  p99:  ${p99Rust}ns")
        println("  最大: ${maxRust}ns")
        println()
        println("【Panamaオーバーヘッド推定】")
        println("  p50:  約${p50Kotlin - p50Rust}ns")
        println("  p99:  約${p99Kotlin - p99Rust}ns")
        println()
    }

    /**
     * 結果サマリー（3方式比較）
     */
    @Test
    fun `print comparison summary`() {
        // Kotlin版
        data class Order(
            val orderId: Long,
            val accountId: Long,
            val symbol: String,
            val side: Byte,
            val qty: Int,
            val price: Long,
            val timestampNs: Long
        )

        fun checkRisk(order: Order): Boolean {
            if (order.qty > 10_000) return false
            if (order.price * order.qty > 1_000_000_000L) return false
            return true
        }

        val queue = ArrayBlockingQueue<Order>(65536)
        val kotlinLatencies = LongArray(BENCH_COUNT)

        repeat(WARMUP_COUNT) { i ->
            val order = Order(i.toLong(), 1, "AAPL", 1, 100, 15000, System.nanoTime())
            checkRisk(order)
            queue.offer(order)
            queue.poll()
        }

        repeat(BENCH_COUNT) { i ->
            val start = System.nanoTime()
            val order = Order(i.toLong(), 1, "AAPL", 1, 100, 15000, start)
            checkRisk(order)
            queue.offer(order)
            kotlinLatencies[i] = System.nanoTime() - start
            queue.poll()
        }

        // Rust JNA版
        GatewayCore.latencyReset()
        val rustJnaLatencies = LongArray(BENCH_COUNT)

        repeat(WARMUP_COUNT) { i ->
            GatewayCore.processOrder(i.toLong(), 1, "AAPL", 1, 100, 15000, System.nanoTime())
            GatewayCore.popOrder()
        }
        GatewayCore.latencyReset()

        repeat(BENCH_COUNT) { i ->
            val start = System.nanoTime()
            GatewayCore.processOrder(i.toLong(), 1, "AAPL", 1, 100, 15000, start)
            rustJnaLatencies[i] = System.nanoTime() - start
            GatewayCore.popOrder()
        }

        val rustJnaInternalP50 = GatewayCore.latencyP50()
        val rustJnaInternalP99 = GatewayCore.latencyP99()

        // Rust Panama版
        GatewayCorePanama.latencyReset()
        val rustPanamaLatencies = LongArray(BENCH_COUNT)

        repeat(WARMUP_COUNT) { i ->
            GatewayCorePanama.processOrder(i.toLong(), 1, "AAPL", 1, 100, 15000, System.nanoTime())
            GatewayCorePanama.popOrder()
        }
        GatewayCorePanama.latencyReset()

        repeat(BENCH_COUNT) { i ->
            val start = System.nanoTime()
            GatewayCorePanama.processOrder(i.toLong(), 1, "AAPL", 1, 100, 15000, start)
            rustPanamaLatencies[i] = System.nanoTime() - start
            GatewayCorePanama.popOrder()
        }

        val rustPanamaInternalP50 = GatewayCorePanama.latencyP50()
        val rustPanamaInternalP99 = GatewayCorePanama.latencyP99()

        // ソート
        kotlinLatencies.sort()
        rustJnaLatencies.sort()
        rustPanamaLatencies.sort()

        val kotlinP50 = kotlinLatencies[BENCH_COUNT / 2]
        val kotlinP99 = kotlinLatencies[(BENCH_COUNT * 0.99).toInt()]
        val rustJnaP50 = rustJnaLatencies[BENCH_COUNT / 2]
        val rustJnaP99 = rustJnaLatencies[(BENCH_COUNT * 0.99).toInt()]
        val rustPanamaP50 = rustPanamaLatencies[BENCH_COUNT / 2]
        val rustPanamaP99 = rustPanamaLatencies[(BENCH_COUNT * 0.99).toInt()]

        println()
        println("=" .repeat(75))
        println("  レイテンシ比較サマリー ($BENCH_COUNT samples)")
        println("=" .repeat(75))
        println()
        println("  %-35s %12s %12s".format("", "p50", "p99"))
        println("  " + "-".repeat(59))
        println("  %-35s %10dns %10dns".format("Kotlin (ArrayBlockingQueue)", kotlinP50, kotlinP99))
        println("  %-35s %10dns %10dns".format("Rust FFI + JNA", rustJnaP50, rustJnaP99))
        println("  %-35s %10dns %10dns".format("Rust FFI + Panama", rustPanamaP50, rustPanamaP99))
        println("  %-35s %10dns %10dns".format("Rust内部 (FFIオーバーヘッド除外)", rustPanamaInternalP50, rustPanamaInternalP99))
        println()
        println("  【オーバーヘッド比較 (p99)】")
        println("    JNA:    ${rustJnaP99 - rustJnaInternalP99}ns")
        println("    Panama: ${rustPanamaP99 - rustPanamaInternalP99}ns")
        println()
        println("  【Kotlin比 改善率 (p99)】")
        println("    vs Rust内部: %.1fx".format(kotlinP99.toDouble() / rustPanamaInternalP99))
        println("    vs Panama:   %.1fx".format(kotlinP99.toDouble() / rustPanamaP99))
        println()
    }
}
