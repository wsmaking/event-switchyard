package app.engine

import app.fast.FastPathEngine
import java.util.concurrent.atomic.AtomicLong

/**
 * レイテンシ要件に基づいて、リクエストをFast PathまたはSlow Pathにルーティングする
 *
 * 環境変数:
 * - FAST_PATH_ENABLE: "1"でFast Pathを有効化 (デフォルト: "0")
 * - FAST_PATH_SYMBOLS: Fast Path対象のシンボルをカンマ区切りで指定 (例: "BTC,ETH")
 * - FAST_PATH_FALLBACK: "1"でFast Path失敗時にSlow Pathへフォールバック (デフォルト: "1")
 */
class Router(
    private val slowPath: Engine,
    private val fastPathEnabled: Boolean = System.getenv("FAST_PATH_ENABLE") == "1",
    private val fastPathSymbols: Set<String> = parseFastPathSymbols(),
    private val fallbackEnabled: Boolean = System.getenv("FAST_PATH_FALLBACK") != "0"
) : AutoCloseable {

    private val fastPath: FastPathEngine? = if (fastPathEnabled) FastPathEngine() else null

    private val fastPathCount = AtomicLong(0)
    private val slowPathCount = AtomicLong(0)
    private val fallbackCount = AtomicLong(0)

    /**
     * キーに基づいてリクエストをルーティング
     * @return 処理された場合true、拒否された場合false (オーナーシップ)
     */
    fun handle(key: String, payload: ByteArray): Boolean {
        // Fast Path判定
        if (fastPath != null && shouldUseFastPath(key)) {
            val published = fastPath.tryPublish(key, payload)

            if (published) {
                fastPathCount.incrementAndGet()
                return true
            }

            // Fast Pathバッファが満杯
            if (fallbackEnabled) {
                fallbackCount.incrementAndGet()
                return handleSlowPath(key, payload)
            }

            return false  // フォールバック無効時はドロップ
        }

        // Slow Path (既存処理)
        return handleSlowPath(key, payload)
    }

    private fun handleSlowPath(key: String, payload: ByteArray): Boolean {
        val handled = slowPath.handle(key, payload)
        if (handled) {
            slowPathCount.incrementAndGet()
        }
        return handled
    }

    private fun shouldUseFastPath(key: String): Boolean {
        // シンボル未指定の場合、全てFast Pathを使用
        if (fastPathSymbols.isEmpty()) return true

        // キーがFast Path対象シンボルに含まれるか確認
        return fastPathSymbols.contains(key)
    }

    fun getStats(): RouterStats {
        return RouterStats(
            fastPathCount = fastPathCount.get(),
            slowPathCount = slowPathCount.get(),
            fallbackCount = fallbackCount.get(),
            fastPathMetrics = fastPath?.getMetrics()?.snapshot()
        )
    }

    override fun close() {
        fastPath?.close()
    }

    companion object {
        private fun parseFastPathSymbols(): Set<String> {
            val symbols = System.getenv("FAST_PATH_SYMBOLS") ?: return emptySet()
            return symbols.split(',')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .toSet()
        }
    }
}

data class RouterStats(
    val fastPathCount: Long,
    val slowPathCount: Long,
    val fallbackCount: Long,
    val fastPathMetrics: app.fast.MetricsSnapshot?
) {
    fun toMap(): Map<String, Any> {
        val map = mutableMapOf<String, Any>(
            "fast_path_count" to fastPathCount,
            "slow_path_count" to slowPathCount,
            "fallback_count" to fallbackCount,
            "fast_path_ratio" to if (totalCount() > 0) fastPathCount.toDouble() / totalCount() else 0.0
        )

        fastPathMetrics?.let {
            map["fast_path_avg_publish_us"] = it.avgPublishLatencyUs()
            map["fast_path_avg_process_us"] = it.avgProcessLatencyUs()
            map["fast_path_drop_count"] = it.dropCount
        }

        return map
    }

    private fun totalCount() = fastPathCount + slowPathCount
}
