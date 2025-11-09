package app.http

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.abs
import kotlin.random.Random

data class StockInfo(
    val symbol: String,
    val name: String,
    val currentPrice: Double,
    val change: Double,
    val changePercent: Double,
    val high: Double,
    val low: Double,
    val volume: Long
)

/**
 * 銘柄情報API（模擬マーケットデータ）
 * 定期的に全銘柄の価格を一斉更新
 */
class MarketDataController : HttpHandler, AutoCloseable {
    private val objectMapper = jacksonObjectMapper()

    // 模擬的な銘柄マスタデータ
    private val stockMaster = mapOf(
        "7203" to "トヨタ自動車",
        "6758" to "ソニーグループ",
        "9984" to "ソフトバンクグループ",
        "6861" to "キーエンス",
        "8306" to "三菱UFJフィナンシャル・グループ"
    )

    // 基準価格（模擬）
    private val basePrices = mapOf(
        "7203" to 2500.0,
        "6758" to 13500.0,
        "9984" to 6200.0,
        "6861" to 52000.0,
        "8306" to 1200.0
    )

    // 価格キャッシュ
    private val priceCache = ConcurrentHashMap<String, StockInfo>()
    private val lastUpdateTime = AtomicLong(0)

    // 定期更新用スケジューラ
    private val scheduler = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "MarketDataUpdater").apply { isDaemon = true }
    }

    companion object {
        private const val UPDATE_INTERVAL_MS = 1000L // 1秒ごとに価格更新
    }

    init {
        // 初回更新
        updateAllPrices()

        // 定期更新開始
        scheduler.scheduleAtFixedRate(
            ::updateAllPrices,
            UPDATE_INTERVAL_MS,
            UPDATE_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        )
    }

    override fun handle(exchange: HttpExchange) {
        try {
            // CORS headers
            exchange.responseHeaders.set("Access-Control-Allow-Origin", "*")
            exchange.responseHeaders.set("Access-Control-Allow-Methods", "GET, OPTIONS")
            exchange.responseHeaders.set("Access-Control-Allow-Headers", "Content-Type")

            if (exchange.requestMethod == "OPTIONS") {
                sendResponse(exchange, 204, "")
                return
            }

            if (exchange.requestMethod != "GET") {
                sendResponse(exchange, 405, "Method Not Allowed")
                return
            }

            // パスから銘柄コードを取得 (/api/market/7203 -> 7203)
            val path = exchange.requestURI.path
            val symbol = path.substringAfterLast("/")

            if (symbol.isEmpty() || !stockMaster.containsKey(symbol)) {
                sendResponse(exchange, 404, "Symbol not found")
                return
            }

            // キャッシュから取得（キャッシュがなければ生成）
            val stockInfo = priceCache[symbol] ?: generateStockInfo(symbol)
            val json = objectMapper.writeValueAsString(stockInfo)

            exchange.responseHeaders.set("Content-Type", "application/json")
            sendResponse(exchange, 200, json)
        } catch (e: Exception) {
            sendResponse(exchange, 500, "Internal Server Error: ${e.message}")
        } finally {
            exchange.close()
        }
    }

    private fun generateStockInfo(symbol: String): StockInfo {
        val basePrice = basePrices[symbol] ?: 1000.0
        val name = stockMaster[symbol] ?: "Unknown"

        // 模擬的な価格変動（±3%以内）
        val changePercent = (Random.nextDouble() - 0.5) * 6.0
        val change = basePrice * changePercent / 100.0
        val currentPrice = basePrice + change

        // 高値・安値（当日レンジ）
        val range = basePrice * 0.02
        val high = currentPrice + abs(Random.nextDouble()) * range
        val low = currentPrice - abs(Random.nextDouble()) * range

        // 出来高（ランダム）
        val volume = (Random.nextLong(1_000_000, 10_000_000))

        return StockInfo(
            symbol = symbol,
            name = name,
            currentPrice = String.format("%.2f", currentPrice).toDouble(),
            change = String.format("%.2f", change).toDouble(),
            changePercent = String.format("%.2f", changePercent).toDouble(),
            high = String.format("%.2f", high).toDouble(),
            low = String.format("%.2f", low).toDouble(),
            volume = volume
        )
    }

    private fun sendResponse(exchange: HttpExchange, statusCode: Int, body: String) {
        val bytes = body.toByteArray(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(statusCode, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }

    // 全銘柄の価格を一斉更新（バッチ処理）
    private fun updateAllPrices() {
        try {
            stockMaster.keys.forEach { symbol ->
                priceCache[symbol] = generateStockInfo(symbol)
            }
            lastUpdateTime.set(System.currentTimeMillis())
        } catch (e: Exception) {
            System.err.println("Error updating prices: ${e.message}")
        }
    }

    // PositionController用の内部メソッド（キャッシュから取得）
    fun getCurrentPrice(symbol: String): Double {
        return priceCache[symbol]?.currentPrice ?: run {
            // キャッシュがない場合は生成して返す
            val info = generateStockInfo(symbol)
            priceCache[symbol] = info
            info.currentPrice
        }
    }

    override fun close() {
        scheduler.shutdown()
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow()
            }
        } catch (e: InterruptedException) {
            scheduler.shutdownNow()
            Thread.currentThread().interrupt()
        }
    }
}
