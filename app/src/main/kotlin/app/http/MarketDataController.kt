package app.http

import app.fast.OrderBook
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import java.nio.charset.StandardCharsets
import java.net.URLDecoder
import java.util.ArrayDeque
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random
import kotlin.math.roundToInt

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

data class PricePoint(
    val timestamp: Long,
    val price: Double
)

data class PriceLevel(
    val price: Int,
    val quantity: Int,
    val orders: Int = 1
)

data class OrderBookDepth(
    val symbol: String,
    val timestamp: Long,
    val bids: List<PriceLevel>,  // 買い板（価格降順）
    val asks: List<PriceLevel>   // 売り板（価格昇順）
)

data class MarketSpec(
    val basePrice: Double,
    val tickSize: Double,
    val volatilityBps: Double,
    val meanReversionBps: Double,
    val maxMovePercent: Double,
    val volumeStepRange: LongRange
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

    // 銘柄ごとの価格特性（ティック・ボラ・最大変動・出来高）
    private val marketSpecs = mapOf(
        "7203" to MarketSpec(
            basePrice = 2500.0,
            tickSize = 1.0,
            volatilityBps = 4.0,
            meanReversionBps = 3.0,
            maxMovePercent = 10.0,
            volumeStepRange = 500L..4_000L
        ),
        "6758" to MarketSpec(
            basePrice = 13500.0,
            tickSize = 5.0,
            volatilityBps = 5.0,
            meanReversionBps = 3.0,
            maxMovePercent = 12.0,
            volumeStepRange = 300L..2_500L
        ),
        "9984" to MarketSpec(
            basePrice = 6200.0,
            tickSize = 1.0,
            volatilityBps = 6.0,
            meanReversionBps = 3.5,
            maxMovePercent = 12.0,
            volumeStepRange = 400L..3_500L
        ),
        "6861" to MarketSpec(
            basePrice = 52000.0,
            tickSize = 10.0,
            volatilityBps = 6.5,
            meanReversionBps = 4.0,
            maxMovePercent = 14.0,
            volumeStepRange = 80L..800L
        ),
        "8306" to MarketSpec(
            basePrice = 1200.0,
            tickSize = 0.5,
            volatilityBps = 3.5,
            meanReversionBps = 2.5,
            maxMovePercent = 8.0,
            volumeStepRange = 900L..6_000L
        )
    )

    // 価格キャッシュ
    private val priceCache = ConcurrentHashMap<String, StockInfo>()
    private val lastUpdateTime = AtomicLong(0)
    private val priceHistory = ConcurrentHashMap<String, ArrayDeque<PricePoint>>()
    private val sessionOpen = ConcurrentHashMap<String, Double>()

    // OrderBook管理（銘柄ごと）
    private val orderBooks = ConcurrentHashMap<String, OrderBook>()

    // 定期更新用スケジューラ
    private val scheduler = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "MarketDataUpdater").apply { isDaemon = true }
    }

    companion object {
        private const val UPDATE_INTERVAL_MS = 1000L // 1秒ごとに価格更新
        private const val HISTORY_LIMIT = 300
    }

    init {
        // 各銘柄のOrderBookを初期化
        stockMaster.keys.forEach { symbol ->
            val symbolId = symbol.toIntOrNull() ?: 0
            orderBooks[symbol] = OrderBook(symbolId)
        }

        // 初回更新
        updateAllPrices()
        populateInitialOrders()

        // 定期更新開始
        scheduler.scheduleAtFixedRate(
            ::updateAllPrices,
            UPDATE_INTERVAL_MS,
            UPDATE_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        )

        // 模擬的な注文生成（板を活性化）
        scheduler.scheduleAtFixedRate(
            ::generateRandomOrders,
            500L,
            500L,
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

            // パスから銘柄コードとエンドポイントを取得
            val path = exchange.requestURI.path

            // /api/market-data/7203/orderbook の場合
            when {
                path.endsWith("/orderbook") -> {
                    val symbol = path.split("/").getOrNull(3) ?: ""
                    if (symbol.isEmpty() || !stockMaster.containsKey(symbol)) {
                        sendResponse(exchange, 404, "Symbol not found")
                        return
                    }
                    handleOrderBook(exchange, symbol)
                }
                path.endsWith("/history") -> {
                    val symbol = path.split("/").getOrNull(3) ?: ""
                    if (symbol.isEmpty() || !stockMaster.containsKey(symbol)) {
                        sendResponse(exchange, 404, "Symbol not found")
                        return
                    }
                    val params = parseQuery(exchange.requestURI.rawQuery ?: "")
                    val limit = params["limit"]?.toIntOrNull()
                    handleHistory(exchange, symbol, limit)
                }
                else -> {
                    // /api/market-data/7203 の場合（価格情報）
                    val symbol = path.substringAfterLast("/")
                    if (symbol.isEmpty() || !stockMaster.containsKey(symbol)) {
                        sendResponse(exchange, 404, "Symbol not found")
                        return
                    }
                    val stockInfo = priceCache[symbol] ?: generateStockInfo(symbol, priceCache[symbol])
                    val json = objectMapper.writeValueAsString(stockInfo)
                    exchange.responseHeaders.set("Content-Type", "application/json")
                    sendResponse(exchange, 200, json)
                }
            }
        } catch (e: Exception) {
            sendResponse(exchange, 500, "Internal Server Error: ${e.message}")
        } finally {
            exchange.close()
        }
    }

    private fun generateStockInfo(symbol: String, previous: StockInfo? = priceCache[symbol]): StockInfo {
        val spec = marketSpecs[symbol] ?: MarketSpec(1000.0, 1.0, 5.0, 2.0, 12.0, 500L..3_000L)
        val name = stockMaster[symbol] ?: "Unknown"
        val basePrice = spec.basePrice

        sessionOpen.putIfAbsent(symbol, basePrice)
        val openPrice = sessionOpen[symbol] ?: basePrice

        // 模擬的な価格変動（bpsベースの小刻み揺らぎ + 平均回帰）
        val lastPrice = previous?.currentPrice ?: basePrice
        val noiseBps = Random.nextDouble(-spec.volatilityBps, spec.volatilityBps)
        val reversionBps = ((basePrice - lastPrice) / basePrice) * spec.meanReversionBps
        val moveBps = noiseBps + reversionBps
        val unclamped = lastPrice * (1.0 + moveBps / 10_000.0)
        val cap = spec.maxMovePercent / 100.0
        val clamped = unclamped.coerceIn(basePrice * (1.0 - cap), basePrice * (1.0 + cap))
        val currentPrice = applyTick(clamped, spec.tickSize)

        // 高値・安値は直近の履歴を引き継ぐ
        val high = maxOf(previous?.high ?: currentPrice, currentPrice)
        val low = minOf(previous?.low ?: currentPrice, currentPrice)

        val change = currentPrice - openPrice
        val changePercent = change / openPrice * 100.0

        // 出来高（擬似的に積み上げ）
        val volumeStep = Random.nextLong(spec.volumeStepRange.first, spec.volumeStepRange.last + 1)
        val volume = (previous?.volume ?: 0L) + volumeStep

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
            val now = System.currentTimeMillis()
            stockMaster.keys.forEach { symbol ->
                val info = generateStockInfo(symbol, priceCache[symbol])
                priceCache[symbol] = info
                recordHistory(symbol, info.currentPrice, now)
            }
            lastUpdateTime.set(now)
        } catch (e: Exception) {
            System.err.println("Error updating prices: ${e.message}")
        }
    }

    private fun applyTick(price: Double, tickSize: Double): Double {
        if (tickSize <= 0.0) return price
        val ticks = (price / tickSize).roundToInt()
        return ticks * tickSize
    }

    // PositionController用の内部メソッド（キャッシュから取得）
    fun getCurrentPrice(symbol: String): Double {
        return priceCache[symbol]?.currentPrice ?: run {
            // キャッシュがない場合は生成して返す
            val info = generateStockInfo(symbol, priceCache[symbol])
            priceCache[symbol] = info
            info.currentPrice
        }
    }

    // WebSocket用の公開メソッド（板情報取得）
    fun getOrderBookDepthPublic(symbol: String): OrderBookDepth? {
        val orderBook = orderBooks[symbol] ?: return null
        return getOrderBookDepth(symbol, orderBook)
    }

    // OrderController用の公開メソッド（OrderBook取得）
    fun getOrderBook(symbol: String): OrderBook? {
        return orderBooks[symbol]
    }

    // OrderBook板情報エンドポイント
    private fun handleOrderBook(exchange: HttpExchange, symbol: String) {
        val orderBook = orderBooks[symbol]
        if (orderBook == null) {
            sendResponse(exchange, 404, "OrderBook not found for symbol: $symbol")
            return
        }

        // OrderBookから板情報を取得（現時点ではダミーデータ）
        val depth = getOrderBookDepth(symbol, orderBook)
        val json = objectMapper.writeValueAsString(depth)

        exchange.responseHeaders.set("Content-Type", "application/json")
        sendResponse(exchange, 200, json)
    }

    private fun handleHistory(exchange: HttpExchange, symbol: String, limit: Int?) {
        val deque = priceHistory[symbol]
        val points = if (deque == null) {
            emptyList()
        } else {
            synchronized(deque) { deque.toList() }
        }
        val trimmed =
            if (limit != null && limit > 0 && points.size > limit) {
                points.takeLast(limit)
            } else {
                points
            }
        val json = objectMapper.writeValueAsString(trimmed)
        exchange.responseHeaders.set("Content-Type", "application/json")
        sendResponse(exchange, 200, json)
    }

    private fun recordHistory(symbol: String, price: Double, timestamp: Long) {
        val deque = priceHistory.computeIfAbsent(symbol) { ArrayDeque() }
        synchronized(deque) {
            deque.addLast(PricePoint(timestamp = timestamp, price = price))
            while (deque.size > HISTORY_LIMIT) {
                deque.removeFirst()
            }
        }
    }

    private fun parseQuery(q: String): Map<String, String> {
        if (q.isEmpty()) return emptyMap()
        val pairs = mutableListOf<Pair<String, String>>()
        for (item in q.split('&')) {
            if (item.isEmpty()) continue
            val idx = item.indexOf('=')
            val k = if (idx >= 0) item.substring(0, idx) else item
            val v = if (idx >= 0) item.substring(idx + 1) else ""
            pairs += URLDecoder.decode(k, Charsets.UTF_8) to URLDecoder.decode(v, Charsets.UTF_8)
        }
        return pairs.toMap()
    }

    // OrderBookから5レベル板情報を生成
    private fun getOrderBookDepth(symbol: String, orderBook: OrderBook): OrderBookDepth {
        val spec = marketSpecs[symbol] ?: MarketSpec(1000.0, 1.0, 5.0, 2.0, 12.0, 500L..3_000L)
        val referencePrice = priceCache[symbol]?.currentPrice ?: spec.basePrice
        val priceInt = referencePrice.roundToInt()

        // 模擬的な5レベル板情報を生成
        val bids = (1..5).map { level ->
            PriceLevel(
                price = priceInt - level * 10,
                quantity = Random.nextInt(100, 1000),
                orders = Random.nextInt(1, 10)
            )
        }

        val asks = (1..5).map { level ->
            PriceLevel(
                price = priceInt + level * 10,
                quantity = Random.nextInt(100, 1000),
                orders = Random.nextInt(1, 10)
            )
        }

        return OrderBookDepth(
            symbol = symbol,
            timestamp = System.currentTimeMillis(),
            bids = bids,
            asks = asks
        )
    }

    // 初期注文を投入
    private fun populateInitialOrders() {
        stockMaster.keys.forEach { symbol ->
            val orderBook = orderBooks[symbol] ?: return@forEach
            val spec = marketSpecs[symbol] ?: MarketSpec(1000.0, 1.0, 5.0, 2.0, 12.0, 500L..3_000L)
            val basePrice = spec.basePrice.roundToInt()

            // 買い注文を投入
            repeat(10) {
                val price = basePrice - Random.nextInt(10, 50)
                val quantity = Random.nextInt(100, 1000)
                orderBook.processBuyOrder(price, quantity, System.nanoTime())
            }

            // 売り注文を投入
            repeat(10) {
                val price = basePrice + Random.nextInt(10, 50)
                val quantity = Random.nextInt(100, 1000)
                orderBook.processSellOrder(price, quantity, System.nanoTime())
            }
        }
    }

    // ランダムに注文を生成（板を活性化）
    private fun generateRandomOrders() {
        stockMaster.keys.forEach { symbol ->
            val orderBook = orderBooks[symbol] ?: return@forEach
            val spec = marketSpecs[symbol] ?: MarketSpec(1000.0, 1.0, 5.0, 2.0, 12.0, 500L..3_000L)
            val basePrice = (priceCache[symbol]?.currentPrice ?: spec.basePrice).roundToInt()

            // 50%の確率で注文を生成
            if (Random.nextBoolean()) {
                if (Random.nextBoolean()) {
                    // 買い注文
                    val price = basePrice - Random.nextInt(5, 30)
                    val quantity = Random.nextInt(50, 500)
                    orderBook.processBuyOrder(price, quantity, System.nanoTime())
                } else {
                    // 売り注文
                    val price = basePrice + Random.nextInt(5, 30)
                    val quantity = Random.nextInt(50, 500)
                    orderBook.processSellOrder(price, quantity, System.nanoTime())
                }
            }
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
