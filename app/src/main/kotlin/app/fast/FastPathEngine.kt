package app.fast

import app.audit.AuditLogger
import com.lmax.disruptor.*
import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.dsl.ProducerType
import org.agrona.collections.Object2ObjectHashMap
import org.HdrHistogram.Histogram
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicLong

/**
 * LMAX Disruptorを使用したHFT Fast Pathエンジン
 *
 * 目標: p99 < 100μs, Zero GC
 *
 * 設計方針:
 * - シングルプロデューサー/コンシューマーでロックフリー動作
 * - イベント事前割り当て (ホットパスでのアロケーション無し)
 * - シンボルのインターン化でString生成を回避
 * - Chronicle Queue書き込みはPersistence Queueへ委譲 (非同期)
 */
class FastPathEngine(
    bufferSize: Int = 65536,  // 2の累乗である必要あり
    enableMetrics: Boolean = System.getenv("FAST_PATH_METRICS") == "1",
    private val persistenceQueue: PersistenceQueueEngine? = null,  // 永続化キュー (非同期)
    private val auditLogger: AuditLogger? = null  // 監査ログ (Tick-by-Tick記録)
) : AutoCloseable {

    private val disruptor: Disruptor<TradeEvent>
    private val ringBuffer: RingBuffer<TradeEvent>
    private val metrics = if (enableMetrics) FastPathMetrics() else null

    // シンボルテーブル: 文字列のインターン化でGC回避
    private val symbolTable = SymbolTable()

    init {
        val threadFactory = ThreadFactory { r ->
            Thread(r, "fast-path-engine").apply {
                isDaemon = false
                priority = Thread.MAX_PRIORITY
            }
        }

        disruptor = Disruptor(
            ::TradeEvent,
            bufferSize,
            threadFactory,
            ProducerType.SINGLE,
            YieldingWaitStrategy()  // 最低レイテンシ、高CPU使用率
        )

        // イベントハンドラーを登録
        disruptor.handleEventsWith(TradeEventHandler(metrics, persistenceQueue, symbolTable, auditLogger))
        disruptor.start()

        ringBuffer = disruptor.ringBuffer
    }

    /**
     * Fast Pathへイベントを公開 (ノンブロッキング)
     * @return 公開成功でtrue、バッファ満杯でfalse
     */
    fun tryPublish(key: String, payload: ByteArray): Boolean {
        val receiveNs = System.nanoTime()

        // 監査ログ: イベント受信
        auditLogger?.logEventReceived(key, payload, receiveNs)

        val available = ringBuffer.tryPublishEvent { event, _ ->
            val startNs = System.nanoTime()

            event.timestamp = startNs
            event.symbolId = symbolTable.intern(key)
            event.payloadSize = payload.size.toShort()
            // TODO: ゼロコピーのためoff-heapバッファへペイロードをコピー
            System.arraycopy(payload, 0, event.inlinePayload, 0, minOf(payload.size, 256))

            metrics?.recordPublish(System.nanoTime() - startNs)
        }

        if (!available) {
            metrics?.recordDrop()
            // 監査ログ: イベントドロップ (バッファ満杯)
            auditLogger?.logEventDropped(key, "disruptor_buffer_full", receiveNs)
        }

        return available
    }

    fun getMetrics(): FastPathMetrics? = metrics

    override fun close() {
        disruptor.shutdown()
    }
}

/**
 * 事前割り当てイベント (Disruptorによって再利用される)
 */
data class TradeEvent(
    var timestamp: Long = 0,
    var symbolId: Int = 0,          // インターン化されたシンボルID
    var payloadSize: Short = 0,
    var inlinePayload: ByteArray = ByteArray(256)  // 小ペイロードをインライン格納
) {
    fun clear() {
        timestamp = 0
        symbolId = 0
        payloadSize = 0
        // ペイロード配列のゼロクリアはしない (コストが高い)
    }
}

/**
 * イベントハンドラー (専用スレッドで実行)
 */
private class TradeEventHandler(
    private val metrics: FastPathMetrics?,
    private val persistenceQueue: PersistenceQueueEngine?,
    private val symbolTable: SymbolTable,
    private val auditLogger: AuditLogger?
) : EventHandler<TradeEvent> {

    private val processedCount = AtomicLong(0)
    private val executionCount = AtomicLong(0)

    // シンボルごとのオーダーブック (GC負荷を生成するため意図的に非最適化)
    private val orderBooks = Object2ObjectHashMap<Int, OrderBook>()

    override fun onEvent(event: TradeEvent, sequence: Long, endOfBatch: Boolean) {
        val startNs = System.nanoTime()

        // Fast Path処理 (ロックフリー)
        val execution = processEvent(event)

        val latencyNs = System.nanoTime() - startNs
        metrics?.recordProcess(latencyNs)

        // 監査ログ: イベント処理完了
        val key = symbolTable.resolve(event.symbolId) ?: "unknown"
        val payload = event.inlinePayload.copyOf(event.payloadSize.toInt())
        auditLogger?.logEventProcessed(key, payload, event.timestamp, latencyNs)

        // 約定が発生した場合
        if (execution != null) {
            executionCount.incrementAndGet()
        }

        // 遅いイベントを警告
        if (latencyNs > 100_000) {  // > 100μs
            println("WARN: Slow event ${event.symbolId}: ${latencyNs}ns")
        }

        processedCount.incrementAndGet()
    }

    private fun processEvent(event: TradeEvent): Execution? {
        // ペイロードからオーダー情報をパース (簡易実装)
        val order = parseOrder(event)

        // シンボルごとのオーダーブックを取得
        val orderBook = orderBooks.computeIfAbsent(event.symbolId) {
            OrderBook(event.symbolId)
        }

        // オーダーマッチング
        val execution = when (order.side) {
            OrderSide.BUY -> orderBook.processBuyOrder(order.price, order.quantity, event.timestamp)
            OrderSide.SELL -> orderBook.processSellOrder(order.price, order.quantity, event.timestamp)
        }

        // Persistence Queueへ公開 (非同期、Chronicle Queue書き込み)
        // Fast Path処理から完全分離され、レイテンシに影響しない
        persistenceQueue?.let { queue ->
            val key = symbolTable.resolve(event.symbolId) ?: "unknown"
            val payload = event.inlinePayload.copyOf(event.payloadSize.toInt())
            queue.tryPublish(key, payload, event.timestamp)
        }

        return execution
    }

    private fun parseOrder(event: TradeEvent): SimpleOrder {
        // ペイロードから簡易パース (8バイト想定: side(1) + price(3) + quantity(4))
        // 本番環境ではProtobuf/FlatBuffersなどを使用
        val payload = event.inlinePayload
        val side = if (payload[0].toInt() == 0) OrderSide.BUY else OrderSide.SELL
        val price = ((payload[1].toInt() and 0xFF) shl 16) or
                    ((payload[2].toInt() and 0xFF) shl 8) or
                    (payload[3].toInt() and 0xFF)
        val quantity = ((payload[4].toInt() and 0xFF) shl 24) or
                       ((payload[5].toInt() and 0xFF) shl 16) or
                       ((payload[6].toInt() and 0xFF) shl 8) or
                       (payload[7].toInt() and 0xFF)

        return SimpleOrder(side, price, quantity)
    }

    fun getExecutionCount(): Long = executionCount.get()
}

enum class OrderSide {
    BUY, SELL
}

data class SimpleOrder(
    val side: OrderSide,
    val price: Int,
    val quantity: Int
)

/**
 * シンボルテーブル: 文字列のインターン化 (スレッドセーフ)
 */
private class SymbolTable {
    private val map = Object2ObjectHashMap<String, Int>()
    private val reverseMap = mutableListOf<String>()
    private var nextId = 0

    @Synchronized
    fun intern(symbol: String): Int {
        return map.getOrPut(symbol) {
            val id = nextId++
            reverseMap.add(symbol)
            id
        }
    }

    @Synchronized
    fun resolve(id: Int): String? = reverseMap.getOrNull(id)
}

/**
 * メトリクス収集 (ロックフリー + HDR Histogram)
 */
class FastPathMetrics {
    private val publishLatencyNs = AtomicLong(0)
    private val processLatencyNs = AtomicLong(0)
    private val dropCount = AtomicLong(0)
    private val eventCount = AtomicLong(0)

    // HDR Histogram: 1ns ~ 1秒 (1,000,000,000ns), 3桁精度
    private val publishHistogram = Histogram(1_000_000_000L, 3)
    private val processHistogram = Histogram(1_000_000_000L, 3)

    fun recordPublish(latencyNs: Long) {
        publishLatencyNs.addAndGet(latencyNs)
        eventCount.incrementAndGet()
        publishHistogram.recordValue(latencyNs)
    }

    fun recordProcess(latencyNs: Long) {
        processLatencyNs.addAndGet(latencyNs)
        processHistogram.recordValue(latencyNs)
    }

    fun recordDrop() {
        dropCount.incrementAndGet()
    }

    fun snapshot(): MetricsSnapshot {
        val count = eventCount.get()
        return MetricsSnapshot(
            avgPublishLatencyNs = if (count > 0) publishLatencyNs.get() / count else 0,
            avgProcessLatencyNs = if (count > 0) processLatencyNs.get() / count else 0,
            dropCount = dropCount.get(),
            eventCount = count,
            publishP50Ns = publishHistogram.getValueAtPercentile(50.0),
            publishP99Ns = publishHistogram.getValueAtPercentile(99.0),
            publishP999Ns = publishHistogram.getValueAtPercentile(99.9),
            processP50Ns = processHistogram.getValueAtPercentile(50.0),
            processP99Ns = processHistogram.getValueAtPercentile(99.0),
            processP999Ns = processHistogram.getValueAtPercentile(99.9)
        )
    }
}

data class MetricsSnapshot(
    val avgPublishLatencyNs: Long,
    val avgProcessLatencyNs: Long,
    val dropCount: Long,
    val eventCount: Long,
    val publishP50Ns: Long,
    val publishP99Ns: Long,
    val publishP999Ns: Long,
    val processP50Ns: Long,
    val processP99Ns: Long,
    val processP999Ns: Long
) {
    fun avgPublishLatencyUs() = avgPublishLatencyNs / 1000.0
    fun avgProcessLatencyUs() = avgProcessLatencyNs / 1000.0
    fun publishP50Us() = publishP50Ns / 1000.0
    fun publishP99Us() = publishP99Ns / 1000.0
    fun publishP999Us() = publishP999Ns / 1000.0
    fun processP50Us() = processP50Ns / 1000.0
    fun processP99Us() = processP99Ns / 1000.0
    fun processP999Us() = processP999Ns / 1000.0
}
