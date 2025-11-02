package app.fast

import com.lmax.disruptor.*
import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.dsl.ProducerType
import org.agrona.collections.Object2ObjectHashMap
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
 * - 固定小数点演算で価格を扱う (Doubleのboxingを回避)
 */
class FastPathEngine(
    bufferSize: Int = 65536,  // 2の累乗である必要あり
    enableMetrics: Boolean = System.getenv("FAST_PATH_METRICS") == "1"
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
        disruptor.handleEventsWith(TradeEventHandler(metrics))
        disruptor.start()

        ringBuffer = disruptor.ringBuffer
    }

    /**
     * Fast Pathへイベントを公開 (ノンブロッキング)
     * @return 公開成功でtrue、バッファ満杯でfalse
     */
    fun tryPublish(key: String, payload: ByteArray): Boolean {
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
    private val metrics: FastPathMetrics?
) : EventHandler<TradeEvent> {

    private val processedCount = AtomicLong(0)

    override fun onEvent(event: TradeEvent, sequence: Long, endOfBatch: Boolean) {
        val startNs = System.nanoTime()

        // Fast Path処理 (ロックフリー)
        processEvent(event)

        val latencyNs = System.nanoTime() - startNs
        metrics?.recordProcess(latencyNs)

        // 遅いイベントを警告
        if (latencyNs > 100_000) {  // > 100μs
            println("WARN: Slow event ${event.symbolId}: ${latencyNs}ns")
        }

        processedCount.incrementAndGet()
    }

    private fun processEvent(event: TradeEvent) {
        // TODO: ビジネスロジックを実装
        // - オーダーマッチング
        // - リスクチェック
        // - 約定処理
    }
}

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
 * メトリクス収集 (ロックフリー)
 */
class FastPathMetrics {
    private val publishLatencyNs = AtomicLong(0)
    private val processLatencyNs = AtomicLong(0)
    private val dropCount = AtomicLong(0)
    private val eventCount = AtomicLong(0)

    fun recordPublish(latencyNs: Long) {
        publishLatencyNs.addAndGet(latencyNs)
        eventCount.incrementAndGet()
    }

    fun recordProcess(latencyNs: Long) {
        processLatencyNs.addAndGet(latencyNs)
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
            eventCount = count
        )
    }
}

data class MetricsSnapshot(
    val avgPublishLatencyNs: Long,
    val avgProcessLatencyNs: Long,
    val dropCount: Long,
    val eventCount: Long
) {
    fun avgPublishLatencyUs() = avgPublishLatencyNs / 1000.0
    fun avgProcessLatencyUs() = avgProcessLatencyNs / 1000.0
}
