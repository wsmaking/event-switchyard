package app.fast

import app.audit.AuditLogger
import app.kafka.ChronicleQueueWriter
import com.lmax.disruptor.*
import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.dsl.ProducerType
import org.HdrHistogram.Histogram
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicLong

/**
 * Chronicle Queue書き込み専用のDisruptor Ring Buffer
 *
 * Fast Path処理とChronicle Queue書き込みを分離し、
 * ディスクI/OがFast Pathレイテンシに影響しないようにする。
 *
 * 設計方針:
 * - Fast Pathとは別スレッドで実行
 * - BlockingWaitStrategy (レイテンシよりスループット重視)
 * - 優先度をFast Pathより低く設定
 */
class PersistenceQueueEngine(
    private val chronicleWriter: ChronicleQueueWriter,
    private val auditLogger: AuditLogger? = null,  // 監査ログ
    bufferSize: Int = 32768  // Fast Pathより小さめ (2の累乗)
) : AutoCloseable {

    private val disruptor: Disruptor<PersistenceEvent>
    private val ringBuffer: RingBuffer<PersistenceEvent>

    // メトリクス
    private val publishCount = AtomicLong(0)
    private val dropCount = AtomicLong(0)
    private val processCount = AtomicLong(0)
    private val errorCount = AtomicLong(0)  // Chronicle Queue書き込みエラー数
    private val writeLatencyHistogram = Histogram(1_000_000_000L, 3)  // Chronicle Queue書き込みレイテンシ

    init {
        val threadFactory = ThreadFactory { r ->
            Thread(r, "persistence-queue").apply {
                isDaemon = false
                priority = Thread.NORM_PRIORITY  // Fast Pathより低優先度
            }
        }

        disruptor = Disruptor(
            ::PersistenceEvent,
            bufferSize,
            threadFactory,
            ProducerType.SINGLE,
            BlockingWaitStrategy()  // レイテンシよりスループット重視
        )

        disruptor.handleEventsWith(PersistenceEventHandler(chronicleWriter, auditLogger, processCount, errorCount, writeLatencyHistogram))
        disruptor.start()

        ringBuffer = disruptor.ringBuffer
    }

    /**
     * Persistence Queueへイベントを公開 (ノンブロッキング)
     *
     * @return 公開成功でtrue、バッファ満杯でfalse
     */
    fun tryPublish(key: String, payload: ByteArray, timestamp: Long): Boolean {
        val available = ringBuffer.tryPublishEvent { event, _ ->
            event.timestamp = timestamp
            event.key = key
            event.payloadSize = payload.size.toShort()
            System.arraycopy(payload, 0, event.payload, 0, minOf(payload.size, 256))
        }

        if (available) {
            publishCount.incrementAndGet()
        } else {
            dropCount.incrementAndGet()
        }

        return available
    }

    /**
     * 統計情報を取得
     */
    fun getStats(): PersistenceQueueStats {
        return PersistenceQueueStats(
            publishCount = publishCount.get(),
            dropCount = dropCount.get(),
            processCount = processCount.get(),
            errorCount = errorCount.get(),
            lag = publishCount.get() - processCount.get(),
            writeP50Ns = writeLatencyHistogram.getValueAtPercentile(50.0),
            writeP99Ns = writeLatencyHistogram.getValueAtPercentile(99.0),
            writeP999Ns = writeLatencyHistogram.getValueAtPercentile(99.9)
        )
    }

    override fun close() {
        disruptor.shutdown()
    }
}

/**
 * 永続化用イベント (Disruptorによって再利用)
 */
data class PersistenceEvent(
    var timestamp: Long = 0,
    var key: String = "",
    var payloadSize: Short = 0,
    var payload: ByteArray = ByteArray(256)
) {
    fun clear() {
        timestamp = 0
        key = ""
        payloadSize = 0
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as PersistenceEvent

        if (timestamp != other.timestamp) return false
        if (key != other.key) return false
        if (payloadSize != other.payloadSize) return false
        if (!payload.contentEquals(other.payload)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = timestamp.hashCode()
        result = 31 * result + key.hashCode()
        result = 31 * result + payloadSize
        result = 31 * result + payload.contentHashCode()
        return result
    }
}

/**
 * Chronicle Queue書き込みハンドラー
 */
private class PersistenceEventHandler(
    private val chronicleWriter: ChronicleQueueWriter,
    private val auditLogger: AuditLogger?,
    private val processCount: AtomicLong,
    private val errorCount: AtomicLong,
    private val writeLatencyHistogram: Histogram
) : EventHandler<PersistenceEvent> {

    override fun onEvent(event: PersistenceEvent, sequence: Long, endOfBatch: Boolean) {
        val startNs = System.nanoTime()

        try {
            // Chronicle Queueへ書き込み
            val payload = event.payload.copyOf(event.payloadSize.toInt())
            chronicleWriter.write(
                event.key,
                payload,
                event.timestamp
            )

            val latencyNs = System.nanoTime() - startNs
            writeLatencyHistogram.recordValue(latencyNs)
            processCount.incrementAndGet()

            // 監査ログ: Chronicle Queue書き込み成功
            auditLogger?.logChronicleWrite(event.key, payload, startNs, success = true)
        } catch (e: Exception) {
            errorCount.incrementAndGet()
            System.err.println("ERROR: Chronicle Queue write failed for key=${event.key}, error=${e.message}")
            e.printStackTrace()
            // エラー時もレイテンシは記録 (トラブルシューティング用)
            val latencyNs = System.nanoTime() - startNs
            writeLatencyHistogram.recordValue(latencyNs)

            // 監査ログ: Chronicle Queue書き込み失敗
            auditLogger?.logChronicleWrite(event.key, event.payload.copyOf(event.payloadSize.toInt()), startNs, success = false)
        }
    }
}

/**
 * Persistence Queue統計情報
 */
data class PersistenceQueueStats(
    val publishCount: Long,
    val dropCount: Long,
    val processCount: Long,
    val errorCount: Long,  // Chronicle Queue書き込みエラー数
    val lag: Long,  // 公開数 - 処理数
    val writeP50Ns: Long,
    val writeP99Ns: Long,
    val writeP999Ns: Long
) {
    fun toMap(): Map<String, Any> = mapOf(
        "persistence_queue_publish_count" to publishCount,
        "persistence_queue_drop_count" to dropCount,
        "persistence_queue_process_count" to processCount,
        "persistence_queue_error_count" to errorCount,
        "persistence_queue_lag" to lag,
        "persistence_queue_write_p50_us" to (writeP50Ns / 1000.0),
        "persistence_queue_write_p99_us" to (writeP99Ns / 1000.0),
        "persistence_queue_write_p999_us" to (writeP999Ns / 1000.0)
    )
}
