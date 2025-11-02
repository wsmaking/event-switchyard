package app.fast

import app.kafka.ChronicleQueueWriter
import com.lmax.disruptor.*
import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.dsl.ProducerType
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
    bufferSize: Int = 32768  // Fast Pathより小さめ (2の累乗)
) : AutoCloseable {

    private val disruptor: Disruptor<PersistenceEvent>
    private val ringBuffer: RingBuffer<PersistenceEvent>

    // メトリクス
    private val publishCount = AtomicLong(0)
    private val dropCount = AtomicLong(0)
    private val processCount = AtomicLong(0)

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

        disruptor.handleEventsWith(PersistenceEventHandler(chronicleWriter, processCount))
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
            lag = publishCount.get() - processCount.get()
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
    private val processCount: AtomicLong
) : EventHandler<PersistenceEvent> {

    override fun onEvent(event: PersistenceEvent, sequence: Long, endOfBatch: Boolean) {
        // Chronicle Queueへ書き込み
        chronicleWriter.write(
            event.key,
            event.payload.copyOf(event.payloadSize.toInt()),
            event.timestamp
        )

        processCount.incrementAndGet()
    }
}

/**
 * Persistence Queue統計情報
 */
data class PersistenceQueueStats(
    val publishCount: Long,
    val dropCount: Long,
    val processCount: Long,
    val lag: Long  // 公開数 - 処理数
) {
    fun toMap(): Map<String, Any> = mapOf(
        "persistence_queue_publish_count" to publishCount,
        "persistence_queue_drop_count" to dropCount,
        "persistence_queue_process_count" to processCount,
        "persistence_queue_lag" to lag
    )
}
