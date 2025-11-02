package app.kafka

import net.openhft.chronicle.queue.ChronicleQueue
import net.openhft.chronicle.queue.ExcerptAppender
import net.openhft.chronicle.queue.RollCycles
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong

/**
 * Chronicle Queueへイベントを書き込むライター
 *
 * Fast Path処理後のイベントを永続化ジャーナルに書き込み、
 * Kafka Bridgeが非同期に読み込んでKafkaへ送信する。
 *
 * 環境変数:
 * - CHRONICLE_QUEUE_PATH: キューファイルのパス (デフォルト: ./data/chronicle-queue)
 * - CHRONICLE_QUEUE_ROLL_CYCLE: ロールサイクル (デフォルト: HOURLY)
 */
class ChronicleQueueWriter(
    queuePath: String = System.getenv("CHRONICLE_QUEUE_PATH") ?: "./data/chronicle-queue",
    rollCycle: RollCycles = RollCycles.HOURLY
) : AutoCloseable {

    private val queue: ChronicleQueue
    private val appender: ExcerptAppender

    // メトリクス
    private val writeCount = AtomicLong(0)
    private val writeErrorCount = AtomicLong(0)
    private val totalWriteTimeNs = AtomicLong(0)

    init {
        val path: Path = Paths.get(queuePath)

        queue = SingleChronicleQueueBuilder.builder()
            .path(path)
            .rollCycle(rollCycle)
            .build()

        appender = queue.acquireAppender()

        println("Chronicle Queue initialized: path=$queuePath, rollCycle=$rollCycle")
    }

    /**
     * イベントをChronicle Queueへ書き込み
     *
     * @param key イベントキー（シンボル名など）
     * @param payload イベントペイロード
     * @param timestamp イベントタイムスタンプ (ns)
     * @return 書き込み成功でtrue、失敗でfalse
     */
    fun write(key: String, payload: ByteArray, timestamp: Long = System.nanoTime()): Boolean {
        val startNs = System.nanoTime()

        return try {
            appender.writeDocument { wire ->
                wire.write("timestamp").int64(timestamp)
                wire.write("key").text(key)
                wire.write("payloadSize").int32(payload.size)
                wire.write("payload").bytes(payload)
            }

            val writeTimeNs = System.nanoTime() - startNs
            totalWriteTimeNs.addAndGet(writeTimeNs)
            writeCount.incrementAndGet()

            true
        } catch (e: Exception) {
            writeErrorCount.incrementAndGet()
            println("ERROR: Chronicle Queue write failed: ${e.message}")
            false
        }
    }

    /**
     * バッチ書き込み（複数イベントを効率的に書き込み）
     */
    fun writeBatch(events: List<QueueEvent>): Int {
        var successCount = 0

        for (event in events) {
            if (write(event.key, event.payload, event.timestamp)) {
                successCount++
            }
        }

        return successCount
    }

    /**
     * 統計情報を取得
     */
    fun getStats(): ChronicleQueueStats {
        val count = writeCount.get()
        val avgWriteTimeNs = if (count > 0) totalWriteTimeNs.get() / count else 0

        return ChronicleQueueStats(
            writeCount = count,
            writeErrorCount = writeErrorCount.get(),
            avgWriteTimeUs = avgWriteTimeNs / 1000.0,
            queueSize = estimateQueueSize()
        )
    }

    /**
     * キュー内の未処理イベント数を推定
     * (正確な値ではなく、おおよその目安)
     */
    private fun estimateQueueSize(): Long {
        return try {
            val lastIndex = queue.createTailer().toEnd().index()
            val firstIndex = queue.firstIndex()
            maxOf(0, lastIndex - firstIndex)
        } catch (e: Exception) {
            -1  // 推定不可
        }
    }

    override fun close() {
        try {
            appender.close()
            queue.close()
            println("Chronicle Queue closed. Total writes: ${writeCount.get()}")
        } catch (e: Exception) {
            println("ERROR: Chronicle Queue close failed: ${e.message}")
        }
    }
}

/**
 * Chronicle Queueに書き込むイベント
 */
data class QueueEvent(
    val key: String,
    val payload: ByteArray,
    val timestamp: Long = System.nanoTime()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as QueueEvent

        if (key != other.key) return false
        if (!payload.contentEquals(other.payload)) return false
        if (timestamp != other.timestamp) return false

        return true
    }

    override fun hashCode(): Int {
        var result = key.hashCode()
        result = 31 * result + payload.contentHashCode()
        result = 31 * result + timestamp.hashCode()
        return result
    }
}

/**
 * Chronicle Queue統計情報
 */
data class ChronicleQueueStats(
    val writeCount: Long,
    val writeErrorCount: Long,
    val avgWriteTimeUs: Double,
    val queueSize: Long  // 未処理イベント数（推定値）
) {
    fun toMap(): Map<String, Any> = mapOf(
        "chronicle_write_count" to writeCount,
        "chronicle_write_error_count" to writeErrorCount,
        "chronicle_avg_write_us" to avgWriteTimeUs,
        "chronicle_queue_size" to queueSize
    )
}
