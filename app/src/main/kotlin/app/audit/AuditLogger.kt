package app.audit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import java.io.BufferedWriter
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

/**
 * 監査ログ (Tick-by-Tick記録)
 *
 * 証券会社のコンプライアンス要件に準拠:
 * - すべてのイベント (受信・処理・約定) をタイムスタンプ付きで記録
 * - 改ざん防止: Append-only、シーケンス番号付き
 * - ローテーション: 日次でファイル分割
 * - パフォーマンス: 非同期書き込み (キューバッファ)
 */
class AuditLogger(
    private val logDir: String = "var/logs/audit",
    private val bufferSize: Int = 10000
) : AutoCloseable {

    private val mapper = ObjectMapper()
    private val queue = ArrayBlockingQueue<AuditEvent>(bufferSize)
    private val running = AtomicBoolean(true)
    private val sequenceNumber = AtomicLong(0)
    private val writerThread: Thread
    private var currentWriter: BufferedWriter? = null
    private var currentDate: String = ""

    companion object {
        private val DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC)
        private val TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_INSTANT
    }

    init {
        // ログディレクトリ作成
        Files.createDirectories(Paths.get(logDir))

        // 非同期書き込みスレッド起動
        writerThread = thread(name = "audit-logger", isDaemon = false) {
            while (running.get() || queue.isNotEmpty()) {
                try {
                    val event = queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS)
                    if (event != null) {
                        writeEvent(event)
                    }
                } catch (e: InterruptedException) {
                    break
                } catch (e: Exception) {
                    System.err.println("監査ログ書き込みエラー: ${e.message}")
                    e.printStackTrace()
                }
            }
            currentWriter?.close()
        }
    }

    /**
     * イベント受信ログ
     */
    fun logEventReceived(key: String, payload: ByteArray, timestampNs: Long) {
        enqueue(AuditEvent(
            eventType = "EVENT_RECEIVED",
            key = key,
            payloadSize = payload.size,
            timestampNs = timestampNs,
            metadata = mapOf(
                "source" to "http_ingress"
            )
        ))
    }

    /**
     * イベント処理ログ
     */
    fun logEventProcessed(key: String, payload: ByteArray, timestampNs: Long, latencyNs: Long) {
        enqueue(AuditEvent(
            eventType = "EVENT_PROCESSED",
            key = key,
            payloadSize = payload.size,
            timestampNs = timestampNs,
            metadata = mapOf(
                "latency_ns" to latencyNs,
                "latency_us" to (latencyNs / 1000.0)
            )
        ))
    }

    /**
     * イベントドロップログ (SLO違反)
     */
    fun logEventDropped(key: String, reason: String, timestampNs: Long) {
        enqueue(AuditEvent(
            eventType = "EVENT_DROPPED",
            key = key,
            payloadSize = 0,
            timestampNs = timestampNs,
            metadata = mapOf(
                "reason" to reason,
                "severity" to "CRITICAL"
            )
        ))
    }

    /**
     * Chronicle Queue書き込みログ
     */
    fun logChronicleWrite(key: String, payload: ByteArray, timestampNs: Long, success: Boolean) {
        enqueue(AuditEvent(
            eventType = "CHRONICLE_WRITE",
            key = key,
            payloadSize = payload.size,
            timestampNs = timestampNs,
            metadata = mapOf(
                "success" to success,
                "destination" to "chronicle_queue"
            )
        ))
    }

    /**
     * Kafka送信ログ
     */
    fun logKafkaSend(key: String, partition: Int, offset: Long, timestampNs: Long) {
        enqueue(AuditEvent(
            eventType = "KAFKA_SEND",
            key = key,
            payloadSize = 0,
            timestampNs = timestampNs,
            metadata = mapOf(
                "partition" to partition,
                "offset" to offset,
                "destination" to "kafka"
            )
        ))
    }

    /**
     * 内部: イベントをキューに追加
     */
    private fun enqueue(event: AuditEvent) {
        event.sequenceNumber = sequenceNumber.incrementAndGet()
        if (!queue.offer(event)) {
            // キューフルの場合は同期書き込み (ドロップしない)
            System.err.println("⚠️ 監査ログキューフル、同期書き込み実行")
            writeEvent(event)
        }
    }

    /**
     * 内部: イベントをファイルに書き込み
     */
    private fun writeEvent(event: AuditEvent) {
        // 日付が変わったらファイルローテーション
        val date = DATE_FORMATTER.format(Instant.ofEpochSecond(0, event.timestampNs))
        if (date != currentDate) {
            currentWriter?.close()
            currentDate = date
            val logFile = Paths.get(logDir, "audit-$date.jsonl").toFile()
            currentWriter = BufferedWriter(FileWriter(logFile, true))
        }

        // JSON行フォーマットで書き込み
        val json = mapper.createObjectNode().apply {
            put("seq", event.sequenceNumber)
            put("event_type", event.eventType)
            put("key", event.key)
            put("payload_size", event.payloadSize)
            put("timestamp_ns", event.timestampNs)
            put("timestamp_iso", TIMESTAMP_FORMATTER.format(Instant.ofEpochSecond(0, event.timestampNs)))

            // メタデータ
            val metaNode = putObject("metadata")
            event.metadata.forEach { (k, v) ->
                when (v) {
                    is String -> metaNode.put(k, v)
                    is Int -> metaNode.put(k, v)
                    is Long -> metaNode.put(k, v)
                    is Double -> metaNode.put(k, v)
                    is Boolean -> metaNode.put(k, v)
                    else -> metaNode.put(k, v.toString())
                }
            }
        }

        currentWriter?.apply {
            write(mapper.writeValueAsString(json))
            newLine()
            flush()
        }
    }

    override fun close() {
        running.set(false)
        writerThread.join(5000) // 最大5秒待機
        currentWriter?.close()
    }
}

/**
 * 監査イベント
 */
data class AuditEvent(
    val eventType: String,
    val key: String,
    val payloadSize: Int,
    val timestampNs: Long,
    val metadata: Map<String, Any> = emptyMap(),
    var sequenceNumber: Long = 0
)
