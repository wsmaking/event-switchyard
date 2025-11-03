package app.kafka

import app.audit.AuditLogger
import net.openhft.chronicle.queue.ChronicleQueue
import net.openhft.chronicle.queue.ExcerptTailer
import net.openhft.chronicle.queue.RollCycles
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.nio.file.Paths
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

/**
 * Chronicle QueueからKafkaへイベントを転送するBridge
 *
 * 専用スレッドでChronicle Queueを読み込み、バッチでKafkaへ送信する。
 * Fast Path処理とは完全に非同期で動作し、Kafka障害時もFast Pathに影響しない。
 *
 * Exactly-Once Semantics対応:
 * - Kafka Transactionsを使用してバッチ送信を原子的に実行
 * - idempotent producerで重複送信を防止
 * - transactional.idでプロデューサーを一意に識別
 *
 * 環境変数:
 * - KAFKA_BRIDGE_ENABLE: "1"で有効化 (デフォルト: "0")
 * - KAFKA_BRIDGE_BATCH_SIZE: バッチサイズ (デフォルト: 100)
 * - KAFKA_BRIDGE_BATCH_TIMEOUT_MS: バッチタイムアウト (デフォルト: 50ms)
 * - KAFKA_BRIDGE_TOPIC: Kafkaトピック名 (デフォルト: "fast-path-events")
 * - KAFKA_BOOTSTRAP_SERVERS: Kafkaブローカー (デフォルト: "localhost:9092")
 * - KAFKA_TRANSACTIONS_ENABLE: "1"でKafka Transactionsを有効化 (デフォルト: "1")
 * - KAFKA_TRANSACTIONAL_ID: トランザクションID (デフォルト: "hft-bridge-tx-{hostname}")
 */
class KafkaBridge(
    private val auditLogger: AuditLogger? = null,  // 監査ログ
    queuePath: String = System.getenv("CHRONICLE_QUEUE_PATH") ?: "./data/chronicle-queue",
    rollCycle: RollCycles = RollCycles.HOURLY,
    private val batchSize: Int = System.getenv("KAFKA_BRIDGE_BATCH_SIZE")?.toIntOrNull() ?: 100,
    private val batchTimeoutMs: Long = System.getenv("KAFKA_BRIDGE_BATCH_TIMEOUT_MS")?.toLongOrNull() ?: 50,
    private val topic: String = System.getenv("KAFKA_BRIDGE_TOPIC") ?: "fast-path-events",
    bootstrapServers: String = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092",
    private val transactionsEnabled: Boolean = System.getenv("KAFKA_TRANSACTIONS_ENABLE") != "0",
    private val transactionalId: String = System.getenv("KAFKA_TRANSACTIONAL_ID")
        ?: "hft-bridge-tx-${java.net.InetAddress.getLocalHost().hostName}"
) : AutoCloseable {

    private val queue: ChronicleQueue
    private val tailer: ExcerptTailer
    private val kafkaProducer: KafkaProducer<String, ByteArray>
    private val running = AtomicBoolean(false)
    private var bridgeThread: Thread? = null

    // メトリクス
    private val readCount = AtomicLong(0)
    private val sendCount = AtomicLong(0)
    private val sendErrorCount = AtomicLong(0)
    private val totalSendTimeMs = AtomicLong(0)

    init {
        // Chronicle Queue初期化
        queue = SingleChronicleQueueBuilder.builder()
            .path(Paths.get(queuePath))
            .rollCycle(rollCycle)
            .build()

        tailer = queue.createTailer()

        // Kafka Producer初期化
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java.name)
            put(ProducerConfig.LINGER_MS_CONFIG, "5")  // 5msバッファリング
            put(ProducerConfig.BATCH_SIZE_CONFIG, "65536")  // 64KB
            put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")  // LZ4圧縮
            put(ProducerConfig.RETRIES_CONFIG, "3")
            put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100")

            if (transactionsEnabled) {
                // Exactly-Once Semantics設定
                put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")  // べき等性有効化
                put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)  // トランザクションID
                put(ProducerConfig.ACKS_CONFIG, "all")  // 全レプリカACK必須
                put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")  // べき等性: 最大5
            } else {
                put(ProducerConfig.ACKS_CONFIG, "1")  // リーダーACKのみ（パフォーマンス重視）
                put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
            }
        }

        kafkaProducer = KafkaProducer(props)

        // トランザクション初期化
        if (transactionsEnabled) {
            kafkaProducer.initTransactions()
            println("Kafka Bridge initialized: topic=$topic, batchSize=$batchSize, batchTimeoutMs=${batchTimeoutMs}ms, transactions=ENABLED, transactionalId=$transactionalId")
        } else {
            println("Kafka Bridge initialized: topic=$topic, batchSize=$batchSize, batchTimeoutMs=${batchTimeoutMs}ms, transactions=DISABLED")
        }
    }

    /**
     * Bridgeスレッドを開始
     */
    fun start() {
        if (running.compareAndSet(false, true)) {
            bridgeThread = thread(name = "kafka-bridge", isDaemon = false) {
                runBridgeLoop()
            }
            println("Kafka Bridge started")
        }
    }

    /**
     * Bridgeメインループ
     */
    private fun runBridgeLoop() {
        val batch = mutableListOf<QueueEvent>()
        var lastSendTime = System.currentTimeMillis()

        while (running.get()) {
            try {
                // Chronicle Queueから読み込み
                val event = readNextEvent()

                if (event != null) {
                    batch.add(event)
                    readCount.incrementAndGet()
                }

                val now = System.currentTimeMillis()
                val shouldSend = batch.size >= batchSize ||
                        (batch.isNotEmpty() && (now - lastSendTime) >= batchTimeoutMs)

                if (shouldSend) {
                    sendBatchToKafka(batch)
                    batch.clear()
                    lastSendTime = now
                }

                // イベントがない場合は短時間スリープ
                if (event == null) {
                    Thread.sleep(1)
                }

            } catch (e: InterruptedException) {
                println("Kafka Bridge interrupted")
                break
            } catch (e: Exception) {
                println("ERROR: Kafka Bridge loop error: ${e.message}")
                e.printStackTrace()
                Thread.sleep(100)  // エラー時は少し待つ
            }
        }

        // 残りのバッチを送信
        if (batch.isNotEmpty()) {
            sendBatchToKafka(batch)
        }

        println("Kafka Bridge stopped")
    }

    /**
     * Chronicle Queueから次のイベントを読み込み
     */
    private fun readNextEvent(): QueueEvent? {
        return try {
            var key: String? = null
            var payload: ByteArray? = null
            var timestamp: Long = 0

            val hasNext = tailer.readDocument { wire ->
                timestamp = wire.read("timestamp").int64()
                key = wire.read("key").text()
                val payloadSize = wire.read("payloadSize").int32()
                payload = wire.read("payload").bytes()
            }

            if (hasNext && key != null && payload != null) {
                QueueEvent(key!!, payload!!, timestamp)
            } else {
                null
            }
        } catch (e: Exception) {
            println("ERROR: Chronicle Queue read failed: ${e.message}")
            null
        }
    }

    /**
     * バッチでKafkaへ送信 (Exactly-Once Semantics対応)
     */
    private fun sendBatchToKafka(batch: List<QueueEvent>) {
        if (batch.isEmpty()) return

        val startMs = System.currentTimeMillis()

        try {
            if (transactionsEnabled) {
                // トランザクション開始
                kafkaProducer.beginTransaction()
            }

            for (event in batch) {
                val record = ProducerRecord(topic, event.key, event.payload)
                kafkaProducer.send(record) { metadata, exception ->
                    if (exception != null) {
                        sendErrorCount.incrementAndGet()
                        println("ERROR: Kafka send failed for key=${event.key}: ${exception.message}")
                    } else {
                        sendCount.incrementAndGet()
                        // 監査ログ: Kafka送信成功
                        auditLogger?.logKafkaSend(event.key, metadata.partition(), metadata.offset(), System.nanoTime())
                    }
                }
            }

            // flushして送信完了を待つ
            kafkaProducer.flush()

            if (transactionsEnabled) {
                // トランザクションコミット (原子的にバッチ全体を送信)
                kafkaProducer.commitTransaction()
            }

            val sendTimeMs = System.currentTimeMillis() - startMs
            totalSendTimeMs.addAndGet(sendTimeMs)

            if (sendTimeMs > 100) {
                println("WARN: Slow Kafka send: ${sendTimeMs}ms for ${batch.size} events")
            }

        } catch (e: Exception) {
            sendErrorCount.addAndGet(batch.size.toLong())
            println("ERROR: Kafka batch send failed: ${e.message}")
            e.printStackTrace()

            // トランザクションアボート (バッチ全体をロールバック)
            if (transactionsEnabled) {
                try {
                    kafkaProducer.abortTransaction()
                    println("Transaction aborted for batch of ${batch.size} events")
                } catch (abortEx: Exception) {
                    println("ERROR: Failed to abort transaction: ${abortEx.message}")
                }
            }
        }
    }

    /**
     * 統計情報を取得
     */
    fun getStats(): KafkaBridgeStats {
        val sendCnt = sendCount.get()
        val avgSendTimeMs = if (sendCnt > 0) totalSendTimeMs.get().toDouble() / sendCnt else 0.0

        return KafkaBridgeStats(
            readCount = readCount.get(),
            sendCount = sendCnt,
            sendErrorCount = sendErrorCount.get(),
            avgSendTimeMs = avgSendTimeMs,
            lag = readCount.get() - sendCnt  // 未送信イベント数
        )
    }

    /**
     * Bridgeを停止
     */
    fun stop() {
        if (running.compareAndSet(true, false)) {
            bridgeThread?.interrupt()
            bridgeThread?.join(5000)  // 最大5秒待つ
        }
    }

    override fun close() {
        stop()

        try {
            kafkaProducer.close()
            tailer.close()
            queue.close()
            println("Kafka Bridge closed. Total sent: ${sendCount.get()}, errors: ${sendErrorCount.get()}")
        } catch (e: Exception) {
            println("ERROR: Kafka Bridge close failed: ${e.message}")
        }
    }
}

/**
 * Kafka Bridge統計情報
 */
data class KafkaBridgeStats(
    val readCount: Long,
    val sendCount: Long,
    val sendErrorCount: Long,
    val avgSendTimeMs: Double,
    val lag: Long  // Chronicle Queue読み込み - Kafka送信
) {
    fun toMap(): Map<String, Any> = mapOf(
        "kafka_bridge_read_count" to readCount,
        "kafka_bridge_send_count" to sendCount,
        "kafka_bridge_send_error_count" to sendErrorCount,
        "kafka_bridge_avg_send_ms" to avgSendTimeMs,
        "kafka_bridge_lag" to lag
    )
}
