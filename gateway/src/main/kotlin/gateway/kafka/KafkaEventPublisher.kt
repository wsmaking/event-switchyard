package gateway.kafka

import gateway.bus.BusEvent
import gateway.bus.EventPublisher
import gateway.json.Json
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

class KafkaEventPublisher(
    bootstrapServers: String = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092",
    private val topic: String = System.getenv("KAFKA_TOPIC") ?: "events",
    private val enabled: Boolean =
        (System.getenv("KAFKA_ENABLE") ?: "0").let { it == "1" || it.equals("true", ignoreCase = true) },
    queueCapacity: Int = (System.getenv("KAFKA_QUEUE_CAPACITY") ?: "10000").toInt()
) : EventPublisher {
    private val queue = ArrayBlockingQueue<BusEvent>(queueCapacity)
    private val running = AtomicBoolean(false)
    private val producer: KafkaProducer<String, ByteArray>?
    private var worker: Thread? = null
    private val dropped = AtomicLong(0)
    private val publishErrors = AtomicLong(0)

    init {
        producer =
            if (enabled) {
                KafkaProducer(
                    Properties().apply {
                        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                        put(ProducerConfig.CLIENT_ID_CONFIG, System.getenv("KAFKA_CLIENT_ID") ?: "gateway")
                        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
                        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java.name)
                        put(ProducerConfig.ACKS_CONFIG, "all")
                        put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                        put(ProducerConfig.LINGER_MS_CONFIG, "5")
                        put(ProducerConfig.BATCH_SIZE_CONFIG, "65536")
                        put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
                        put(ProducerConfig.RETRIES_CONFIG, "3")
                    }
                )
            } else {
                null
            }

        if (producer != null) {
            running.set(true)
            worker = thread(name = "kafka-event-publisher", isDaemon = false, start = true) {
                try {
                    while (running.get() || queue.isNotEmpty()) {
                        val ev =
                            try {
                                queue.poll(200, TimeUnit.MILLISECONDS)
                            } catch (_: InterruptedException) {
                                break
                            } ?: continue

                        try {
                            val key = ev.orderId ?: ev.accountId
                            val bytes = Json.mapper.writeValueAsBytes(ev)
                            producer.send(ProducerRecord(topic, key, bytes)) { _, ex ->
                                if (ex != null) publishErrors.incrementAndGet()
                            }
                        } catch (_: Throwable) {
                            publishErrors.incrementAndGet()
                        }
                    }
                } finally {
                    try {
                        producer.flush()
                    } catch (_: Throwable) {
                    }
                }
            }
        }
    }

    override fun publish(event: BusEvent) {
        if (!enabled || producer == null) return
        if (!queue.offer(event)) {
            dropped.incrementAndGet()
        }
    }

    fun isEnabled(): Boolean = enabled

    fun queueDepth(): Int = queue.size

    fun droppedTotal(): Long = dropped.get()

    fun publishErrorsTotal(): Long = publishErrors.get()

    override fun close() {
        running.set(false)
        worker?.interrupt()
        try {
            worker?.join(2000)
        } catch (_: InterruptedException) {
        }
        val droppedCount = dropped.get()
        val errorCount = publishErrors.get()
        if (enabled && (droppedCount > 0 || errorCount > 0)) {
            System.err.println("KafkaEventPublisher stats: dropped=$droppedCount errors=$errorCount topic=$topic")
        }
        try {
            producer?.close()
        } catch (_: Throwable) {
        }
    }
}
