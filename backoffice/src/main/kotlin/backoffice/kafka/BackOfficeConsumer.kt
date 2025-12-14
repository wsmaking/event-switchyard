package backoffice.kafka

import backoffice.json.Json
import backoffice.model.BusEvent
import backoffice.model.Side
import backoffice.store.InMemoryBackOfficeStore
import backoffice.store.OrderMeta
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class BackOfficeConsumer(
    private val store: InMemoryBackOfficeStore,
    bootstrapServers: String = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092",
    topic: String = System.getenv("KAFKA_TOPIC") ?: "events",
    groupId: String = System.getenv("KAFKA_GROUP_ID") ?: "backoffice",
    private val enabled: Boolean =
        (System.getenv("BACKOFFICE_ENABLE") ?: "1").let { it == "1" || it.equals("true", ignoreCase = true) }
) : AutoCloseable {
    private val running = AtomicBoolean(false)
    private val consumer: KafkaConsumer<String, ByteArray>?
    private val topic: String
    private var worker: Thread? = null

    init {
        this.topic = topic
        consumer =
            if (enabled) {
                KafkaConsumer<String, ByteArray>(
                    Properties().apply {
                        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                        put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
                    }
                )
            } else {
                null
            }
    }

    fun start() {
        if (!enabled || consumer == null) {
            println("BackOfficeConsumer disabled (BACKOFFICE_ENABLE=0)")
            return
        }
        if (!running.compareAndSet(false, true)) return
        consumer.subscribe(listOf(topic))
        worker = thread(name = "backoffice-consumer", isDaemon = false, start = true) {
            try {
                while (running.get()) {
                    val records = consumer.poll(Duration.ofMillis(250))
                    for (rec in records) {
                        onRecord(rec)
                    }
                }
            } catch (_: InterruptedException) {
            } catch (_: Throwable) {
            } finally {
                try {
                    consumer.close()
                } catch (_: Throwable) {
                }
            }
        }
    }

    private fun onRecord(rec: ConsumerRecord<String, ByteArray>) {
        val ev =
            try {
                Json.mapper.readValue(rec.value(), BusEvent::class.java)
            } catch (_: Throwable) {
                return
            }

        when (ev.type) {
            "OrderAccepted" -> onOrderAccepted(ev)
            "ExecutionReport" -> onExecutionReport(ev)
        }
    }

    private fun onOrderAccepted(ev: BusEvent) {
        val orderId = ev.orderId ?: return
        val symbol = ev.data["symbol"] as? String ?: return
        val sideStr = ev.data["side"] as? String ?: return
        val side =
            try {
                Side.valueOf(sideStr)
            } catch (_: Throwable) {
                return
            }
        store.upsertOrderMeta(
            OrderMeta(
                accountId = ev.accountId,
                orderId = orderId,
                symbol = symbol,
                side = side
            )
        )
    }

    private fun onExecutionReport(ev: BusEvent) {
        val orderId = ev.orderId ?: return
        val meta = store.findOrderMeta(orderId) ?: return

        val filledDelta =
            when (val v = ev.data["filledQtyDelta"]) {
                is Number -> v.toLong()
                is String -> v.toLongOrNull()
                else -> null
            } ?: return

        val price =
            when (val v = ev.data["price"]) {
                is Number -> v.toDouble()
                is String -> v.toDoubleOrNull()
                else -> null
            }

        val signed = if (meta.side == Side.BUY) filledDelta else -filledDelta
        store.applyFill(meta.accountId, meta.symbol, signed, price)
    }

    override fun close() {
        running.set(false)
        worker?.interrupt()
        try {
            worker?.join(1000)
        } catch (_: InterruptedException) {
        }
        try {
            consumer?.wakeup()
        } catch (_: Throwable) {
        }
        try {
            consumer?.close()
        } catch (_: Throwable) {
        }
    }
}

