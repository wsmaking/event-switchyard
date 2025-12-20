package backoffice.kafka

import backoffice.json.Json
import backoffice.ledger.FileLedger
import backoffice.ledger.LedgerFill
import backoffice.ledger.LedgerOrderAccepted
import backoffice.model.BusEvent
import backoffice.model.Side
import backoffice.store.InMemoryBackOfficeStore
import backoffice.store.OrderMeta
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class BackOfficeConsumer(
    private val store: InMemoryBackOfficeStore,
    private val ledger: FileLedger,
    bootstrapServers: String = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092",
    topic: String = System.getenv("KAFKA_TOPIC") ?: "events",
    groupId: String = System.getenv("KAFKA_GROUP_ID") ?: "backoffice",
    private val quoteCcy: String = System.getenv("BACKOFFICE_QUOTE_CCY") ?: "JPY",
    feeBps: Long = (System.getenv("BACKOFFICE_FEE_BPS") ?: "10").toLong(),
    private val enabled: Boolean =
        (System.getenv("BACKOFFICE_ENABLE") ?: "1").let { it == "1" || it.equals("true", ignoreCase = true) }
) : AutoCloseable {
    private val running = AtomicBoolean(false)
    private val consumer: KafkaConsumer<String, ByteArray>?
    private val topic: String
    private var worker: Thread? = null
    private val feeBps: Long = feeBps.coerceAtLeast(0)
    private val pendingOffsets = ConcurrentHashMap<TopicPartition, Long>()

    init {
        this.topic = topic
        consumer =
            if (enabled) {
                KafkaConsumer<String, ByteArray>(
                    Properties().apply {
                        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                        put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
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
        consumer.subscribe(listOf(topic), object : ConsumerRebalanceListener {
            override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
                commitOffsets()
            }

            override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
            }
        })
        worker = thread(name = "backoffice-consumer", isDaemon = false, start = true) {
            try {
                while (running.get()) {
                    val records = consumer.poll(Duration.ofMillis(250))
                    var processed = 0
                    for (rec in records) {
                        if (processRecord(rec)) {
                            val tp = TopicPartition(rec.topic(), rec.partition())
                            pendingOffsets[tp] = rec.offset()
                        }
                        processed++
                    }
                    if (processed > 0) {
                        commitOffsets()
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

    private fun processRecord(rec: ConsumerRecord<String, ByteArray>): Boolean {
        val ev =
            try {
                Json.mapper.readValue(rec.value(), BusEvent::class.java)
            } catch (_: Throwable) {
                return true
            }

        when (ev.type) {
            "OrderAccepted" -> onOrderAccepted(ev)
            "ExecutionReport" -> onExecutionReport(ev)
            else -> return true
        }
        return true
    }

    private fun onOrderAccepted(ev: BusEvent) {
        val orderId = ev.orderId ?: return
        if (store.findOrderMeta(orderId) != null) return
        val symbol = ev.data["symbol"] as? String ?: return
        val sideStr = ev.data["side"] as? String ?: return
        val side =
            try {
                Side.valueOf(sideStr)
            } catch (_: Throwable) {
                return
            }
        ledger.append(
            LedgerOrderAccepted(
                at = ev.at,
                accountId = ev.accountId,
                orderId = orderId,
                symbol = symbol,
                side = side
            )
        )
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
        val lastTotal = store.lastFilledTotal(orderId)

        val filledDelta =
            when (val v = ev.data["filledQtyDelta"]) {
                is Number -> v.toLong()
                is String -> v.toLongOrNull()
                else -> null
            } ?: return

        val price =
            when (val v = ev.data["price"]) {
                is Number -> v.toLong()
                is String -> v.toLongOrNull()
                else -> null
            }

        val filledTotal =
            when (val v = ev.data["filledQtyTotal"]) {
                is Number -> v.toLong()
                is String -> v.toLongOrNull()
                else -> null
            } ?: return

        if (filledTotal <= lastTotal) return
        val delta = filledDelta.coerceAtMost(filledTotal - lastTotal).coerceAtLeast(0L)
        if (delta <= 0L) return

        val notional = if (price == null) 0L else delta * price
        val fee = (notional * feeBps) / 10_000L
        val cashDeltaExFee = if (meta.side == Side.BUY) -notional else notional
        val cashDelta = cashDeltaExFee - fee

        ledger.append(
            LedgerFill(
                at = ev.at,
                accountId = meta.accountId,
                orderId = orderId,
                symbol = meta.symbol,
                side = meta.side,
                filledQtyDelta = delta,
                filledQtyTotal = filledTotal,
                price = price,
                quoteCcy = quoteCcy,
                quoteCashDelta = cashDelta,
                feeQuote = fee
            )
        )

        store.applyFill(
            at = ev.at,
            accountId = meta.accountId,
            orderId = orderId,
            symbol = meta.symbol,
            side = meta.side,
            filledQtyDelta = delta,
            filledQtyTotal = filledTotal,
            price = price,
            quoteCcy = quoteCcy,
            quoteCashDelta = cashDelta,
            feeQuote = fee
        )
    }

    private fun commitOffsets() {
        if (consumer == null) return
        if (pendingOffsets.isEmpty()) return
        val commitMap = HashMap<TopicPartition, OffsetAndMetadata>()
        for ((tp, offset) in pendingOffsets.entries) {
            commitMap[tp] = OffsetAndMetadata(offset + 1)
        }
        try {
            consumer.commitSync(commitMap)
        } catch (_: Throwable) {
            // best-effort; on next poll, offsets will be re-committed
        }
    }

    override fun close() {
        running.set(false)
        worker?.interrupt()
        try {
            worker?.join(1000)
        } catch (_: InterruptedException) {
        }
        commitOffsets()
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
