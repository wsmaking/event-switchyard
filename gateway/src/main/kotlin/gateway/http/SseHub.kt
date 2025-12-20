package gateway.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.HttpExchange
import gateway.json.Json
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

class SseHub(
    private val mapper: ObjectMapper = Json.mapper,
    bufferSize: Int = (System.getenv("SSE_BUFFER_SIZE") ?: "1000").toInt(),
    private val keepAliveSec: Long = (System.getenv("SSE_KEEPALIVE_SEC") ?: "15").toLong(),
    queueCapacity: Int = (System.getenv("SSE_DISPATCH_QUEUE_CAPACITY") ?: "10000").toInt()
) : AutoCloseable {
    private val clientsByOrderId = ConcurrentHashMap<String, CopyOnWriteArrayList<SseClient>>()
    private val clientsByAccountId = ConcurrentHashMap<String, CopyOnWriteArrayList<SseClient>>()

    private val nextEventId = AtomicLong(0)
    private val orderBuffers = ConcurrentHashMap<String, EventBuffer>()
    private val accountBuffers = ConcurrentHashMap<String, EventBuffer>()
    private val dispatchQueue = ArrayBlockingQueue<Dispatch>(queueCapacity)
    private val dispatchDropped = AtomicLong(0)
    private val running = AtomicBoolean(true)
    private val worker: Thread
    private val defaultBufferSize: Int = bufferSize

    init {
        worker = thread(name = "sse-dispatch", isDaemon = false, start = true) {
            val keepAliveMs = maxOf(1L, keepAliveSec) * 1000L
            while (running.get()) {
                try {
                    val item = dispatchQueue.poll(keepAliveMs, TimeUnit.MILLISECONDS)
                    if (item == null) {
                        sendKeepAlive()
                        continue
                    }
                    when (item.target) {
                        is DispatchTarget.Order -> sendToOrder(item.target.orderId, item.event)
                        is DispatchTarget.Account -> sendToAccount(item.target.accountId, item.event)
                    }
                } catch (_: InterruptedException) {
                    break
                } catch (_: Throwable) {
                }
            }
        }
    }

    fun open(orderId: String, lastEventId: Long?, ex: HttpExchange) {
        ex.responseHeaders.add("Content-Type", "text/event-stream; charset=utf-8")
        ex.responseHeaders.add("Cache-Control", "no-cache")
        ex.responseHeaders.add("Connection", "keep-alive")
        ex.sendResponseHeaders(200, 0)

        val writer = BufferedWriter(OutputStreamWriter(ex.responseBody, UTF_8))
        val client = SseClient(ex, writer)
        clientsByOrderId.computeIfAbsent(orderId) { CopyOnWriteArrayList() }.add(client)

        // Make sure the worker doesn't interleave messages with the initial replay.
        client.sendEvent(
            id = null,
            event = "ready",
            jsonData = mapper.writeValueAsString(mapOf("orderId" to orderId))
        )
        maybeSendResync(
            scope = "order",
            entityId = orderId,
            lastEventId = lastEventId,
            buffer = orderBuffer(orderId),
            client = client
        )
        replayOrder(orderId, lastEventId, client)
    }

    fun openAccount(accountId: String, lastEventId: Long?, ex: HttpExchange) {
        ex.responseHeaders.add("Content-Type", "text/event-stream; charset=utf-8")
        ex.responseHeaders.add("Cache-Control", "no-cache")
        ex.responseHeaders.add("Connection", "keep-alive")
        ex.sendResponseHeaders(200, 0)

        val writer = BufferedWriter(OutputStreamWriter(ex.responseBody, UTF_8))
        val client = SseClient(ex, writer)
        clientsByAccountId.computeIfAbsent(accountId) { CopyOnWriteArrayList() }.add(client)

        client.sendEvent(
            id = null,
            event = "ready",
            jsonData = mapper.writeValueAsString(mapOf("accountId" to accountId))
        )
        replayAccount(accountId, lastEventId, client)
    }

    fun publish(orderId: String, event: String, data: Any) {
        val e = SseEvent(id = nextEventId(), event = event, jsonData = mapper.writeValueAsString(data))
        orderBuffer(orderId).append(e)
        if (!dispatchQueue.offer(Dispatch(DispatchTarget.Order(orderId), e))) {
            dispatchDropped.incrementAndGet()
        }
    }

    fun publishAccount(accountId: String, event: String, data: Any) {
        val e = SseEvent(id = nextEventId(), event = event, jsonData = mapper.writeValueAsString(data))
        accountBuffer(accountId).append(e)
        if (!dispatchQueue.offer(Dispatch(DispatchTarget.Account(accountId), e))) {
            dispatchDropped.incrementAndGet()
        }
    }

    private fun sendToOrder(orderId: String, event: SseEvent) {
        val clients = clientsByOrderId[orderId] ?: return
        if (clients.isEmpty()) return
        val dead = mutableListOf<SseClient>()
        for (c in clients) {
            val ok = c.sendEvent(event.id, event.event, event.jsonData)
            if (!ok) dead.add(c)
        }
        if (dead.isNotEmpty()) {
            clients.removeAll(dead.toSet())
            dead.forEach { it.closeQuietly() }
        }
    }

    private fun sendToAccount(accountId: String, event: SseEvent) {
        val clients = clientsByAccountId[accountId] ?: return
        if (clients.isEmpty()) return
        val dead = mutableListOf<SseClient>()
        for (c in clients) {
            val ok = c.sendEvent(event.id, event.event, event.jsonData)
            if (!ok) dead.add(c)
        }
        if (dead.isNotEmpty()) {
            clients.removeAll(dead.toSet())
            dead.forEach { it.closeQuietly() }
        }
    }

    private fun sendKeepAlive() {
        val dead = mutableListOf<SseClient>()

        for (list in clientsByOrderId.values) {
            for (c in list) {
                val ok = c.sendComment("keepalive")
                if (!ok) dead.add(c)
            }
            if (dead.isNotEmpty()) {
                list.removeAll(dead.toSet())
                dead.forEach { it.closeQuietly() }
                dead.clear()
            }
        }

        for (list in clientsByAccountId.values) {
            for (c in list) {
                val ok = c.sendComment("keepalive")
                if (!ok) dead.add(c)
            }
            if (dead.isNotEmpty()) {
                list.removeAll(dead.toSet())
                dead.forEach { it.closeQuietly() }
                dead.clear()
            }
        }
    }

    private fun replayOrder(orderId: String, lastEventId: Long?, client: SseClient) {
        val from = lastEventId ?: return
        val events = orderBuffer(orderId).after(from)
        for (e in events) {
            client.sendEvent(e.id, e.event, e.jsonData)
        }
    }

    private fun replayAccount(accountId: String, lastEventId: Long?, client: SseClient) {
        val from = lastEventId ?: return
        val events = accountBuffer(accountId).after(from)
        for (e in events) {
            client.sendEvent(e.id, e.event, e.jsonData)
        }
    }

    private fun maybeSendResync(
        scope: String,
        entityId: String,
        lastEventId: Long?,
        buffer: EventBuffer,
        client: SseClient
    ) {
        val from = lastEventId ?: return
        val range = buffer.idRange() ?: return
        if (from >= range.first) return
        val data =
            mapOf(
                "scope" to scope,
                "id" to entityId,
                "lastEventId" to from,
                "oldestAvailableId" to range.first,
                "eventsEndpoint" to when (scope) {
                    "order" -> "/orders/$entityId/events"
                    else -> "/accounts/$entityId/events"
                }
            )
        client.sendEvent(id = null, event = "resync_required", jsonData = mapper.writeValueAsString(data))
    }

    private fun nextEventId(): Long = nextEventId.incrementAndGet()

    private fun orderBuffer(orderId: String): EventBuffer {
        return orderBuffers.computeIfAbsent(orderId) { EventBuffer(defaultBufferSize) }
    }

    private fun accountBuffer(accountId: String): EventBuffer {
        return accountBuffers.computeIfAbsent(accountId) { EventBuffer(defaultBufferSize) }
    }

    fun orderClientCount(): Int = clientsByOrderId.values.sumOf { it.size }

    fun accountClientCount(): Int = clientsByAccountId.values.sumOf { it.size }

    fun dispatchQueueDepth(): Int = dispatchQueue.size

    fun dispatchDroppedTotal(): Long = dispatchDropped.get()

    override fun close() {
        running.set(false)
        worker.interrupt()
        try {
            worker.join(1000)
        } catch (_: InterruptedException) {
        }
    }

    private data class SseEvent(
        val id: Long,
        val event: String,
        val jsonData: String
    )

    private sealed interface DispatchTarget {
        data class Order(val orderId: String) : DispatchTarget
        data class Account(val accountId: String) : DispatchTarget
    }

    private data class Dispatch(val target: DispatchTarget, val event: SseEvent)

    private class EventBuffer(private val capacity: Int) {
        private val deque = ArrayDeque<SseEvent>(capacity)

        fun append(event: SseEvent) {
            synchronized(this) {
                deque.addLast(event)
                while (deque.size > capacity) deque.removeFirst()
            }
        }

        fun after(lastId: Long): List<SseEvent> {
            synchronized(this) {
                if (deque.isEmpty()) return emptyList()
                return deque.filter { it.id > lastId }
            }
        }

        fun idRange(): LongRange? {
            synchronized(this) {
                if (deque.isEmpty()) return null
                return deque.first().id..deque.last().id
            }
        }
    }

    private class SseClient(
        private val ex: HttpExchange,
        private val writer: BufferedWriter
    ) {
        private val lock = Any()

        fun sendEvent(id: Long?, event: String, jsonData: String): Boolean {
            return try {
                synchronized(lock) {
                    if (id != null) {
                        writer.write("id: ")
                        writer.write(id.toString())
                        writer.newLine()
                    }
                    writer.write("event: ")
                    writer.write(event)
                    writer.newLine()
                    writer.write("data: ")
                    writer.write(jsonData)
                    writer.newLine()
                    writer.newLine()
                    writer.flush()
                }
                true
            } catch (_: Throwable) {
                false
            }
        }

        fun sendComment(comment: String): Boolean {
            return try {
                synchronized(lock) {
                    writer.write(": ")
                    writer.write(comment)
                    writer.newLine()
                    writer.newLine()
                    writer.flush()
                }
                true
            } catch (_: Throwable) {
                false
            }
        }

        fun closeQuietly() {
            try {
                ex.close()
            } catch (_: Throwable) {
            }
        }
    }
}
