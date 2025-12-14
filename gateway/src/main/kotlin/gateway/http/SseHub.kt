package gateway.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.HttpExchange
import gateway.json.Json
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

class SseHub(
    private val mapper: ObjectMapper = Json.mapper
) {
    private val clientsByOrderId = ConcurrentHashMap<String, CopyOnWriteArrayList<SseClient>>()
    private val clientsByAccountId = ConcurrentHashMap<String, CopyOnWriteArrayList<SseClient>>()

    fun open(orderId: String, ex: HttpExchange) {
        ex.responseHeaders.add("Content-Type", "text/event-stream; charset=utf-8")
        ex.responseHeaders.add("Cache-Control", "no-cache")
        ex.responseHeaders.add("Connection", "keep-alive")
        ex.sendResponseHeaders(200, 0)

        val writer = BufferedWriter(OutputStreamWriter(ex.responseBody, UTF_8))
        val client = SseClient(ex, writer)
        clientsByOrderId.computeIfAbsent(orderId) { CopyOnWriteArrayList() }.add(client)

        val json = mapper.writeValueAsString(mapOf("orderId" to orderId))
        client.sendRaw(event = "ready", jsonData = json)
    }

    fun openAccount(accountId: String, ex: HttpExchange) {
        ex.responseHeaders.add("Content-Type", "text/event-stream; charset=utf-8")
        ex.responseHeaders.add("Cache-Control", "no-cache")
        ex.responseHeaders.add("Connection", "keep-alive")
        ex.sendResponseHeaders(200, 0)

        val writer = BufferedWriter(OutputStreamWriter(ex.responseBody, UTF_8))
        val client = SseClient(ex, writer)
        clientsByAccountId.computeIfAbsent(accountId) { CopyOnWriteArrayList() }.add(client)

        val json = mapper.writeValueAsString(mapOf("accountId" to accountId))
        client.sendRaw(event = "ready", jsonData = json)
    }

    fun publish(orderId: String, event: String, data: Any) {
        val clients = clientsByOrderId[orderId] ?: return
        if (clients.isEmpty()) return

        val json = mapper.writeValueAsString(data)
        val dead = mutableListOf<SseClient>()

        for (c in clients) {
            val ok = c.sendRaw(event = event, jsonData = json)
            if (!ok) dead.add(c)
        }

        if (dead.isNotEmpty()) {
            clients.removeAll(dead.toSet())
            dead.forEach { it.closeQuietly() }
        }
    }

    fun publishAccount(accountId: String, event: String, data: Any) {
        val clients = clientsByAccountId[accountId] ?: return
        if (clients.isEmpty()) return

        val json = mapper.writeValueAsString(data)
        val dead = mutableListOf<SseClient>()
        for (c in clients) {
            val ok = c.sendRaw(event = event, jsonData = json)
            if (!ok) dead.add(c)
        }
        if (dead.isNotEmpty()) {
            clients.removeAll(dead.toSet())
            dead.forEach { it.closeQuietly() }
        }
    }

    private class SseClient(
        private val ex: HttpExchange,
        private val writer: BufferedWriter
    ) {
        private val lock = Any()

        fun sendRaw(event: String, jsonData: String): Boolean {
            return try {
                synchronized(lock) {
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

        fun closeQuietly() {
            try {
                ex.close()
            } catch (_: Throwable) {
            }
        }
    }
}
