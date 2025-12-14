package backoffice.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import backoffice.json.Json
import backoffice.store.InMemoryBackOfficeStore
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.Executors

class HttpBackOffice(
    private val port: Int,
    private val store: InMemoryBackOfficeStore,
    private val mapper: ObjectMapper = Json.mapper
) : AutoCloseable {
    private val server: HttpServer =
        HttpServer.create(InetSocketAddress(port), 0).apply {
            createContext("/health") { ex ->
                try {
                    if (ex.requestMethod != "GET") return@createContext sendText(ex, 405, "METHOD_NOT_ALLOWED")
                    sendJson(ex, 200, mapOf("status" to "ok"))
                } finally {
                    ex.close()
                }
            }

            createContext("/positions") { ex ->
                try {
                    if (ex.requestMethod != "GET") return@createContext sendText(ex, 405, "METHOD_NOT_ALLOWED")
                    val accountId = ex.requestURI.query.orEmpty().split('&')
                        .firstOrNull { it.startsWith("accountId=") }
                        ?.substringAfter("accountId=")
                        ?.takeIf { it.isNotBlank() }
                    val positions = store.listPositions(accountId)
                    sendJson(ex, 200, mapOf("positions" to positions))
                } catch (_: Throwable) {
                    sendText(ex, 500, "ERROR")
                } finally {
                    ex.close()
                }
            }

            executor = Executors.newCachedThreadPool()
        }

    fun start() {
        server.start()
        println("BackOffice listening on :$port")
    }

    private fun sendJson(ex: HttpExchange, status: Int, body: Any) {
        val bytes = mapper.writeValueAsBytes(body)
        ex.responseHeaders.add("Content-Type", "application/json; charset=utf-8")
        ex.sendResponseHeaders(status, bytes.size.toLong())
        ex.responseBody.use { it.write(bytes) }
    }

    private fun sendText(ex: HttpExchange, status: Int, text: String) {
        val bytes = text.toByteArray(UTF_8)
        ex.responseHeaders.add("Content-Type", "text/plain; charset=utf-8")
        ex.sendResponseHeaders(status, bytes.size.toLong())
        ex.responseBody.use { it.write(bytes) }
    }

    override fun close() {
        server.stop(0)
    }
}

