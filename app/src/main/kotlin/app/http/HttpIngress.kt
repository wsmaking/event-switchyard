package app.http

import app.engine.Engine
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.Executors

class HttpIngress(
    private val engine: Engine,
    port: Int
) : AutoCloseable {

    private val server: HttpServer = HttpServer.create(InetSocketAddress(port), 0).apply {
        createContext("/events") { ex ->
            try {
                if (ex.requestMethod != "POST") {
                    return@createContext send(ex, 405, "METHOD_NOT_ALLOWED")
                }
                val params = parseQuery(ex.requestURI.rawQuery ?: "")
                val key = params["key"]
                if (key.isNullOrEmpty()) {
                    return@createContext send(ex, 400, "MISSING_KEY")
                }
                val body = ex.requestBody.readAllBytes()
                val ok = engine.handle(key, body)
                if (ok) send(ex, 200, "OK") else send(ex, 409, "NOT_OWNER")
            } catch (_: Throwable) {
                send(ex, 500, "ERROR")
            } finally {
                ex.close()
            }
        }
        executor = Executors.newCachedThreadPool()
        start()
    }

    private fun send(ex: HttpExchange, status: Int, text: String) {
        val bytes = text.toByteArray(UTF_8)
        ex.responseHeaders.add("Content-Type", "text/plain; charset=utf-8")
        ex.sendResponseHeaders(status, bytes.size.toLong())
        ex.responseBody.use { it.write(bytes) }
    }

    // "&"区切りを安全にMapへ。空要素や "k" だけのケースにも耐性あり
    private fun parseQuery(q: String): Map<String, String> {
        if (q.isEmpty()) return emptyMap()
        val pairs = mutableListOf<Pair<String, String>>()
        for (item in q.split('&')) {
            if (item.isEmpty()) continue
            val idx = item.indexOf('=')
            val k = if (idx >= 0) item.substring(0, idx) else item
            val v = if (idx >= 0) item.substring(idx + 1) else ""
            pairs += URLDecoder.decode(k, UTF_8) to URLDecoder.decode(v, UTF_8)
        }
        return pairs.toMap()
    }

    override fun close() {
        server.stop(0)
    }
}