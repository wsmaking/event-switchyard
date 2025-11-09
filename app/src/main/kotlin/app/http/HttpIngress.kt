package app.http

import app.engine.Engine
import app.engine.Router
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.Executors

interface RequestHandler {
    fun handle(key: String, payload: ByteArray): Boolean
}

private class EngineHandler(private val engine: Engine) : RequestHandler {
    override fun handle(key: String, payload: ByteArray) = engine.handle(key, payload)
}

private class RouterHandler(private val router: Router) : RequestHandler {
    override fun handle(key: String, payload: ByteArray) = router.handle(key, payload)
}

class HttpIngress : AutoCloseable {
    private val handler: RequestHandler
    private val server: HttpServer
    private var router: Router? = null
    private var marketDataController: MarketDataController? = null

    constructor(engine: Engine, port: Int) {
        this.handler = EngineHandler(engine)
        this.server = createServer(port)
    }

    constructor(router: Router, port: Int) {
        this.handler = RouterHandler(router)
        this.router = router
        this.server = createServer(port)
    }

    private fun createServer(port: Int): HttpServer {
        return HttpServer.create(InetSocketAddress(port), 0).apply {
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
                    val ok = handler.handle(key, body)
                    if (ok) send(ex, 200, "OK") else send(ex, 409, "NOT_OWNER")
                } catch (_: Throwable) {
                    send(ex, 500, "ERROR")
                } finally {
                    ex.close()
                }
            }
            // Routerが利用可能な場合、/stats と /health と /metrics エンドポイントを追加
            router?.let { r ->
                createContext("/stats", StatsController(r))
                createContext("/health", HealthController(r))
                createContext("/metrics", MetricsController(r))

                val mdc = MarketDataController()
                this@HttpIngress.marketDataController = mdc
                val orderController = OrderController(r, mdc)
                createContext("/api/orders", orderController)
                createContext("/api/market/", mdc)
            }

            // フロントエンド静的ファイル配信 (最後に登録してAPI優先)
            createContext("/", StaticFileController())

            executor = Executors.newCachedThreadPool()
            start()
        }
    }

    private fun send(ex: HttpExchange, status: Int, text: String) {
        val bytes = text.toByteArray(UTF_8)
        ex.responseHeaders.add("Content-Type", "text/plain; charset=utf-8")
        ex.sendResponseHeaders(status, bytes.size.toLong())
        ex.responseBody.use { it.write(bytes) }
    }

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
        marketDataController?.close()
        server.stop(0)
    }
}
