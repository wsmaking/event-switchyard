package backoffice.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import backoffice.auth.JwtAuth
import backoffice.auth.Principal
import backoffice.json.Json
import backoffice.store.InMemoryBackOfficeStore
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.Executors

class HttpBackOffice(
    private val port: Int,
    private val store: InMemoryBackOfficeStore,
    private val jwtAuth: JwtAuth,
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
                    val principal = requirePrincipal(ex) ?: return@createContext
                    val accountId = resolveAccountId(ex, principal) ?: return@createContext
                    val positions = store.listPositions(accountId)
                    sendJson(ex, 200, mapOf("positions" to positions))
                } catch (_: Throwable) {
                    sendText(ex, 500, "ERROR")
                } finally {
                    ex.close()
                }
            }

            createContext("/balances") { ex ->
                try {
                    if (ex.requestMethod != "GET") return@createContext sendText(ex, 405, "METHOD_NOT_ALLOWED")
                    val principal = requirePrincipal(ex) ?: return@createContext
                    val accountId = resolveAccountId(ex, principal) ?: return@createContext
                    val balances = store.listBalances(accountId)
                    sendJson(ex, 200, mapOf("balances" to balances))
                } catch (_: Throwable) {
                    sendText(ex, 500, "ERROR")
                } finally {
                    ex.close()
                }
            }

            createContext("/fills") { ex ->
                try {
                    if (ex.requestMethod != "GET") return@createContext sendText(ex, 405, "METHOD_NOT_ALLOWED")
                    val principal = requirePrincipal(ex) ?: return@createContext
                    val accountId = resolveAccountId(ex, principal) ?: return@createContext
                    val fills = store.listFills(accountId)
                    sendJson(ex, 200, mapOf("fills" to fills))
                } catch (_: Throwable) {
                    sendText(ex, 500, "ERROR")
                } finally {
                    ex.close()
                }
            }

            createContext("/pnl") { ex ->
                try {
                    if (ex.requestMethod != "GET") return@createContext sendText(ex, 405, "METHOD_NOT_ALLOWED")
                    val principal = requirePrincipal(ex) ?: return@createContext
                    val accountId = resolveAccountId(ex, principal) ?: return@createContext
                    val pnl = store.listRealizedPnl(accountId)
                    sendJson(ex, 200, mapOf("pnl" to pnl))
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

    private fun requirePrincipal(ex: HttpExchange): Principal? {
        return when (val auth = jwtAuth.authenticate(ex.requestHeaders.getFirst("Authorization"))) {
            is JwtAuth.Result.Ok -> auth.principal
            is JwtAuth.Result.Err -> {
                sendJson(ex, 401, mapOf("status" to "UNAUTHORIZED", "reason" to auth.reason))
                null
            }
        }
    }

    private fun resolveAccountId(ex: HttpExchange, principal: Principal): String? {
        val accountId = queryParam(ex, "accountId")
        if (accountId != null && accountId != principal.accountId) {
            sendText(ex, 404, "NOT_FOUND")
            return null
        }
        return accountId ?: principal.accountId
    }

    private fun queryParam(ex: HttpExchange, key: String): String? {
        val query = ex.requestURI.rawQuery ?: return null
        val pairs = query.split('&')
        for (pair in pairs) {
            if (pair.isBlank()) continue
            val idx = pair.indexOf('=')
            val k = if (idx >= 0) pair.substring(0, idx) else pair
            val v = if (idx >= 0) pair.substring(idx + 1) else ""
            if (URLDecoder.decode(k, UTF_8) == key) {
                val decoded = URLDecoder.decode(v, UTF_8)
                return decoded.takeIf { it.isNotBlank() }
            }
        }
        return null
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
