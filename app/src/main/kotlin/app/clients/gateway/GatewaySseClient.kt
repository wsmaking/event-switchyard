package app.clients.gateway

import app.auth.JwtSigner
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

data class GatewaySseEvent(
    val id: Long?,
    val event: String,
    val data: String
)

data class GatewayExecutionReport(
    val orderId: String? = null,
    val status: String? = null,
    val filledQtyDelta: Long? = null,
    val filledQtyTotal: Long? = null,
    val price: Long? = null,
    val at: String? = null
)

interface GatewaySseListener {
    fun onGatewayEvent(event: GatewaySseEvent)
}

class GatewaySseClient(
    baseUrl: String = System.getenv("GATEWAY_BASE_URL") ?: "http://localhost:8081",
    private val accountId: String = System.getenv("ACCOUNT_ID") ?: "acct_demo",
    private val staticJwt: String? = System.getenv("GATEWAY_JWT")?.trim()?.takeIf { it.isNotEmpty() },
    private val jwtFilePath: String? = System.getenv("GATEWAY_JWT_FILE")?.trim()?.takeIf { it.isNotEmpty() },
    private val jwtSigner: JwtSigner? = JwtSigner.fromEnv(),
    private val httpClient: OkHttpClient = OkHttpClient.Builder()
        .callTimeout(0, TimeUnit.SECONDS)
        .readTimeout(0, TimeUnit.SECONDS)
        .build(),
    private val enabled: Boolean =
        (System.getenv("GATEWAY_SSE_ENABLE") ?: "1").let { it == "1" || it.equals("true", ignoreCase = true) }
) : AutoCloseable {
    private val url = baseUrl.trimEnd('/') + "/stream"
    private val running = AtomicBoolean(false)
    private val lastEventId = AtomicLong(0)
    private var worker: Thread? = null

    fun start(listener: GatewaySseListener) {
        if (!enabled) {
            println("GatewaySseClient disabled (GATEWAY_SSE_ENABLE=0)")
            return
        }
        if (!running.compareAndSet(false, true)) return
        worker = thread(name = "gateway-sse", isDaemon = true, start = true) {
            while (running.get()) {
                val ok = connect(listener)
                if (!ok) {
                    sleepBackoff()
                }
            }
        }
    }

    private fun connect(listener: GatewaySseListener): Boolean {
        val token = loadJwt() ?: jwtSigner?.sign(accountId) ?: return false
        val reqBuilder = Request.Builder()
            .url(url)
            .get()
            .header("Authorization", "Bearer $token")

        val last = lastEventId.get()
        if (last > 0) {
            reqBuilder.header("Last-Event-ID", last.toString())
        }

        httpClient.newCall(reqBuilder.build()).execute().use { resp ->
            if (!resp.isSuccessful) return false
            val body = resp.body ?: return false
            val reader = BufferedReader(InputStreamReader(body.byteStream(), UTF_8))
            var eventName = "message"
            var dataBuffer = StringBuilder()
            var id: Long? = null

            while (running.get()) {
                val line = reader.readLine() ?: break
                if (line.isEmpty()) {
                    if (dataBuffer.isNotEmpty()) {
                        val payload = dataBuffer.toString()
                        listener.onGatewayEvent(GatewaySseEvent(id, eventName, payload))
                        if (id != null) {
                            lastEventId.set(id!!)
                        }
                        dataBuffer = StringBuilder()
                        eventName = "message"
                        id = null
                    }
                    continue
                }
                if (line.startsWith(":")) continue
                when {
                    line.startsWith("event:") -> eventName = line.substringAfter("event:").trim().ifEmpty { "message" }
                    line.startsWith("data:") -> {
                        if (dataBuffer.isNotEmpty()) dataBuffer.append('\n')
                        dataBuffer.append(line.substringAfter("data:").trimStart())
                    }
                    line.startsWith("id:") -> {
                        id = line.substringAfter("id:").trim().toLongOrNull()
                    }
                }
            }
        }
        return false
    }

    private fun sleepBackoff() {
        try {
            Thread.sleep(1000)
        } catch (_: InterruptedException) {
        }
    }

    private fun loadJwt(): String? {
        if (!jwtFilePath.isNullOrBlank()) {
            val text =
                try {
                    java.io.File(jwtFilePath).readText()
                } catch (_: Throwable) {
                    null
                }
            val trimmed = text?.trim()
            if (!trimmed.isNullOrEmpty()) return trimmed
        }
        return staticJwt
    }

    override fun close() {
        running.set(false)
        worker?.interrupt()
        try {
            worker?.join(1000)
        } catch (_: InterruptedException) {
        }
    }
}
