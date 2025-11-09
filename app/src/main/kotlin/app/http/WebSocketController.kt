package app.http

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * 簡易WebSocketコントローラー
 * リアルタイム板情報配信用
 */
class WebSocketController(
    private val marketDataController: MarketDataController
) : HttpHandler, AutoCloseable {

    private val objectMapper = jacksonObjectMapper()
    private val connections = CopyOnWriteArraySet<WebSocketConnection>()

    // 定期的に板情報をブロードキャスト
    private val broadcaster = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "WebSocketBroadcaster").apply { isDaemon = true }
    }

    init {
        // 1秒ごとに全接続に板情報を配信
        broadcaster.scheduleAtFixedRate(
            ::broadcastOrderBooks,
            1000L,
            1000L,
            TimeUnit.MILLISECONDS
        )
    }

    override fun handle(exchange: HttpExchange) {
        try {
            // WebSocketハンドシェイク処理
            val upgradeHeader = exchange.requestHeaders.getFirst("Upgrade")
            val connectionHeader = exchange.requestHeaders.getFirst("Connection")
            val webSocketKey = exchange.requestHeaders.getFirst("Sec-WebSocket-Key")

            if (upgradeHeader != null && upgradeHeader.equals("websocket", ignoreCase = true) &&
                connectionHeader != null && connectionHeader.contains("Upgrade", ignoreCase = true) &&
                webSocketKey != null) {

                // WebSocketハンドシェイク応答
                performHandshake(exchange, webSocketKey)
            } else {
                // 通常のHTTPリクエスト（エラー応答）
                val response = "WebSocket endpoint. Use ws:// protocol."
                exchange.sendResponseHeaders(426, response.length.toLong())
                exchange.responseBody.use { it.write(response.toByteArray()) }
            }
        } catch (e: Exception) {
            System.err.println("WebSocket error: ${e.message}")
            e.printStackTrace()
        }
    }

    private fun performHandshake(exchange: HttpExchange, webSocketKey: String) {
        // RFC 6455に従ってAccept-Keyを生成
        val magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        val concatenated = webSocketKey + magic
        val sha1 = MessageDigest.getInstance("SHA-1")
        val hash = sha1.digest(concatenated.toByteArray(StandardCharsets.UTF_8))
        val acceptKey = Base64.getEncoder().encodeToString(hash)

        // WebSocketハンドシェイク応答
        exchange.responseHeaders.set("Upgrade", "websocket")
        exchange.responseHeaders.set("Connection", "Upgrade")
        exchange.responseHeaders.set("Sec-WebSocket-Accept", acceptKey)
        exchange.sendResponseHeaders(101, -1)

        // 接続を管理リストに追加
        val connection = WebSocketConnection(exchange.responseBody, exchange.remoteAddress.toString())
        connections.add(connection)

        println("WebSocket connected: ${connection.id} (Total: ${connections.size})")

        // 初回メッセージ送信
        sendWelcomeMessage(connection)
    }

    private fun sendWelcomeMessage(connection: WebSocketConnection) {
        val message = mapOf(
            "type" to "welcome",
            "message" to "Connected to Order Book WebSocket",
            "timestamp" to System.currentTimeMillis()
        )
        connection.send(objectMapper.writeValueAsString(message))
    }

    private fun broadcastOrderBooks() {
        try {
            // 全銘柄の板情報を取得してブロードキャスト
            val symbols = listOf("7203", "6758", "9984", "6861", "8306")

            symbols.forEach { symbol ->
                val depth = marketDataController.getOrderBookDepthPublic(symbol)
                if (depth != null) {
                    val message = mapOf(
                        "type" to "orderbook",
                        "data" to depth
                    )
                    val json = objectMapper.writeValueAsString(message)
                    broadcast(json)
                }
            }

            // 切断された接続を削除
            val disconnected = connections.filter { !it.isActive }
            disconnected.forEach {
                connections.remove(it)
                println("WebSocket disconnected: ${it.id} (Total: ${connections.size})")
            }
        } catch (e: Exception) {
            System.err.println("Broadcast error: ${e.message}")
        }
    }

    private fun broadcast(message: String) {
        connections.forEach { connection ->
            try {
                connection.send(message)
            } catch (e: Exception) {
                System.err.println("Failed to send to ${connection.id}: ${e.message}")
                connection.isActive = false
            }
        }
    }

    override fun close() {
        broadcaster.shutdown()
        try {
            if (!broadcaster.awaitTermination(5, TimeUnit.SECONDS)) {
                broadcaster.shutdownNow()
            }
        } catch (e: InterruptedException) {
            broadcaster.shutdownNow()
            Thread.currentThread().interrupt()
        }

        connections.forEach { it.close() }
        connections.clear()
    }
}

/**
 * WebSocket接続管理
 */
class WebSocketConnection(
    private val output: OutputStream,
    val id: String
) {
    var isActive = true
        internal set

    fun send(message: String) {
        if (!isActive) return

        try {
            val frame = createTextFrame(message)
            synchronized(output) {
                output.write(frame)
                output.flush()
            }
        } catch (e: Exception) {
            isActive = false
            throw e
        }
    }

    private fun createTextFrame(message: String): ByteArray {
        val payload = message.toByteArray(StandardCharsets.UTF_8)
        val payloadLength = payload.size

        // WebSocketフレーム作成（FIN=1, Opcode=0x1 (Text))
        val frame = mutableListOf<Byte>()
        frame.add(0x81.toByte()) // FIN=1, Opcode=0x1

        // ペイロード長
        when {
            payloadLength < 126 -> {
                frame.add(payloadLength.toByte())
            }
            payloadLength < 65536 -> {
                frame.add(126.toByte())
                frame.add((payloadLength shr 8).toByte())
                frame.add((payloadLength and 0xFF).toByte())
            }
            else -> {
                frame.add(127.toByte())
                for (i in 7 downTo 0) {
                    frame.add((payloadLength shr (i * 8)).toByte())
                }
            }
        }

        // ペイロード追加
        frame.addAll(payload.toList())

        return frame.toByteArray()
    }

    fun close() {
        try {
            isActive = false
            output.close()
        } catch (e: Exception) {
            // Ignore
        }
    }
}
