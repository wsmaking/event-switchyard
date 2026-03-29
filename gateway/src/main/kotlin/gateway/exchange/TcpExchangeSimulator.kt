package gateway.exchange

import gateway.json.Json
import gateway.order.OrderSnapshot
import gateway.order.OrderSide
import gateway.order.OrderStatus
import gateway.order.OrderType
import gateway.order.TimeInForce
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStream
import java.io.InputStreamReader
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.time.Instant
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

// TCPで注文を受け取り、ExecutionReportを返す疑似取引所サーバ。
// 受信した注文は ExchangeSimulator に渡し、結果だけをTCPで返す。
class TcpExchangeSimulator(
    private val bindHost: String = System.getenv("EXCHANGE_TCP_BIND") ?: "0.0.0.0",
    private val port: Int = (System.getenv("EXCHANGE_TCP_PORT") ?: "9901").toInt(),
    private val adminBindHost: String = System.getenv("EXCHANGE_SIM_ADMIN_BIND") ?: "127.0.0.1",
    private val adminPort: Int = (System.getenv("EXCHANGE_SIM_ADMIN_PORT") ?: "9902").toInt(),
    private val exchange: ExchangeClient = ExchangeSimulator()
) : AutoCloseable {
    private val mapper = Json.mapper
    private val running = AtomicBoolean(true)
    private val serverSocket = ServerSocket()
    private val clientPool: ExecutorService =
        Executors.newCachedThreadPool { r -> Thread(r, "tcp-exchange-client").apply { isDaemon = false } }
    private var adminServer: HttpServer? = null

    fun start() {
        serverSocket.bind(InetSocketAddress(bindHost, port))
        startAdminServer()
        thread(name = "tcp-exchange-accept", isDaemon = false, start = true) {
            while (running.get()) {
                val socket = try {
                    serverSocket.accept()
                } catch (_: Throwable) {
                    break
                }
                clientPool.execute { handle(socket) }
            }
        }
        println("TCP Exchange Simulator listening on $bindHost:$port")
    }

    override fun close() {
        running.set(false)
        try {
            serverSocket.close()
        } catch (_: Throwable) {
        }
        exchange.close()
        clientPool.shutdownNow()
        adminServer?.stop(0)
    }

    private fun handle(socket: Socket) {
        socket.tcpNoDelay = true
        val writer = BufferedWriter(OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8))
        val reader = BufferedReader(InputStreamReader(socket.getInputStream(), Charsets.UTF_8))
        val writerLock = Any()

        // 1行1JSONでExecutionReportを返す。
        fun sendReport(report: ExecutionReport) {
            val line = mapper.writeValueAsString(report)
            synchronized(writerLock) {
                writer.write(line)
                writer.newLine()
                writer.flush()
            }
        }

        while (running.get()) {
            val line = reader.readLine() ?: break
            val request =
                try {
                    mapper.readValue(line, TcpExchangeRequest::class.java)
                } catch (_: Throwable) {
                    continue
                }
            when (request.type) {
                TcpExchangeRequestType.NEW -> handleNewOrder(request, ::sendReport)
                TcpExchangeRequestType.CANCEL -> handleCancel(request, ::sendReport)
            }
        }
        try {
            socket.close()
        } catch (_: Throwable) {
        }
    }

    // 受信した注文を疑似取引所に渡し、返ってきた結果をそのまま返信。
    private fun handleNewOrder(request: TcpExchangeRequest, onReport: (ExecutionReport) -> Unit) {
        val symbol = request.symbol ?: return reject(request.orderId, onReport)
        val side = request.side ?: OrderSide.BUY
        val qty = request.qty ?: return reject(request.orderId, onReport)
        val now = Instant.now()
        val snapshot =
            OrderSnapshot(
                orderId = request.orderId,
                accountId = "tcp-sim",
                clientOrderId = null,
                symbol = symbol,
                side = side,
                type = OrderType.LIMIT,
                qty = qty,
                price = request.price,
                timeInForce = TimeInForce.GTC,
                expireAt = null,
                status = OrderStatus.ACCEPTED,
                acceptedAt = now
            )
        exchange.sendNewOrder(snapshot, onReport)
    }

    private fun handleCancel(request: TcpExchangeRequest, onReport: (ExecutionReport) -> Unit) {
        exchange.sendCancel(request.orderId, onReport)
    }

    private fun reject(orderId: String, onReport: (ExecutionReport) -> Unit) {
        onReport(
            ExecutionReport(
                orderId = orderId,
                status = OrderStatus.REJECTED,
                filledQtyDelta = 0,
                filledQtyTotal = 0,
                price = null,
                at = Instant.now()
            )
        )
    }

    private fun startAdminServer() {
        val managedExchange = exchange as? ExchangeSimulator ?: return
        if (adminPort <= 0) return
        val server = HttpServer.create(InetSocketAddress(adminBindHost, adminPort), 0)
        server.createContext("/health") { exchange ->
            writeJson(exchange, 200, mapOf("status" to "UP", "service" to "tcp-exchange-sim"))
        }
        server.createContext("/admin/status") { exchange ->
            if (exchange.requestMethod.equals("GET", ignoreCase = true)) {
                writeJson(exchange, 200, managedExchange.snapshot())
            } else {
                writeJson(exchange, 405, mapOf("error" to "method_not_allowed"))
            }
        }
        server.createContext("/admin/control") { exchange ->
            if (!exchange.requestMethod.equals("POST", ignoreCase = true)) {
                writeJson(exchange, 405, mapOf("error" to "method_not_allowed"))
                return@createContext
            }
            val request = readJson(exchange.requestBody, ExchangeSimulatorControlRequest::class.java)
                ?: ExchangeSimulatorControlRequest()
            writeJson(exchange, 200, managedExchange.applyControl(request))
        }
        server.createContext("/admin/reset") { exchange ->
            if (!exchange.requestMethod.equals("POST", ignoreCase = true)) {
                writeJson(exchange, 405, mapOf("error" to "method_not_allowed"))
                return@createContext
            }
            writeJson(exchange, 200, managedExchange.resetControl())
        }
        server.executor = clientPool
        server.start()
        adminServer = server
        println("TCP Exchange Simulator admin listening on $adminBindHost:$adminPort")
    }

    private fun writeJson(exchange: HttpExchange, statusCode: Int, body: Any) {
        val payload = mapper.writeValueAsBytes(body)
        exchange.responseHeaders.add("Content-Type", "application/json")
        exchange.sendResponseHeaders(statusCode, payload.size.toLong())
        exchange.responseBody.use { output ->
            output.write(payload)
            output.flush()
        }
        exchange.close()
    }

    private fun <T> readJson(input: InputStream, type: Class<T>): T? {
        return runCatching { mapper.readValue(input, type) }.getOrNull()
    }
}
