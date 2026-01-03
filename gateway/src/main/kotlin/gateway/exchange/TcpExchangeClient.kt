package gateway.exchange

import gateway.json.Json
import gateway.order.OrderSnapshot
import gateway.order.OrderStatus
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class TcpExchangeClient(
    private val host: String,
    private val port: Int,
    private val connectTimeoutMs: Int = 1500
) : ExchangeClient {
    private val mapper = Json.mapper
    private val running = AtomicBoolean(true)
    private val socket = Socket()
    private val callbacks = ConcurrentHashMap<String, (ExecutionReport) -> Unit>()
    private val writerLock = Any()
    private val readerThread: Thread
    private val writer: BufferedWriter

    init {
        socket.tcpNoDelay = true
        socket.connect(InetSocketAddress(host, port), connectTimeoutMs)
        writer = BufferedWriter(OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8))
        readerThread =
            thread(name = "tcp-exchange-client", isDaemon = false, start = true) {
                readLoop()
            }
    }

    override fun sendNewOrder(order: OrderSnapshot, onReport: (ExecutionReport) -> Unit) {
        callbacks[order.orderId] = onReport
        val request =
            TcpExchangeRequest(
                type = TcpExchangeRequestType.NEW,
                orderId = order.orderId,
                symbol = order.symbol,
                side = order.side,
                qty = order.qty,
                price = order.price
            )
        send(request)
    }

    override fun sendCancel(orderId: String, onReport: (ExecutionReport) -> Unit) {
        callbacks[orderId] = onReport
        val request =
            TcpExchangeRequest(
                type = TcpExchangeRequestType.CANCEL,
                orderId = orderId
            )
        send(request)
    }

    override fun close() {
        running.set(false)
        try {
            socket.close()
        } catch (_: Throwable) {
        }
    }

    private fun send(request: TcpExchangeRequest) {
        val line = mapper.writeValueAsString(request)
        synchronized(writerLock) {
            writer.write(line)
            writer.newLine()
            writer.flush()
        }
    }

    private fun readLoop() {
        val reader = BufferedReader(InputStreamReader(socket.getInputStream(), Charsets.UTF_8))
        while (running.get()) {
            val line = reader.readLine() ?: break
            val report =
                try {
                    mapper.readValue(line, ExecutionReport::class.java)
                } catch (_: Throwable) {
                    continue
                }
            val callback = callbacks[report.orderId]
            if (callback != null) {
                callback(report)
                if (report.status.isTerminal()) {
                    callbacks.remove(report.orderId)
                }
            }
        }
    }

    private fun OrderStatus.isTerminal(): Boolean {
        return this == OrderStatus.FILLED ||
            this == OrderStatus.CANCELED ||
            this == OrderStatus.REJECTED
    }
}
