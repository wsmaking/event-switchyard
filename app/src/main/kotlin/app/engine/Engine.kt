package app.engine

import app.ownership.Ownership
import app.log.LogQueue
import app.cdc.CdcQueue
import java.util.concurrent.atomic.AtomicLong

class Engine(
    private val ownership: Ownership,
    private val logQ: LogQueue,
    private val cdcQ: CdcQueue
) {
    private val seq = AtomicLong(0)

    fun handle(key: String, payload: ByteArray): Boolean {
        if (!ownership.owns(key)) return false
        logQ.offer(LogRecord(key, payload))
        cdcQ.offer(CdcEvent.Ingress(key, payload, seq.incrementAndGet()))
        return true
    }
}

data class LogRecord(val key: String, val payload: ByteArray)

sealed interface CdcEvent {
    data class Ingress(val key: String, val payload: ByteArray, val seq: Long) : CdcEvent
}
