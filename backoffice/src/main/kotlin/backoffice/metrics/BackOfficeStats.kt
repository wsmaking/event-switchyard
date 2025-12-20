package backoffice.metrics

import backoffice.ledger.FileLedger
import backoffice.model.BusEvent
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class BackOfficeStats {
    private val startedAt = Instant.now()
    private val processed = AtomicLong(0)
    private val skipped = AtomicLong(0)
    private val lastEventAt = AtomicReference<Instant?>(null)
    private val lastKafkaOffset = AtomicReference<String?>(null)
    private val replayStats = AtomicReference<FileLedger.ReplayStats?>(null)

    fun setReplayStats(stats: FileLedger.ReplayStats) {
        replayStats.set(stats)
    }

    fun onProcessed(event: BusEvent, offsetInfo: String) {
        processed.incrementAndGet()
        lastEventAt.set(event.at)
        lastKafkaOffset.set(offsetInfo)
    }

    fun onSkipped(offsetInfo: String) {
        skipped.incrementAndGet()
        lastKafkaOffset.set(offsetInfo)
    }

    fun snapshot(): Map<String, Any?> {
        return mapOf(
            "startedAt" to startedAt,
            "processed" to processed.get(),
            "skipped" to skipped.get(),
            "lastEventAt" to lastEventAt.get(),
            "lastKafkaOffset" to lastKafkaOffset.get(),
            "ledgerReplay" to replayStats.get()
        )
    }
}
