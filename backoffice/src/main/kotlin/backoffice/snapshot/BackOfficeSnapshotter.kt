package backoffice.snapshot

import backoffice.ledger.FileLedger
import backoffice.store.InMemoryBackOfficeStore
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class BackOfficeSnapshotter(
    private val store: InMemoryBackOfficeStore,
    private val ledger: FileLedger,
    private val snapshotStore: BackOfficeSnapshotStore,
    intervalSec: Long = (System.getenv("BACKOFFICE_SNAPSHOT_SEC") ?: "30").toLong(),
    private val enabled: Boolean =
        (System.getenv("BACKOFFICE_SNAPSHOT_ENABLE") ?: "1").let { it == "1" || it.equals("true", ignoreCase = true) }
) : AutoCloseable {
    private val scheduler = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "backoffice-snapshot").apply { isDaemon = true }
    }
    private val intervalSec = intervalSec.coerceAtLeast(5)

    fun start() {
        if (!enabled) {
            println("BackOfficeSnapshotter disabled (BACKOFFICE_SNAPSHOT_ENABLE=0)")
            return
        }
        scheduler.scheduleAtFixedRate(
            ::snapshot,
            intervalSec,
            intervalSec,
            TimeUnit.SECONDS
        )
        println("BackOfficeSnapshotter enabled (intervalSec=$intervalSec)")
    }

    private fun snapshot() {
        val state = store.snapshotState()
        val lines = ledger.currentLineCount()
        snapshotStore.save(
            BackOfficeSnapshot(
                createdAt = Instant.now(),
                ledgerLines = lines,
                state = state
            )
        )
    }

    override fun close() {
        scheduler.shutdown()
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow()
            }
        } catch (_: InterruptedException) {
            scheduler.shutdownNow()
            Thread.currentThread().interrupt()
        }
    }
}
