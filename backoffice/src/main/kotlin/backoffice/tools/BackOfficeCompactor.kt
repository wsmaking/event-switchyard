package backoffice.tools

import backoffice.ledger.FileLedger
import backoffice.snapshot.BackOfficeSnapshotStore
import java.nio.file.Path

fun main() {
    val ledgerPath = System.getenv("BACKOFFICE_LEDGER_PATH") ?: "var/backoffice/ledger.log"
    val snapshotPath = System.getenv("BACKOFFICE_SNAPSHOT_PATH") ?: "var/backoffice/snapshot.json"
    val outputPath = System.getenv("BACKOFFICE_COMPACT_OUT") ?: "var/backoffice/ledger.compact.log"

    val snapshotStore = BackOfficeSnapshotStore(Path.of(snapshotPath))
    val snapshot = snapshotStore.load()
    if (snapshot == null) {
        println("Snapshot not found: $snapshotPath")
        return
    }

    val ledger = FileLedger(Path.of(ledgerPath))
    val result = ledger.compactFromLine(snapshot.ledgerLines, Path.of(outputPath))
    ledger.close()
    println("Compaction done: $result -> $outputPath")
}
