package backoffice

import backoffice.http.HttpBackOffice
import backoffice.kafka.BackOfficeConsumer
import backoffice.ledger.FileLedger
import backoffice.ledger.LedgerFill
import backoffice.ledger.LedgerOrderAccepted
import backoffice.ledger.LedgerReconciler
import backoffice.metrics.BackOfficeStats
import backoffice.auth.JwtAuth
import backoffice.snapshot.BackOfficeSnapshotStore
import backoffice.snapshot.BackOfficeSnapshotter
import backoffice.store.BackOfficeStore
import backoffice.store.InMemoryBackOfficeStore
import backoffice.store.OrderMeta
import backoffice.store.PostgresBackOfficeStore
import java.nio.file.Path

fun main() {
    val port = (System.getenv("BACKOFFICE_PORT") ?: "8082").toInt()
    val ledgerPath = System.getenv("BACKOFFICE_LEDGER_PATH") ?: "var/backoffice/ledger.log"
    val snapshotPath = System.getenv("BACKOFFICE_SNAPSHOT_PATH") ?: "var/backoffice/snapshot.json"

    val dbUrl = System.getenv("BACKOFFICE_DB_URL")?.takeIf { it.isNotBlank() }
    val dbEnabled =
        (System.getenv("BACKOFFICE_DB_ENABLE") ?: "0").let { it == "1" || it.equals("true", ignoreCase = true) } ||
            dbUrl != null
    val store: BackOfficeStore = if (dbEnabled) PostgresBackOfficeStore() else InMemoryBackOfficeStore()
    val ledgerFile = FileLedger(Path.of(ledgerPath))
    val stats = BackOfficeStats()
    val snapshotStore = BackOfficeSnapshotStore(Path.of(snapshotPath))
    val snapshot = if (store is InMemoryBackOfficeStore) snapshotStore.load() else null

    val replayStats =
        if (snapshot != null && snapshot.ledgerLines <= ledgerFile.currentLineCount()) {
            store.restoreState(snapshot.state)
            ledgerFile.replayFromLine(snapshot.ledgerLines) { entry ->
                when (entry) {
                    is LedgerOrderAccepted -> {
                        store.upsertOrderMeta(
                            OrderMeta(
                                accountId = entry.accountId,
                                orderId = entry.orderId,
                                symbol = entry.symbol,
                                side = entry.side
                            )
                        )
                    }
                    is LedgerFill -> {
                        store.applyFill(
                            at = entry.at,
                            accountId = entry.accountId,
                            orderId = entry.orderId,
                            symbol = entry.symbol,
                            side = entry.side,
                            filledQtyDelta = entry.filledQtyDelta,
                            filledQtyTotal = entry.filledQtyTotal,
                            price = entry.price,
                            quoteCcy = entry.quoteCcy,
                            quoteCashDelta = entry.quoteCashDelta,
                            feeQuote = entry.feeQuote
                        )
                    }
                }
            }
        } else {
            ledgerFile.replay { entry ->
                when (entry) {
                    is LedgerOrderAccepted -> {
                        store.upsertOrderMeta(
                            OrderMeta(
                                accountId = entry.accountId,
                                orderId = entry.orderId,
                                symbol = entry.symbol,
                                side = entry.side
                            )
                        )
                    }
                    is LedgerFill -> {
                        store.applyFill(
                            at = entry.at,
                            accountId = entry.accountId,
                            orderId = entry.orderId,
                            symbol = entry.symbol,
                            side = entry.side,
                            filledQtyDelta = entry.filledQtyDelta,
                            filledQtyTotal = entry.filledQtyTotal,
                            price = entry.price,
                            quoteCcy = entry.quoteCcy,
                            quoteCashDelta = entry.quoteCashDelta,
                            feeQuote = entry.feeQuote
                        )
                    }
                }
            }
        }
    if (replayStats.lines > 0) {
        println("Ledger replay: $replayStats (path=$ledgerPath)")
    }
    stats.setReplayStats(replayStats)

    val reconciler = LedgerReconciler(ledger = ledgerFile, store = store)
    val consumer = BackOfficeConsumer(store = store, ledger = ledgerFile, stats = stats)
    val jwtAuth = JwtAuth()
    val snapshotter =
        if (store is InMemoryBackOfficeStore) {
            BackOfficeSnapshotter(store = store, ledger = ledgerFile, snapshotStore = snapshotStore)
        } else {
            null
        }
    val server =
        HttpBackOffice(
            port = port,
            store = store,
            ledger = ledgerFile,
            stats = stats,
            reconciler = reconciler,
            jwtAuth = jwtAuth
        )

    Runtime.getRuntime().addShutdownHook(Thread {
        server.close()
        consumer.close()
        snapshotter?.close()
        ledgerFile.close()
        store.close()
    })

    consumer.start()
    snapshotter?.start()
    server.start()
}
