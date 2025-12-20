package backoffice

import backoffice.http.HttpBackOffice
import backoffice.kafka.BackOfficeConsumer
import backoffice.ledger.FileLedger
import backoffice.ledger.LedgerFill
import backoffice.ledger.LedgerOrderAccepted
import backoffice.ledger.LedgerReconciler
import backoffice.metrics.BackOfficeStats
import backoffice.auth.JwtAuth
import backoffice.store.InMemoryBackOfficeStore
import backoffice.store.OrderMeta
import java.nio.file.Path

fun main() {
    val port = (System.getenv("BACKOFFICE_PORT") ?: "8082").toInt()
    val ledgerPath = System.getenv("BACKOFFICE_LEDGER_PATH") ?: "var/backoffice/ledger.log"

    val store = InMemoryBackOfficeStore()
    val ledgerFile = FileLedger(Path.of(ledgerPath))
    val stats = BackOfficeStats()
    val replayStats =
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
    if (replayStats.lines > 0) {
        println("Ledger replay: $replayStats (path=$ledgerPath)")
    }
    stats.setReplayStats(replayStats)

    val reconciler = LedgerReconciler(ledger = ledgerFile, store = store)
    val consumer = BackOfficeConsumer(store = store, ledger = ledgerFile, stats = stats)
    val jwtAuth = JwtAuth()
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
        ledgerFile.close()
    })

    consumer.start()
    server.start()
}
