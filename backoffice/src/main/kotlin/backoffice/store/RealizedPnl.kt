package backoffice.store

data class RealizedPnl(
    val accountId: String,
    val symbol: String,
    val quoteCcy: String,
    val realizedPnl: Long
)

