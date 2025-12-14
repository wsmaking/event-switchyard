package backoffice.store

data class Position(
    val accountId: String,
    val symbol: String,
    val netQty: Long,
    val avgPrice: Double?
)

