package backoffice.store

data class Balance(
    val accountId: String,
    val currency: String,
    val amount: Long
)

