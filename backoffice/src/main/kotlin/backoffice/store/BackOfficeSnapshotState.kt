package backoffice.store

data class BackOfficeSnapshotState(
    val orderMeta: List<OrderMeta>,
    val lastFilledTotals: Map<String, Long>,
    val positions: List<Position>,
    val balances: List<Balance>,
    val realizedPnl: List<RealizedPnl>,
    val fillsByAccount: Map<String, List<FillRecord>>
)
