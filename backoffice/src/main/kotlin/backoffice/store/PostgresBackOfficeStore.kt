package backoffice.store

import backoffice.model.Side
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.Timestamp
import java.time.Instant

class PostgresBackOfficeStore(
    jdbcUrl: String = System.getenv("BACKOFFICE_DB_URL") ?: "jdbc:postgresql://localhost:5432/backoffice",
    user: String = System.getenv("BACKOFFICE_DB_USER") ?: "backoffice",
    password: String = System.getenv("BACKOFFICE_DB_PASSWORD") ?: "backoffice",
    maxPoolSize: Int = (System.getenv("BACKOFFICE_DB_POOL") ?: "4").toInt(),
    private val maxFills: Int = (System.getenv("BACKOFFICE_MAX_FILLS") ?: "10000").toInt()
) : BackOfficeStore {
    private val dataSource: HikariDataSource

    init {
        val config =
            HikariConfig().apply {
                this.jdbcUrl = jdbcUrl
                username = user
                this.password = password
                maximumPoolSize = maxPoolSize.coerceAtLeast(1)
                minimumIdle = 1
                connectionTimeout = 5_000
                poolName = "backoffice-db"
            }
        dataSource = HikariDataSource(config)
        initSchema()
    }

    override fun upsertOrderMeta(meta: OrderMeta) {
        dataSource.connection.use { conn ->
            upsertOrderMeta(conn, meta)
        }
    }

    override fun findOrderMeta(orderId: String): OrderMeta? {
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                "SELECT account_id, symbol, side FROM order_meta WHERE order_id = ?"
            ).use { ps ->
                ps.setString(1, orderId)
                ps.executeQuery().use { rs ->
                    if (!rs.next()) return null
                    val accountId = rs.getString("account_id")
                    val symbol = rs.getString("symbol")
                    val side = Side.valueOf(rs.getString("side"))
                    return OrderMeta(accountId = accountId, orderId = orderId, symbol = symbol, side = side)
                }
            }
        }
    }

    override fun lastFilledTotal(orderId: String): Long {
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                "SELECT filled_qty_total FROM order_filled_total WHERE order_id = ?"
            ).use { ps ->
                ps.setString(1, orderId)
                ps.executeQuery().use { rs ->
                    return if (rs.next()) rs.getLong("filled_qty_total") else 0L
                }
            }
        }
    }

    override fun setLastFilledTotal(orderId: String, filledQtyTotal: Long) {
        dataSource.connection.use { conn ->
            upsertLastFilledTotal(conn, orderId, filledQtyTotal)
        }
    }

    override fun applyFill(
        at: Instant,
        accountId: String,
        orderId: String,
        symbol: String,
        side: Side,
        filledQtyDelta: Long,
        filledQtyTotal: Long,
        price: Long?,
        quoteCcy: String,
        quoteCashDelta: Long,
        feeQuote: Long
    ) {
        if (filledQtyDelta <= 0L) return
        dataSource.connection.use { conn ->
            conn.autoCommit = false
            try {
                val lastTotal = selectLastFilledTotal(conn, orderId, forUpdate = true)
                if (filledQtyTotal <= lastTotal) {
                    conn.rollback()
                    return
                }

                val signedQtyDelta = if (side == Side.BUY) filledQtyDelta else -filledQtyDelta
                val currentPos = selectPosition(conn, accountId, symbol, forUpdate = true)
                val update = PositionMath.applyDelta(currentPos, signedQtyDelta, price?.toDouble())

                upsertPosition(conn, accountId, symbol, update.netQty, update.avgPrice)
                if (update.realizedDelta != 0L) {
                    mergeRealizedPnl(conn, accountId, symbol, quoteCcy, update.realizedDelta)
                }
                mergeBalance(conn, accountId, quoteCcy, quoteCashDelta)
                upsertLastFilledTotal(conn, orderId, filledQtyTotal)
                insertFill(
                    conn = conn,
                    at = at,
                    accountId = accountId,
                    orderId = orderId,
                    symbol = symbol,
                    side = side,
                    filledQtyDelta = filledQtyDelta,
                    filledQtyTotal = filledQtyTotal,
                    price = price,
                    quoteCcy = quoteCcy,
                    quoteCashDelta = quoteCashDelta,
                    feeQuote = feeQuote
                )
                conn.commit()
            } catch (ex: Throwable) {
                conn.rollback()
                throw ex
            } finally {
                conn.autoCommit = true
            }
        }
    }

    override fun listPositions(accountId: String?): List<Position> {
        val rows = ArrayList<Position>()
        dataSource.connection.use { conn ->
            val (sql, binder) =
                if (accountId == null) {
                    "SELECT account_id, symbol, net_qty, avg_price FROM positions ORDER BY account_id, symbol" to null
                } else {
                    "SELECT account_id, symbol, net_qty, avg_price FROM positions WHERE account_id = ? ORDER BY account_id, symbol" to accountId
                }
            conn.prepareStatement(sql).use { ps ->
                if (binder != null) ps.setString(1, binder)
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        rows.add(
                            Position(
                                accountId = rs.getString("account_id"),
                                symbol = rs.getString("symbol"),
                                netQty = rs.getLong("net_qty"),
                                avgPrice = rs.getDouble("avg_price").let { if (rs.wasNull()) null else it }
                            )
                        )
                    }
                }
            }
        }
        return rows
    }

    override fun listBalances(accountId: String?): List<Balance> {
        val rows = ArrayList<Balance>()
        dataSource.connection.use { conn ->
            val (sql, binder) =
                if (accountId == null) {
                    "SELECT account_id, currency, amount FROM balances ORDER BY account_id, currency" to null
                } else {
                    "SELECT account_id, currency, amount FROM balances WHERE account_id = ? ORDER BY account_id, currency" to accountId
                }
            conn.prepareStatement(sql).use { ps ->
                if (binder != null) ps.setString(1, binder)
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        rows.add(
                            Balance(
                                accountId = rs.getString("account_id"),
                                currency = rs.getString("currency"),
                                amount = rs.getLong("amount")
                            )
                        )
                    }
                }
            }
        }
        return rows
    }

    override fun listFills(accountId: String): List<FillRecord> {
        val rows = ArrayList<FillRecord>()
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                """
                SELECT at, account_id, order_id, symbol, side, filled_qty_delta, filled_qty_total, price,
                       quote_ccy, quote_cash_delta, fee_quote
                FROM fills
                WHERE account_id = ?
                ORDER BY id DESC
                LIMIT ?
                """.trimIndent()
            ).use { ps ->
                ps.setString(1, accountId)
                ps.setInt(2, maxFills)
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        rows.add(
                            FillRecord(
                                at = rs.getTimestamp("at").toInstant(),
                                accountId = rs.getString("account_id"),
                                orderId = rs.getString("order_id"),
                                symbol = rs.getString("symbol"),
                                side = Side.valueOf(rs.getString("side")),
                                filledQtyDelta = rs.getLong("filled_qty_delta"),
                                filledQtyTotal = rs.getLong("filled_qty_total"),
                                price = rs.getLong("price").let { if (rs.wasNull()) null else it },
                                quoteCcy = rs.getString("quote_ccy"),
                                quoteCashDelta = rs.getLong("quote_cash_delta"),
                                feeQuote = rs.getLong("fee_quote")
                            )
                        )
                    }
                }
            }
        }
        rows.reverse()
        return rows
    }

    override fun listRealizedPnl(accountId: String?): List<RealizedPnl> {
        val rows = ArrayList<RealizedPnl>()
        dataSource.connection.use { conn ->
            val (sql, binder) =
                if (accountId == null) {
                    """
                    SELECT account_id, symbol, quote_ccy, realized_pnl
                    FROM realized_pnl
                    ORDER BY account_id, symbol, quote_ccy
                    """.trimIndent() to null
                } else {
                    """
                    SELECT account_id, symbol, quote_ccy, realized_pnl
                    FROM realized_pnl
                    WHERE account_id = ?
                    ORDER BY account_id, symbol, quote_ccy
                    """.trimIndent() to accountId
                }
            conn.prepareStatement(sql).use { ps ->
                if (binder != null) ps.setString(1, binder)
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        rows.add(
                            RealizedPnl(
                                accountId = rs.getString("account_id"),
                                symbol = rs.getString("symbol"),
                                quoteCcy = rs.getString("quote_ccy"),
                                realizedPnl = rs.getLong("realized_pnl")
                            )
                        )
                    }
                }
            }
        }
        return rows
    }

    override fun snapshotState(): BackOfficeSnapshotState {
        val orderMeta = ArrayList<OrderMeta>()
        val lastFilledTotals = HashMap<String, Long>()
        dataSource.connection.use { conn ->
            conn.prepareStatement("SELECT order_id, account_id, symbol, side FROM order_meta").use { ps ->
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        orderMeta.add(
                            OrderMeta(
                                accountId = rs.getString("account_id"),
                                orderId = rs.getString("order_id"),
                                symbol = rs.getString("symbol"),
                                side = Side.valueOf(rs.getString("side"))
                            )
                        )
                    }
                }
            }
            conn.prepareStatement("SELECT order_id, filled_qty_total FROM order_filled_total").use { ps ->
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        lastFilledTotals[rs.getString("order_id")] = rs.getLong("filled_qty_total")
                    }
                }
            }
        }

        val fillsByAccount = HashMap<String, List<FillRecord>>()
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                """
                SELECT at, account_id, order_id, symbol, side, filled_qty_delta, filled_qty_total, price,
                       quote_ccy, quote_cash_delta, fee_quote
                FROM fills
                ORDER BY id ASC
                """.trimIndent()
            ).use { ps ->
                ps.executeQuery().use { rs ->
                    val temp = HashMap<String, MutableList<FillRecord>>()
                    while (rs.next()) {
                        val record =
                            FillRecord(
                                at = rs.getTimestamp("at").toInstant(),
                                accountId = rs.getString("account_id"),
                                orderId = rs.getString("order_id"),
                                symbol = rs.getString("symbol"),
                                side = Side.valueOf(rs.getString("side")),
                                filledQtyDelta = rs.getLong("filled_qty_delta"),
                                filledQtyTotal = rs.getLong("filled_qty_total"),
                                price = rs.getLong("price").let { if (rs.wasNull()) null else it },
                                quoteCcy = rs.getString("quote_ccy"),
                                quoteCashDelta = rs.getLong("quote_cash_delta"),
                                feeQuote = rs.getLong("fee_quote")
                            )
                        temp.computeIfAbsent(record.accountId) { ArrayList() }.add(record)
                    }
                    for ((accountId, records) in temp) {
                        fillsByAccount[accountId] = records
                    }
                }
            }
        }

        return BackOfficeSnapshotState(
            orderMeta = orderMeta,
            lastFilledTotals = lastFilledTotals,
            positions = listPositions(),
            balances = listBalances(),
            realizedPnl = listRealizedPnl(),
            fillsByAccount = fillsByAccount
        )
    }

    override fun restoreState(state: BackOfficeSnapshotState) {
        dataSource.connection.use { conn ->
            conn.autoCommit = false
            try {
                conn.prepareStatement("TRUNCATE fills, positions, balances, realized_pnl, order_filled_total, order_meta").use { ps ->
                    ps.executeUpdate()
                }

                for (meta in state.orderMeta) {
                    upsertOrderMeta(conn, meta)
                }
                for ((orderId, total) in state.lastFilledTotals.entries) {
                    upsertLastFilledTotal(conn, orderId, total)
                }
                for (pos in state.positions) {
                    upsertPosition(conn, pos.accountId, pos.symbol, pos.netQty, pos.avgPrice)
                }
                for (bal in state.balances) {
                    mergeBalance(conn, bal.accountId, bal.currency, bal.amount)
                }
                for (pnl in state.realizedPnl) {
                    mergeRealizedPnl(conn, pnl.accountId, pnl.symbol, pnl.quoteCcy, pnl.realizedPnl)
                }
                for ((_, records) in state.fillsByAccount) {
                    for (record in records) {
                        insertFill(
                            conn = conn,
                            at = record.at,
                            accountId = record.accountId,
                            orderId = record.orderId,
                            symbol = record.symbol,
                            side = record.side,
                            filledQtyDelta = record.filledQtyDelta,
                            filledQtyTotal = record.filledQtyTotal,
                            price = record.price,
                            quoteCcy = record.quoteCcy,
                            quoteCashDelta = record.quoteCashDelta,
                            feeQuote = record.feeQuote
                        )
                    }
                }
                conn.commit()
            } catch (ex: Throwable) {
                conn.rollback()
                throw ex
            } finally {
                conn.autoCommit = true
            }
        }
    }

    override fun snapshotBalances(accountId: String): Map<String, Long> {
        val values = HashMap<String, Long>()
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                "SELECT currency, amount FROM balances WHERE account_id = ?"
            ).use { ps ->
                ps.setString(1, accountId)
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        values[rs.getString("currency")] = rs.getLong("amount")
                    }
                }
            }
        }
        return values
    }

    override fun snapshotPositions(accountId: String): Map<String, Long> {
        val values = HashMap<String, Long>()
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                "SELECT symbol, net_qty FROM positions WHERE account_id = ?"
            ).use { ps ->
                ps.setString(1, accountId)
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        values[rs.getString("symbol")] = rs.getLong("net_qty")
                    }
                }
            }
        }
        return values
    }

    override fun close() {
        dataSource.close()
    }

    private fun initSchema() {
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                """
                CREATE TABLE IF NOT EXISTS order_meta (
                    order_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL
                )
                """.trimIndent()
            ).use { ps -> ps.executeUpdate() }
            conn.prepareStatement(
                """
                CREATE TABLE IF NOT EXISTS order_filled_total (
                    order_id TEXT PRIMARY KEY,
                    filled_qty_total BIGINT NOT NULL
                )
                """.trimIndent()
            ).use { ps -> ps.executeUpdate() }
            conn.prepareStatement(
                """
                CREATE TABLE IF NOT EXISTS positions (
                    account_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    net_qty BIGINT NOT NULL,
                    avg_price DOUBLE PRECISION,
                    PRIMARY KEY (account_id, symbol)
                )
                """.trimIndent()
            ).use { ps -> ps.executeUpdate() }
            conn.prepareStatement(
                """
                CREATE TABLE IF NOT EXISTS balances (
                    account_id TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    amount BIGINT NOT NULL,
                    PRIMARY KEY (account_id, currency)
                )
                """.trimIndent()
            ).use { ps -> ps.executeUpdate() }
            conn.prepareStatement(
                """
                CREATE TABLE IF NOT EXISTS realized_pnl (
                    account_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    quote_ccy TEXT NOT NULL,
                    realized_pnl BIGINT NOT NULL,
                    PRIMARY KEY (account_id, symbol, quote_ccy)
                )
                """.trimIndent()
            ).use { ps -> ps.executeUpdate() }
            conn.prepareStatement(
                """
                CREATE TABLE IF NOT EXISTS fills (
                    id BIGSERIAL PRIMARY KEY,
                    at TIMESTAMPTZ NOT NULL,
                    account_id TEXT NOT NULL,
                    order_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    filled_qty_delta BIGINT NOT NULL,
                    filled_qty_total BIGINT NOT NULL,
                    price BIGINT,
                    quote_ccy TEXT NOT NULL,
                    quote_cash_delta BIGINT NOT NULL,
                    fee_quote BIGINT NOT NULL
                )
                """.trimIndent()
            ).use { ps -> ps.executeUpdate() }
            conn.prepareStatement(
                "CREATE INDEX IF NOT EXISTS fills_account_idx ON fills (account_id, id)"
            ).use { ps -> ps.executeUpdate() }
        }
    }

    private fun selectLastFilledTotal(conn: Connection, orderId: String, forUpdate: Boolean): Long {
        val sql =
            if (forUpdate) {
                "SELECT filled_qty_total FROM order_filled_total WHERE order_id = ? FOR UPDATE"
            } else {
                "SELECT filled_qty_total FROM order_filled_total WHERE order_id = ?"
            }
        conn.prepareStatement(sql).use { ps ->
            ps.setString(1, orderId)
            ps.executeQuery().use { rs ->
                return if (rs.next()) rs.getLong("filled_qty_total") else 0L
            }
        }
    }

    private fun upsertOrderMeta(conn: Connection, meta: OrderMeta) {
        conn.prepareStatement(
            """
            INSERT INTO order_meta (order_id, account_id, symbol, side)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (order_id) DO UPDATE
            SET account_id = EXCLUDED.account_id,
                symbol = EXCLUDED.symbol,
                side = EXCLUDED.side
            """.trimIndent()
        ).use { ps ->
            ps.setString(1, meta.orderId)
            ps.setString(2, meta.accountId)
            ps.setString(3, meta.symbol)
            ps.setString(4, meta.side.name)
            ps.executeUpdate()
        }
    }

    private fun upsertLastFilledTotal(conn: Connection, orderId: String, filledQtyTotal: Long) {
        conn.prepareStatement(
            """
            INSERT INTO order_filled_total (order_id, filled_qty_total)
            VALUES (?, ?)
            ON CONFLICT (order_id) DO UPDATE
            SET filled_qty_total = GREATEST(order_filled_total.filled_qty_total, EXCLUDED.filled_qty_total)
            """.trimIndent()
        ).use { ps ->
            ps.setString(1, orderId)
            ps.setLong(2, filledQtyTotal)
            ps.executeUpdate()
        }
    }

    private fun selectPosition(conn: Connection, accountId: String, symbol: String, forUpdate: Boolean): PositionState {
        val sql =
            if (forUpdate) {
                "SELECT net_qty, avg_price FROM positions WHERE account_id = ? AND symbol = ? FOR UPDATE"
            } else {
                "SELECT net_qty, avg_price FROM positions WHERE account_id = ? AND symbol = ?"
            }
        conn.prepareStatement(sql).use { ps ->
            ps.setString(1, accountId)
            ps.setString(2, symbol)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return PositionState(0, null)
                val avg = rs.getDouble("avg_price").let { if (rs.wasNull()) null else it }
                return PositionState(rs.getLong("net_qty"), avg)
            }
        }
    }

    private fun upsertPosition(conn: Connection, accountId: String, symbol: String, netQty: Long, avgPrice: Double?) {
        conn.prepareStatement(
            """
            INSERT INTO positions (account_id, symbol, net_qty, avg_price)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (account_id, symbol) DO UPDATE
            SET net_qty = EXCLUDED.net_qty,
                avg_price = EXCLUDED.avg_price
            """.trimIndent()
        ).use { ps ->
            ps.setString(1, accountId)
            ps.setString(2, symbol)
            ps.setLong(3, netQty)
            if (avgPrice == null) {
                ps.setNull(4, java.sql.Types.DOUBLE)
            } else {
                ps.setDouble(4, avgPrice)
            }
            ps.executeUpdate()
        }
    }

    private fun mergeBalance(conn: Connection, accountId: String, currency: String, delta: Long) {
        conn.prepareStatement(
            """
            INSERT INTO balances (account_id, currency, amount)
            VALUES (?, ?, ?)
            ON CONFLICT (account_id, currency) DO UPDATE
            SET amount = balances.amount + EXCLUDED.amount
            """.trimIndent()
        ).use { ps ->
            ps.setString(1, accountId)
            ps.setString(2, currency)
            ps.setLong(3, delta)
            ps.executeUpdate()
        }
    }

    private fun mergeRealizedPnl(conn: Connection, accountId: String, symbol: String, quoteCcy: String, delta: Long) {
        conn.prepareStatement(
            """
            INSERT INTO realized_pnl (account_id, symbol, quote_ccy, realized_pnl)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (account_id, symbol, quote_ccy) DO UPDATE
            SET realized_pnl = realized_pnl.realized_pnl + EXCLUDED.realized_pnl
            """.trimIndent()
        ).use { ps ->
            ps.setString(1, accountId)
            ps.setString(2, symbol)
            ps.setString(3, quoteCcy)
            ps.setLong(4, delta)
            ps.executeUpdate()
        }
    }

    private fun insertFill(
        conn: Connection,
        at: Instant,
        accountId: String,
        orderId: String,
        symbol: String,
        side: Side,
        filledQtyDelta: Long,
        filledQtyTotal: Long,
        price: Long?,
        quoteCcy: String,
        quoteCashDelta: Long,
        feeQuote: Long
    ) {
        conn.prepareStatement(
            """
            INSERT INTO fills (
                at, account_id, order_id, symbol, side, filled_qty_delta, filled_qty_total, price,
                quote_ccy, quote_cash_delta, fee_quote
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent()
        ).use { ps ->
            ps.setTimestamp(1, Timestamp.from(at))
            ps.setString(2, accountId)
            ps.setString(3, orderId)
            ps.setString(4, symbol)
            ps.setString(5, side.name)
            ps.setLong(6, filledQtyDelta)
            ps.setLong(7, filledQtyTotal)
            if (price == null) {
                ps.setNull(8, java.sql.Types.BIGINT)
            } else {
                ps.setLong(8, price)
            }
            ps.setString(9, quoteCcy)
            ps.setLong(10, quoteCashDelta)
            ps.setLong(11, feeQuote)
            ps.executeUpdate()
        }
    }

    private data class PositionState(
        val netQty: Long,
        val avgPrice: Double?
    )

    private data class PositionUpdate(
        val netQty: Long,
        val avgPrice: Double?,
        val realizedDelta: Long
    )

    private object PositionMath {
        fun applyDelta(current: PositionState, signedQtyDelta: Long, price: Double?): PositionUpdate {
            if (signedQtyDelta == 0L) {
                return PositionUpdate(current.netQty, current.avgPrice, 0L)
            }
            if (price == null) {
                return PositionUpdate(current.netQty + signedQtyDelta, current.avgPrice, 0L)
            }

            val beforeQty = current.netQty
            val beforeAvg = current.avgPrice
            val realized = realizedFromClose(beforeQty, signedQtyDelta, beforeAvg, price)

            val newQty = beforeQty + signedQtyDelta
            val sameDirection =
                beforeQty == 0L || (beforeQty > 0 && signedQtyDelta > 0) || (beforeQty < 0 && signedQtyDelta < 0)

            val newAvg =
                if (sameDirection) {
                    if (beforeAvg == null) {
                        price
                    } else {
                        val oldAbs = kotlin.math.abs(beforeQty).toDouble()
                        val addAbs = kotlin.math.abs(signedQtyDelta).toDouble()
                        ((beforeAvg * oldAbs) + (price * addAbs)) / (oldAbs + addAbs)
                    }
                } else if (beforeQty != 0L && newQty == 0L) {
                    null
                } else if (beforeQty != 0L && (beforeQty > 0 && newQty < 0 || beforeQty < 0 && newQty > 0)) {
                    price
                } else {
                    beforeAvg
                }

            return PositionUpdate(newQty, newAvg, realized)
        }

        private fun realizedFromClose(beforeQty: Long, signedQtyDelta: Long, beforeAvg: Double?, px: Double): Long {
            if (beforeAvg == null) return 0L

            if (beforeQty > 0 && signedQtyDelta < 0) {
                val closeQty = minOf(beforeQty, kotlin.math.abs(signedQtyDelta))
                val pnl = (px - beforeAvg) * closeQty.toDouble()
                return pnl.toLong()
            }
            if (beforeQty < 0 && signedQtyDelta > 0) {
                val closeQty = minOf(kotlin.math.abs(beforeQty), signedQtyDelta)
                val pnl = (beforeAvg - px) * closeQty.toDouble()
                return pnl.toLong()
            }
            return 0L
        }
    }
}
