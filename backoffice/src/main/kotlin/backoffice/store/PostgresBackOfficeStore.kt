package backoffice.store

import backoffice.jooq.Tables.BALANCES
import backoffice.jooq.Tables.FILLS
import backoffice.jooq.Tables.ORDER_FILLED_TOTAL
import backoffice.jooq.Tables.ORDER_META
import backoffice.jooq.Tables.POSITIONS
import backoffice.jooq.Tables.REALIZED_PNL
import backoffice.model.Side
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.SQLDialect
import org.jooq.conf.RenderQuotedNames
import org.jooq.conf.Settings
import org.jooq.impl.DSL
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

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
    }

    override fun upsertOrderMeta(meta: OrderMeta) {
        withDsl { ctx ->
            upsertOrderMeta(ctx, meta)
        }
    }

    override fun findOrderMeta(orderId: String): OrderMeta? {
        return withDsl { ctx ->
            val record =
                ctx.selectFrom(ORDER_META)
                    .where(ORDER_META.ORDER_ID.eq(orderId))
                    .fetchOne()
                    ?: return@withDsl null
            mapOrderMeta(record)
        }
    }

    override fun lastFilledTotal(orderId: String): Long {
        return withDsl { ctx ->
            selectLastFilledTotal(ctx, orderId, forUpdate = false)
        }
    }

    override fun setLastFilledTotal(orderId: String, filledQtyTotal: Long) {
        withDsl { ctx ->
            upsertLastFilledTotal(ctx, orderId, filledQtyTotal)
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
            val ctx = DSL.using(conn, SQLDialect.POSTGRES)
            try {
                val lastTotal = selectLastFilledTotal(ctx, orderId, forUpdate = true)
                if (filledQtyTotal <= lastTotal) {
                    conn.rollback()
                    return
                }

                val signedQtyDelta = if (side == Side.BUY) filledQtyDelta else -filledQtyDelta
                val currentPos = selectPosition(ctx, accountId, symbol, forUpdate = true)
                val update = PositionMath.applyDelta(currentPos, signedQtyDelta, price?.toDouble())

                upsertPosition(ctx, accountId, symbol, update.netQty, update.avgPrice)
                if (update.realizedDelta != 0L) {
                    mergeRealizedPnl(ctx, accountId, symbol, quoteCcy, update.realizedDelta)
                }
                mergeBalance(ctx, accountId, quoteCcy, quoteCashDelta)
                upsertLastFilledTotal(ctx, orderId, filledQtyTotal)
                insertFill(
                    ctx = ctx,
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
        return withDsl { ctx ->
            val query = ctx.selectFrom(POSITIONS)
            val filtered = if (accountId == null) query else query.where(POSITIONS.ACCOUNT_ID.eq(accountId))
            filtered
                .orderBy(POSITIONS.ACCOUNT_ID, POSITIONS.SYMBOL)
                .fetch()
                .map { record ->
                    Position(
                        accountId = record.get(POSITIONS.ACCOUNT_ID),
                        symbol = record.get(POSITIONS.SYMBOL),
                        netQty = record.get(POSITIONS.NET_QTY),
                        avgPrice = record.get(POSITIONS.AVG_PRICE)
                    )
                }
        }
    }

    override fun listBalances(accountId: String?): List<Balance> {
        return withDsl { ctx ->
            val query = ctx.selectFrom(BALANCES)
            val filtered = if (accountId == null) query else query.where(BALANCES.ACCOUNT_ID.eq(accountId))
            filtered
                .orderBy(BALANCES.ACCOUNT_ID, BALANCES.CURRENCY)
                .fetch()
                .map { record ->
                    Balance(
                        accountId = record.get(BALANCES.ACCOUNT_ID),
                        currency = record.get(BALANCES.CURRENCY),
                        amount = record.get(BALANCES.AMOUNT)
                    )
                }
        }
    }

    override fun listFills(accountId: String): List<FillRecord> {
        return withDsl { ctx ->
            val rows =
                ctx.selectFrom(FILLS)
                    .where(FILLS.ACCOUNT_ID.eq(accountId))
                    .orderBy(FILLS.ID.desc())
                    .limit(maxFills)
                    .fetch()
                    .map { record ->
                        FillRecord(
                            at = record.get(FILLS.AT).toInstant(),
                            accountId = record.get(FILLS.ACCOUNT_ID),
                            orderId = record.get(FILLS.ORDER_ID),
                            symbol = record.get(FILLS.SYMBOL),
                            side = Side.valueOf(record.get(FILLS.SIDE)),
                            filledQtyDelta = record.get(FILLS.FILLED_QTY_DELTA),
                            filledQtyTotal = record.get(FILLS.FILLED_QTY_TOTAL),
                            price = record.get(FILLS.PRICE),
                            quoteCcy = record.get(FILLS.QUOTE_CCY),
                            quoteCashDelta = record.get(FILLS.QUOTE_CASH_DELTA),
                            feeQuote = record.get(FILLS.FEE_QUOTE)
                        )
                    }
                    .toMutableList()
            rows.reverse()
            rows
        }
    }

    override fun listRealizedPnl(accountId: String?): List<RealizedPnl> {
        return withDsl { ctx ->
            val query = ctx.selectFrom(REALIZED_PNL)
            val filtered = if (accountId == null) query else query.where(REALIZED_PNL.ACCOUNT_ID.eq(accountId))
            filtered
                .orderBy(REALIZED_PNL.ACCOUNT_ID, REALIZED_PNL.SYMBOL, REALIZED_PNL.QUOTE_CCY)
                .fetch()
                .map { record ->
                    RealizedPnl(
                        accountId = record.get(REALIZED_PNL.ACCOUNT_ID),
                        symbol = record.get(REALIZED_PNL.SYMBOL),
                        quoteCcy = record.get(REALIZED_PNL.QUOTE_CCY),
                        realizedPnl = record.get(REALIZED_PNL.REALIZED_PNL_)
                    )
                }
        }
    }

    override fun snapshotState(): BackOfficeSnapshotState {
        return withDsl { ctx ->
            val orderMeta =
                ctx.selectFrom(ORDER_META)
                    .fetch()
                    .map { record -> mapOrderMeta(record) }
            val lastFilledTotals =
                ctx.selectFrom(ORDER_FILLED_TOTAL)
                    .fetch()
                    .associate { record ->
                        record.get(ORDER_FILLED_TOTAL.ORDER_ID) to record.get(ORDER_FILLED_TOTAL.FILLED_QTY_TOTAL)
                    }
            val fillsByAccount =
                ctx.selectFrom(FILLS)
                    .orderBy(FILLS.ID.asc())
                    .fetch()
                    .groupBy({ record -> record.get(FILLS.ACCOUNT_ID) }) { record ->
                        FillRecord(
                            at = record.get(FILLS.AT).toInstant(),
                            accountId = record.get(FILLS.ACCOUNT_ID),
                            orderId = record.get(FILLS.ORDER_ID),
                            symbol = record.get(FILLS.SYMBOL),
                            side = Side.valueOf(record.get(FILLS.SIDE)),
                            filledQtyDelta = record.get(FILLS.FILLED_QTY_DELTA),
                            filledQtyTotal = record.get(FILLS.FILLED_QTY_TOTAL),
                            price = record.get(FILLS.PRICE),
                            quoteCcy = record.get(FILLS.QUOTE_CCY),
                            quoteCashDelta = record.get(FILLS.QUOTE_CASH_DELTA),
                            feeQuote = record.get(FILLS.FEE_QUOTE)
                        )
                    }
            BackOfficeSnapshotState(
                orderMeta = orderMeta,
                lastFilledTotals = lastFilledTotals,
                positions = listPositions(null),
                balances = listBalances(null),
                realizedPnl = listRealizedPnl(null),
                fillsByAccount = fillsByAccount
            )
        }
    }

    override fun restoreState(state: BackOfficeSnapshotState) {
        dataSource.connection.use { conn ->
            conn.autoCommit = false
            val ctx = DSL.using(conn, SQLDialect.POSTGRES)
            try {
                ctx.truncate(FILLS, POSITIONS, BALANCES, REALIZED_PNL, ORDER_FILLED_TOTAL, ORDER_META).execute()

                state.orderMeta.forEach { meta -> upsertOrderMeta(ctx, meta) }
                state.lastFilledTotals.forEach { (orderId, total) -> upsertLastFilledTotal(ctx, orderId, total) }
                state.positions.forEach { pos -> upsertPosition(ctx, pos.accountId, pos.symbol, pos.netQty, pos.avgPrice) }
                state.balances.forEach { bal -> mergeBalance(ctx, bal.accountId, bal.currency, bal.amount) }
                state.realizedPnl.forEach { pnl ->
                    mergeRealizedPnl(ctx, pnl.accountId, pnl.symbol, pnl.quoteCcy, pnl.realizedPnl)
                }
                state.fillsByAccount.values.flatten().forEach { record ->
                    insertFill(
                        ctx = ctx,
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
        return withDsl { ctx ->
            ctx.select(BALANCES.CURRENCY, BALANCES.AMOUNT)
                .from(BALANCES)
                .where(BALANCES.ACCOUNT_ID.eq(accountId))
                .fetch()
                .associate { record ->
                    record.get(BALANCES.CURRENCY) to record.get(BALANCES.AMOUNT)
                }
        }
    }

    override fun snapshotPositions(accountId: String): Map<String, Long> {
        return withDsl { ctx ->
            ctx.select(POSITIONS.SYMBOL, POSITIONS.NET_QTY)
                .from(POSITIONS)
                .where(POSITIONS.ACCOUNT_ID.eq(accountId))
                .fetch()
                .associate { record ->
                    record.get(POSITIONS.SYMBOL) to record.get(POSITIONS.NET_QTY)
                }
        }
    }

    override fun close() {
        dataSource.close()
    }

    private fun <T> withDsl(block: (DSLContext) -> T): T {
        dataSource.connection.use { conn ->
            val settings = Settings().withRenderQuotedNames(RenderQuotedNames.NEVER)
            val ctx = DSL.using(conn, SQLDialect.POSTGRES, settings)
            return block(ctx)
        }
    }

    private fun mapOrderMeta(record: Record): OrderMeta {
        return OrderMeta(
            accountId = record.get(ORDER_META.ACCOUNT_ID),
            orderId = record.get(ORDER_META.ORDER_ID),
            symbol = record.get(ORDER_META.SYMBOL),
            side = Side.valueOf(record.get(ORDER_META.SIDE))
        )
    }

    private fun selectLastFilledTotal(ctx: DSLContext, orderId: String, forUpdate: Boolean): Long {
        val query =
            ctx.select(ORDER_FILLED_TOTAL.FILLED_QTY_TOTAL)
                .from(ORDER_FILLED_TOTAL)
                .where(ORDER_FILLED_TOTAL.ORDER_ID.eq(orderId))
        val record = if (forUpdate) query.forUpdate().fetchOne() else query.fetchOne()
        return record?.get(ORDER_FILLED_TOTAL.FILLED_QTY_TOTAL) ?: 0L
    }

    private fun upsertOrderMeta(ctx: DSLContext, meta: OrderMeta) {
        ctx.insertInto(ORDER_META)
            .set(ORDER_META.ORDER_ID, meta.orderId)
            .set(ORDER_META.ACCOUNT_ID, meta.accountId)
            .set(ORDER_META.SYMBOL, meta.symbol)
            .set(ORDER_META.SIDE, meta.side.name)
            .onConflict(ORDER_META.ORDER_ID)
            .doUpdate()
            .set(ORDER_META.ACCOUNT_ID, meta.accountId)
            .set(ORDER_META.SYMBOL, meta.symbol)
            .set(ORDER_META.SIDE, meta.side.name)
            .execute()
    }

    private fun upsertLastFilledTotal(ctx: DSLContext, orderId: String, filledQtyTotal: Long) {
        ctx.insertInto(ORDER_FILLED_TOTAL)
            .set(ORDER_FILLED_TOTAL.ORDER_ID, orderId)
            .set(ORDER_FILLED_TOTAL.FILLED_QTY_TOTAL, filledQtyTotal)
            .onConflict(ORDER_FILLED_TOTAL.ORDER_ID)
            .doUpdate()
            .set(
                ORDER_FILLED_TOTAL.FILLED_QTY_TOTAL,
                DSL.greatest(ORDER_FILLED_TOTAL.FILLED_QTY_TOTAL, DSL.excluded(ORDER_FILLED_TOTAL.FILLED_QTY_TOTAL))
            )
            .execute()
    }

    private fun selectPosition(ctx: DSLContext, accountId: String, symbol: String, forUpdate: Boolean): PositionState {
        val query =
            ctx.select(POSITIONS.NET_QTY, POSITIONS.AVG_PRICE)
                .from(POSITIONS)
                .where(POSITIONS.ACCOUNT_ID.eq(accountId))
                .and(POSITIONS.SYMBOL.eq(symbol))
        val record = if (forUpdate) query.forUpdate().fetchOne() else query.fetchOne()
        if (record == null) return PositionState(0L, null)
        return PositionState(
            netQty = record.get(POSITIONS.NET_QTY),
            avgPrice = record.get(POSITIONS.AVG_PRICE)
        )
    }

    private fun upsertPosition(ctx: DSLContext, accountId: String, symbol: String, netQty: Long, avgPrice: Double?) {
        ctx.insertInto(POSITIONS)
            .set(POSITIONS.ACCOUNT_ID, accountId)
            .set(POSITIONS.SYMBOL, symbol)
            .set(POSITIONS.NET_QTY, netQty)
            .set(POSITIONS.AVG_PRICE, avgPrice)
            .onConflict(POSITIONS.ACCOUNT_ID, POSITIONS.SYMBOL)
            .doUpdate()
            .set(POSITIONS.NET_QTY, netQty)
            .set(POSITIONS.AVG_PRICE, avgPrice)
            .execute()
    }

    private fun mergeBalance(ctx: DSLContext, accountId: String, currency: String, delta: Long) {
        ctx.insertInto(BALANCES)
            .set(BALANCES.ACCOUNT_ID, accountId)
            .set(BALANCES.CURRENCY, currency)
            .set(BALANCES.AMOUNT, delta)
            .onConflict(BALANCES.ACCOUNT_ID, BALANCES.CURRENCY)
            .doUpdate()
            .set(BALANCES.AMOUNT, BALANCES.AMOUNT.add(DSL.excluded(BALANCES.AMOUNT)))
            .execute()
    }

    private fun mergeRealizedPnl(ctx: DSLContext, accountId: String, symbol: String, quoteCcy: String, delta: Long) {
        ctx.insertInto(REALIZED_PNL)
            .set(REALIZED_PNL.ACCOUNT_ID, accountId)
            .set(REALIZED_PNL.SYMBOL, symbol)
            .set(REALIZED_PNL.QUOTE_CCY, quoteCcy)
            .set(REALIZED_PNL.REALIZED_PNL_, delta)
            .onConflict(REALIZED_PNL.ACCOUNT_ID, REALIZED_PNL.SYMBOL, REALIZED_PNL.QUOTE_CCY)
            .doUpdate()
            .set(
                REALIZED_PNL.REALIZED_PNL_,
                REALIZED_PNL.REALIZED_PNL_.add(DSL.excluded(REALIZED_PNL.REALIZED_PNL_))
            )
            .execute()
    }

    private fun insertFill(
        ctx: DSLContext,
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
        ctx.insertInto(FILLS)
            .set(FILLS.AT, OffsetDateTime.ofInstant(at, ZoneOffset.UTC))
            .set(FILLS.ACCOUNT_ID, accountId)
            .set(FILLS.ORDER_ID, orderId)
            .set(FILLS.SYMBOL, symbol)
            .set(FILLS.SIDE, side.name)
            .set(FILLS.FILLED_QTY_DELTA, filledQtyDelta)
            .set(FILLS.FILLED_QTY_TOTAL, filledQtyTotal)
            .set(FILLS.PRICE, price)
            .set(FILLS.QUOTE_CCY, quoteCcy)
            .set(FILLS.QUOTE_CASH_DELTA, quoteCashDelta)
            .set(FILLS.FEE_QUOTE, feeQuote)
            .execute()
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
