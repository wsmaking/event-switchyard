package backofficejava.account;

import backofficejava.persistence.JdbcConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class JdbcBackOfficeStore {
    private static final long INITIAL_CASH = 10_000_000L;

    private final JdbcConnectionFactory connectionFactory;
    private final String seedAccountId;

    public JdbcBackOfficeStore(JdbcConnectionFactory connectionFactory, String seedAccountId) {
        this.connectionFactory = connectionFactory;
        this.seedAccountId = seedAccountId;
        ensureSeedAccount();
    }

    public AccountOverviewReadModel accountOverviewReadModel() {
        return new AccountOverviewAdapter(this);
    }

    public PositionReadModel positionReadModel() {
        return new PositionAdapter(this);
    }

    public FillReadModel fillReadModel() {
        return new FillAdapter(this);
    }

    public OrderProjectionStateStore orderProjectionStateStore() {
        return new OrderStateAdapter(this);
    }

    public LedgerReadModel ledgerReadModel() {
        return new LedgerAdapter(this);
    }

    private Optional<AccountOverviewView> loadAccountOverview(String accountId) {
        String sql = """
            SELECT account_id, cash_balance, available_buying_power, reserved_buying_power,
                   position_count, realized_pnl, updated_at
            FROM bo_account_overview
            WHERE account_id = ?
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, accountId);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? Optional.of(mapAccountOverview(resultSet)) : Optional.empty();
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_load_backoffice_account:" + accountId, exception);
        }
    }

    private void upsertAccountOverview(AccountOverviewView view) {
        String sql = """
            INSERT INTO bo_account_overview (
                account_id, cash_balance, available_buying_power, reserved_buying_power,
                position_count, realized_pnl, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (account_id)
            DO UPDATE SET
                cash_balance = EXCLUDED.cash_balance,
                available_buying_power = EXCLUDED.available_buying_power,
                reserved_buying_power = EXCLUDED.reserved_buying_power,
                position_count = EXCLUDED.position_count,
                realized_pnl = EXCLUDED.realized_pnl,
                updated_at = EXCLUDED.updated_at
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, view.accountId());
            statement.setLong(2, view.cashBalance());
            statement.setLong(3, view.availableBuyingPower());
            statement.setLong(4, view.reservedBuyingPower());
            statement.setInt(5, view.positionCount());
            statement.setLong(6, view.realizedPnl());
            statement.setTimestamp(7, Timestamp.from(view.updatedAt()));
            statement.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_upsert_backoffice_account:" + view.accountId(), exception);
        }
    }

    private List<PositionView> loadPositionsByAccountId(String accountId) {
        String sql = """
            SELECT account_id, symbol, net_qty, avg_price
            FROM bo_positions
            WHERE account_id = ?
            ORDER BY symbol ASC
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, accountId);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<PositionView> positions = new ArrayList<>();
                while (resultSet.next()) {
                    positions.add(new PositionView(
                        resultSet.getString("account_id"),
                        resultSet.getString("symbol"),
                        resultSet.getLong("net_qty"),
                        resultSet.getDouble("avg_price")
                    ));
                }
                return positions;
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_load_backoffice_positions:" + accountId, exception);
        }
    }

    private void replacePositions(String accountId, List<PositionView> positions) {
        String deleteSql = "DELETE FROM bo_positions WHERE account_id = ?";
        String insertSql = """
            INSERT INTO bo_positions (account_id, symbol, net_qty, avg_price)
            VALUES (?, ?, ?, ?)
            """;
        withTransaction(connection -> {
            try (PreparedStatement delete = connection.prepareStatement(deleteSql)) {
                delete.setString(1, accountId);
                delete.executeUpdate();
            }
            try (PreparedStatement insert = connection.prepareStatement(insertSql)) {
                for (PositionView position : positions) {
                    insert.setString(1, position.accountId());
                    insert.setString(2, position.symbol());
                    insert.setLong(3, position.netQty());
                    insert.setDouble(4, position.avgPrice());
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            updatePositionCount(connection, accountId, positions.size());
        }, "failed_to_replace_backoffice_positions:" + accountId);
    }

    private List<FillView> loadFillsByOrderId(String orderId) {
        String sql = """
            SELECT fill_id, order_id, account_id, symbol, side, quantity, price, notional, liquidity, filled_at
            FROM bo_fills
            WHERE order_id = ?
            ORDER BY filled_at ASC, fill_id ASC
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, orderId);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<FillView> fills = new ArrayList<>();
                while (resultSet.next()) {
                    fills.add(new FillView(
                        resultSet.getString("fill_id"),
                        resultSet.getString("order_id"),
                        resultSet.getString("account_id"),
                        resultSet.getString("symbol"),
                        resultSet.getString("side"),
                        resultSet.getLong("quantity"),
                        resultSet.getDouble("price"),
                        resultSet.getLong("notional"),
                        resultSet.getString("liquidity"),
                        resultSet.getLong("filled_at")
                    ));
                }
                return fills;
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_load_backoffice_fills:" + orderId, exception);
        }
    }

    private void replaceFills(String orderId, List<FillView> fills) {
        String deleteSql = "DELETE FROM bo_fills WHERE order_id = ?";
        String insertSql = """
            INSERT INTO bo_fills (
                fill_id, order_id, account_id, symbol, side, quantity, price, notional, liquidity, filled_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        withTransaction(connection -> {
            try (PreparedStatement delete = connection.prepareStatement(deleteSql)) {
                delete.setString(1, orderId);
                delete.executeUpdate();
            }
            try (PreparedStatement insert = connection.prepareStatement(insertSql)) {
                for (FillView fill : fills) {
                    insert.setString(1, fill.fillId());
                    insert.setString(2, fill.orderId());
                    insert.setString(3, fill.accountId());
                    insert.setString(4, fill.symbol());
                    insert.setString(5, fill.side());
                    insert.setLong(6, fill.quantity());
                    insert.setDouble(7, fill.price());
                    insert.setLong(8, fill.notional());
                    insert.setString(9, fill.liquidity());
                    insert.setLong(10, fill.filledAt());
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }, "failed_to_replace_backoffice_fills:" + orderId);
    }

    private Optional<OrderProjectionState> loadOrderStateByOrderId(String orderId) {
        String sql = """
            SELECT order_id, account_id, symbol, side, quantity, working_price, submitted_at,
                   last_event_at, status, filled_quantity, reserved_amount
            FROM bo_order_states
            WHERE order_id = ?
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, orderId);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? Optional.of(mapOrderState(resultSet)) : Optional.empty();
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_load_backoffice_order_state:" + orderId, exception);
        }
    }

    private List<OrderProjectionState> loadOrderStatesByAccountId(String accountId) {
        String sql = """
            SELECT order_id, account_id, symbol, side, quantity, working_price, submitted_at,
                   last_event_at, status, filled_quantity, reserved_amount
            FROM bo_order_states
            WHERE account_id = ?
            ORDER BY last_event_at DESC, order_id ASC
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, accountId);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<OrderProjectionState> states = new ArrayList<>();
                while (resultSet.next()) {
                    states.add(mapOrderState(resultSet));
                }
                return states;
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_load_backoffice_order_states:" + accountId, exception);
        }
    }

    private void upsertOrderState(OrderProjectionState state) {
        String sql = """
            INSERT INTO bo_order_states (
                order_id, account_id, symbol, side, quantity, working_price, submitted_at,
                last_event_at, status, filled_quantity, reserved_amount
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (order_id)
            DO UPDATE SET
                account_id = EXCLUDED.account_id,
                symbol = EXCLUDED.symbol,
                side = EXCLUDED.side,
                quantity = EXCLUDED.quantity,
                working_price = EXCLUDED.working_price,
                submitted_at = EXCLUDED.submitted_at,
                last_event_at = EXCLUDED.last_event_at,
                status = EXCLUDED.status,
                filled_quantity = EXCLUDED.filled_quantity,
                reserved_amount = EXCLUDED.reserved_amount
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, state.orderId());
            statement.setString(2, state.accountId());
            statement.setString(3, state.symbol());
            statement.setString(4, state.side());
            statement.setLong(5, state.quantity());
            statement.setLong(6, state.workingPrice());
            statement.setLong(7, state.submittedAt());
            statement.setLong(8, state.lastEventAt());
            statement.setString(9, state.status());
            statement.setLong(10, state.filledQuantity());
            statement.setLong(11, state.reservedAmount());
            statement.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_upsert_backoffice_order_state:" + state.orderId(), exception);
        }
    }

    private List<LedgerEntryView> queryLedger(String accountId, String orderId, String eventType, int limit, Long afterEventAt) {
        StringBuilder sql = new StringBuilder("""
            SELECT entry_id, event_ref, account_id, order_id, event_type, symbol, side,
                   quantity_delta, cash_delta, reserved_buying_power_delta, realized_pnl_delta,
                   detail, event_at, source
            FROM bo_ledger_entries
            WHERE 1 = 1
            """);
        List<Object> parameters = new ArrayList<>();
        if (accountId != null && !accountId.isBlank()) {
            sql.append(" AND account_id = ?");
            parameters.add(accountId);
        }
        if (orderId != null && !orderId.isBlank()) {
            sql.append(" AND order_id = ?");
            parameters.add(orderId);
        }
        if (eventType != null && !eventType.isBlank()) {
            sql.append(" AND event_type = ?");
            parameters.add(eventType);
        }
        if (afterEventAt != null) {
            sql.append(" AND event_at > ?");
            parameters.add(afterEventAt);
        }
        sql.append(" ORDER BY event_at DESC, entry_id DESC LIMIT ?");
        parameters.add(Math.max(1, limit));
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql.toString())
        ) {
            bindParameters(statement, parameters);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<LedgerEntryView> entries = new ArrayList<>();
                while (resultSet.next()) {
                    entries.add(mapLedgerEntry(resultSet));
                }
                return entries;
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_query_backoffice_ledger", exception);
        }
    }

    private List<LedgerEntryView> loadLedgerByAccountId(String accountId) {
        String sql = """
            SELECT entry_id, event_ref, account_id, order_id, event_type, symbol, side,
                   quantity_delta, cash_delta, reserved_buying_power_delta, realized_pnl_delta,
                   detail, event_at, source
            FROM bo_ledger_entries
            WHERE account_id = ?
            ORDER BY event_at ASC, entry_id ASC
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, accountId);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<LedgerEntryView> entries = new ArrayList<>();
                while (resultSet.next()) {
                    entries.add(mapLedgerEntry(resultSet));
                }
                return entries;
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_load_backoffice_ledger_by_account:" + accountId, exception);
        }
    }

    private void appendLedger(LedgerEntryView entry) {
        String insertLedgerSql = """
            INSERT INTO bo_ledger_entries (
                entry_id, event_ref, account_id, order_id, event_type, symbol, side,
                quantity_delta, cash_delta, reserved_buying_power_delta, realized_pnl_delta,
                detail, event_at, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        String insertDedupSql = """
            INSERT INTO bo_event_dedup (event_ref, recorded_at)
            VALUES (?, ?)
            ON CONFLICT (event_ref) DO NOTHING
            """;
        withTransaction(connection -> {
            try (PreparedStatement ledger = connection.prepareStatement(insertLedgerSql)) {
                ledger.setString(1, entry.entryId());
                ledger.setString(2, entry.eventRef());
                ledger.setString(3, entry.accountId());
                ledger.setString(4, entry.orderId());
                ledger.setString(5, entry.eventType());
                ledger.setString(6, entry.symbol());
                ledger.setString(7, entry.side());
                ledger.setLong(8, entry.quantityDelta());
                ledger.setLong(9, entry.cashDelta());
                ledger.setLong(10, entry.reservedBuyingPowerDelta());
                ledger.setLong(11, entry.realizedPnlDelta());
                ledger.setString(12, entry.detail());
                ledger.setLong(13, entry.eventAt());
                ledger.setString(14, entry.source());
                ledger.executeUpdate();
            }
            try (PreparedStatement dedup = connection.prepareStatement(insertDedupSql)) {
                dedup.setString(1, entry.eventRef());
                dedup.setLong(2, entry.eventAt());
                dedup.executeUpdate();
            }
        }, "failed_to_append_backoffice_ledger:" + entry.entryId());
    }

    private boolean containsEventRefInternal(String eventRef) {
        String sql = "SELECT 1 FROM bo_event_dedup WHERE event_ref = ?";
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, eventRef);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_check_backoffice_event_ref:" + eventRef, exception);
        }
    }

    private void resetAll() {
        withTransaction(connection -> {
            execute(connection, "DELETE FROM bo_fills");
            execute(connection, "DELETE FROM bo_positions");
            execute(connection, "DELETE FROM bo_order_states");
            execute(connection, "DELETE FROM bo_ledger_entries");
            execute(connection, "DELETE FROM bo_event_dedup");
            execute(connection, "DELETE FROM bo_dead_letter");
            execute(connection, "DELETE FROM bo_pending_orphan");
            execute(connection, "DELETE FROM bo_account_overview");
            insertSeedAccount(connection);
        }, "failed_to_reset_backoffice_store");
    }

    private void ensureSeedAccount() {
        if (loadAccountOverview(seedAccountId).isEmpty()) {
            upsertAccountOverview(initialAccount(seedAccountId));
        }
    }

    private void insertSeedAccount(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
            INSERT INTO bo_account_overview (
                account_id, cash_balance, available_buying_power, reserved_buying_power,
                position_count, realized_pnl, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """)) {
            AccountOverviewView seed = initialAccount(seedAccountId);
            statement.setString(1, seed.accountId());
            statement.setLong(2, seed.cashBalance());
            statement.setLong(3, seed.availableBuyingPower());
            statement.setLong(4, seed.reservedBuyingPower());
            statement.setInt(5, seed.positionCount());
            statement.setLong(6, seed.realizedPnl());
            statement.setTimestamp(7, Timestamp.from(seed.updatedAt()));
            statement.executeUpdate();
        }
    }

    private static AccountOverviewView initialAccount(String accountId) {
        return new AccountOverviewView(
            accountId,
            INITIAL_CASH,
            INITIAL_CASH,
            0L,
            0,
            0L,
            Instant.parse("2026-03-28T00:00:01Z")
        );
    }

    private static AccountOverviewView mapAccountOverview(ResultSet resultSet) throws SQLException {
        return new AccountOverviewView(
            resultSet.getString("account_id"),
            resultSet.getLong("cash_balance"),
            resultSet.getLong("available_buying_power"),
            resultSet.getLong("reserved_buying_power"),
            resultSet.getInt("position_count"),
            resultSet.getLong("realized_pnl"),
            resultSet.getTimestamp("updated_at").toInstant()
        );
    }

    private static OrderProjectionState mapOrderState(ResultSet resultSet) throws SQLException {
        return new OrderProjectionState(
            resultSet.getString("order_id"),
            resultSet.getString("account_id"),
            resultSet.getString("symbol"),
            resultSet.getString("side"),
            resultSet.getLong("quantity"),
            resultSet.getLong("working_price"),
            resultSet.getLong("submitted_at"),
            resultSet.getLong("last_event_at"),
            resultSet.getString("status"),
            resultSet.getLong("filled_quantity"),
            resultSet.getLong("reserved_amount")
        );
    }

    private static LedgerEntryView mapLedgerEntry(ResultSet resultSet) throws SQLException {
        return new LedgerEntryView(
            resultSet.getString("entry_id"),
            resultSet.getString("event_ref"),
            resultSet.getString("account_id"),
            resultSet.getString("order_id"),
            resultSet.getString("event_type"),
            resultSet.getString("symbol"),
            resultSet.getString("side"),
            resultSet.getLong("quantity_delta"),
            resultSet.getLong("cash_delta"),
            resultSet.getLong("reserved_buying_power_delta"),
            resultSet.getLong("realized_pnl_delta"),
            resultSet.getString("detail"),
            resultSet.getLong("event_at"),
            resultSet.getString("source")
        );
    }

    private static void bindParameters(PreparedStatement statement, List<Object> parameters) throws SQLException {
        for (int index = 0; index < parameters.size(); index++) {
            Object value = parameters.get(index);
            if (value instanceof String string) {
                statement.setString(index + 1, string);
            } else if (value instanceof Long longValue) {
                statement.setLong(index + 1, longValue);
            } else if (value instanceof Integer intValue) {
                statement.setInt(index + 1, intValue);
            } else {
                statement.setObject(index + 1, value);
            }
        }
    }

    private static void updatePositionCount(Connection connection, String accountId, int positionCount) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
            UPDATE bo_account_overview
            SET position_count = ?
            WHERE account_id = ?
            """)) {
            statement.setInt(1, positionCount);
            statement.setString(2, accountId);
            statement.executeUpdate();
        }
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeUpdate();
        }
    }

    private void withTransaction(SqlConsumer consumer, String errorMessage) {
        try (Connection connection = connectionFactory.openConnection()) {
            connection.setAutoCommit(false);
            try {
                consumer.accept(connection);
                connection.commit();
            } catch (Exception exception) {
                connection.rollback();
                throw exception;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (Exception exception) {
            throw new IllegalStateException(errorMessage, exception);
        }
    }

    private record AccountOverviewAdapter(JdbcBackOfficeStore store) implements AccountOverviewReadModel {
        @Override
        public Optional<AccountOverviewView> findByAccountId(String accountId) {
            return store.loadAccountOverview(accountId);
        }

        @Override
        public void upsert(AccountOverviewView view) {
            store.upsertAccountOverview(view);
        }

        @Override
        public void reset() {
            store.resetAll();
        }
    }

    private record PositionAdapter(JdbcBackOfficeStore store) implements PositionReadModel {
        @Override
        public List<PositionView> findByAccountId(String accountId) {
            return store.loadPositionsByAccountId(accountId);
        }

        @Override
        public void replacePositions(String accountId, List<PositionView> positions) {
            store.replacePositions(accountId, positions);
        }

        @Override
        public void reset() {
            store.resetAll();
        }
    }

    private record FillAdapter(JdbcBackOfficeStore store) implements FillReadModel {
        @Override
        public List<FillView> findByOrderId(String orderId) {
            return store.loadFillsByOrderId(orderId);
        }

        @Override
        public void replaceFills(String orderId, List<FillView> fills) {
            store.replaceFills(orderId, fills);
        }

        @Override
        public void reset() {
            store.resetAll();
        }
    }

    private record OrderStateAdapter(JdbcBackOfficeStore store) implements OrderProjectionStateStore {
        @Override
        public Optional<OrderProjectionState> findByOrderId(String orderId) {
            return store.loadOrderStateByOrderId(orderId);
        }

        @Override
        public List<OrderProjectionState> findByAccountId(String accountId) {
            return store.loadOrderStatesByAccountId(accountId);
        }

        @Override
        public void upsert(OrderProjectionState state) {
            store.upsertOrderState(state);
        }

        @Override
        public void reset() {
            store.resetAll();
        }
    }

    private record LedgerAdapter(JdbcBackOfficeStore store) implements LedgerReadModel {
        @Override
        public List<LedgerEntryView> find(String accountId, String orderId, String eventType, int limit, Long afterEventAt) {
            return store.queryLedger(accountId, orderId, eventType, limit, afterEventAt);
        }

        @Override
        public List<LedgerEntryView> findByAccountId(String accountId) {
            return store.loadLedgerByAccountId(accountId);
        }

        @Override
        public void append(LedgerEntryView entry) {
            store.appendLedger(entry);
        }

        @Override
        public boolean containsEventRef(String eventRef) {
            return store.containsEventRefInternal(eventRef);
        }

        @Override
        public void reset() {
            store.resetAll();
        }
    }

    @FunctionalInterface
    private interface SqlConsumer {
        void accept(Connection connection) throws Exception;
    }
}
