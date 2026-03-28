package oms.order;

import oms.persistence.JdbcConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class JdbcOrderReadModel implements OrderReadModel {
    private final JdbcConnectionFactory connectionFactory;

    public JdbcOrderReadModel(JdbcConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public List<OrderView> findAll() {
        String sql = """
            SELECT order_id, account_id, symbol, side, type, quantity, price, time_in_force, expire_at, status,
                   submitted_at, filled_at, execution_time_ms, status_reason, filled_quantity, remaining_quantity
            FROM oms_orders
            ORDER BY submitted_at DESC
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery()
        ) {
            List<OrderView> orders = new ArrayList<>();
            while (resultSet.next()) {
                orders.add(mapOrder(resultSet));
            }
            return orders;
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_load_oms_orders", exception);
        }
    }

    @Override
    public Optional<OrderView> findById(String orderId) {
        String sql = """
            SELECT order_id, account_id, symbol, side, type, quantity, price, time_in_force, expire_at, status,
                   submitted_at, filled_at, execution_time_ms, status_reason, filled_quantity, remaining_quantity
            FROM oms_orders
            WHERE order_id = ?
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, orderId);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? Optional.of(mapOrder(resultSet)) : Optional.empty();
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_load_oms_order:" + orderId, exception);
        }
    }

    @Override
    public List<OrderEventView> findEventsByOrderId(String orderId) {
        String sql = """
            SELECT order_id, event_type, event_at, label, detail, source, event_ref
            FROM oms_order_events
            WHERE order_id = ?
            ORDER BY event_at ASC, event_ref ASC
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, orderId);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<OrderEventView> events = new ArrayList<>();
                while (resultSet.next()) {
                    events.add(new OrderEventView(
                        resultSet.getString("order_id"),
                        resultSet.getString("event_type"),
                        resultSet.getLong("event_at"),
                        resultSet.getString("label"),
                        resultSet.getString("detail"),
                        resultSet.getString("source"),
                        resultSet.getString("event_ref")
                    ));
                }
                return events;
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_load_oms_events:" + orderId, exception);
        }
    }

    @Override
    public List<ReservationView> findReservationsByAccountId(String accountId) {
        String sql = """
            SELECT reservation_id, account_id, order_id, symbol, side, reserved_quantity, reserved_amount,
                   released_amount, status, opened_at, updated_at
            FROM oms_reservations
            WHERE account_id = ?
            ORDER BY updated_at DESC, reservation_id ASC
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, accountId);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<ReservationView> reservations = new ArrayList<>();
                while (resultSet.next()) {
                    reservations.add(new ReservationView(
                        resultSet.getString("reservation_id"),
                        resultSet.getString("account_id"),
                        resultSet.getString("order_id"),
                        resultSet.getString("symbol"),
                        resultSet.getString("side"),
                        resultSet.getLong("reserved_quantity"),
                        resultSet.getLong("reserved_amount"),
                        resultSet.getLong("released_amount"),
                        resultSet.getString("status"),
                        resultSet.getLong("opened_at"),
                        resultSet.getLong("updated_at")
                    ));
                }
                return reservations;
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_load_oms_reservations:" + accountId, exception);
        }
    }

    @Override
    public void upsert(OrderView orderView) {
        String sql = """
            INSERT INTO oms_orders (
                order_id, account_id, symbol, side, type, quantity, price, time_in_force, expire_at, status,
                submitted_at, filled_at, execution_time_ms, status_reason, filled_quantity, remaining_quantity, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (order_id)
            DO UPDATE SET
                account_id = EXCLUDED.account_id,
                symbol = EXCLUDED.symbol,
                side = EXCLUDED.side,
                type = EXCLUDED.type,
                quantity = EXCLUDED.quantity,
                price = EXCLUDED.price,
                time_in_force = EXCLUDED.time_in_force,
                expire_at = EXCLUDED.expire_at,
                status = EXCLUDED.status,
                submitted_at = EXCLUDED.submitted_at,
                filled_at = EXCLUDED.filled_at,
                execution_time_ms = EXCLUDED.execution_time_ms,
                status_reason = EXCLUDED.status_reason,
                filled_quantity = EXCLUDED.filled_quantity,
                remaining_quantity = EXCLUDED.remaining_quantity,
                updated_at = EXCLUDED.updated_at
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            bindOrder(statement, orderView);
            statement.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_upsert_oms_order:" + orderView.id(), exception);
        }
    }

    @Override
    public void replaceEvents(String orderId, List<OrderEventView> events) {
        String deleteSql = "DELETE FROM oms_order_events WHERE order_id = ?";
        String insertSql = """
            INSERT INTO oms_order_events (order_id, event_ref, event_type, event_at, label, detail, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;
        withTransaction(connection -> {
            try (PreparedStatement delete = connection.prepareStatement(deleteSql)) {
                delete.setString(1, orderId);
                delete.executeUpdate();
            }
            try (PreparedStatement insert = connection.prepareStatement(insertSql)) {
                for (OrderEventView event : events) {
                    insert.setString(1, event.orderId());
                    insert.setString(2, event.eventRef());
                    insert.setString(3, event.eventType());
                    insert.setLong(4, event.eventAt());
                    insert.setString(5, event.label());
                    insert.setString(6, event.detail());
                    insert.setString(7, event.source());
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }, "failed_to_replace_oms_events:" + orderId);
    }

    @Override
    public void replaceReservations(String accountId, List<ReservationView> reservations) {
        String deleteSql = "DELETE FROM oms_reservations WHERE account_id = ?";
        String insertSql = """
            INSERT INTO oms_reservations (
                reservation_id, account_id, order_id, symbol, side, reserved_quantity,
                reserved_amount, released_amount, status, opened_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        withTransaction(connection -> {
            try (PreparedStatement delete = connection.prepareStatement(deleteSql)) {
                delete.setString(1, accountId);
                delete.executeUpdate();
            }
            try (PreparedStatement insert = connection.prepareStatement(insertSql)) {
                for (ReservationView reservation : reservations) {
                    insert.setString(1, reservation.reservationId());
                    insert.setString(2, reservation.accountId());
                    insert.setString(3, reservation.orderId());
                    insert.setString(4, reservation.symbol());
                    insert.setString(5, reservation.side());
                    insert.setLong(6, reservation.reservedQuantity());
                    insert.setLong(7, reservation.reservedAmount());
                    insert.setLong(8, reservation.releasedAmount());
                    insert.setString(9, reservation.status());
                    insert.setLong(10, reservation.openedAt());
                    insert.setLong(11, reservation.updatedAt());
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }, "failed_to_replace_oms_reservations:" + accountId);
    }

    @Override
    public void reset() {
        withTransaction(connection -> {
            execute(connection, "DELETE FROM oms_order_events");
            execute(connection, "DELETE FROM oms_reservations");
            execute(connection, "DELETE FROM oms_orders");
            execute(connection, "DELETE FROM oms_event_dedup");
            execute(connection, "DELETE FROM oms_outbox");
            execute(connection, "DELETE FROM oms_dead_letter");
            execute(connection, "DELETE FROM oms_pending_orphan");
        }, "failed_to_reset_oms_store");
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

    private static void bindOrder(PreparedStatement statement, OrderView orderView) throws SQLException {
        statement.setString(1, orderView.id());
        statement.setString(2, orderView.accountId());
        statement.setString(3, orderView.symbol());
        statement.setString(4, orderView.side());
        statement.setString(5, orderView.type());
        statement.setInt(6, orderView.quantity());
        if (orderView.price() == null) {
            statement.setNull(7, Types.DOUBLE);
        } else {
            statement.setDouble(7, orderView.price());
        }
        statement.setString(8, orderView.timeInForce());
        if (orderView.expireAt() == null) {
            statement.setNull(9, Types.BIGINT);
        } else {
            statement.setLong(9, orderView.expireAt());
        }
        statement.setString(10, orderView.status().name());
        statement.setLong(11, orderView.submittedAt());
        if (orderView.filledAt() == null) {
            statement.setNull(12, Types.BIGINT);
        } else {
            statement.setLong(12, orderView.filledAt());
        }
        if (orderView.executionTimeMs() == null) {
            statement.setNull(13, Types.DOUBLE);
        } else {
            statement.setDouble(13, orderView.executionTimeMs());
        }
        statement.setString(14, orderView.statusReason());
        statement.setLong(15, orderView.filledQuantity());
        statement.setLong(16, orderView.remainingQuantity());
        statement.setLong(17, System.currentTimeMillis());
    }

    private static OrderView mapOrder(ResultSet resultSet) throws SQLException {
        return new OrderView(
            resultSet.getString("order_id"),
            resultSet.getString("account_id"),
            resultSet.getString("symbol"),
            resultSet.getString("side"),
            resultSet.getString("type"),
            resultSet.getInt("quantity"),
            nullableDouble(resultSet, "price"),
            resultSet.getString("time_in_force"),
            nullableLong(resultSet, "expire_at"),
            OrderStatus.valueOf(resultSet.getString("status")),
            resultSet.getLong("submitted_at"),
            nullableLong(resultSet, "filled_at"),
            nullableDouble(resultSet, "execution_time_ms"),
            resultSet.getString("status_reason"),
            resultSet.getLong("filled_quantity"),
            resultSet.getLong("remaining_quantity")
        );
    }

    private static Long nullableLong(ResultSet resultSet, String column) throws SQLException {
        long value = resultSet.getLong(column);
        return resultSet.wasNull() ? null : value;
    }

    private static Double nullableDouble(ResultSet resultSet, String column) throws SQLException {
        double value = resultSet.getDouble(column);
        return resultSet.wasNull() ? null : value;
    }

    @FunctionalInterface
    private interface SqlConsumer {
        void accept(Connection connection) throws Exception;
    }
}
