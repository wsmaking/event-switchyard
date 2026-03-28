package oms.audit;

import oms.persistence.JdbcConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class JdbcPendingOrphanStore implements PendingOrphanStore {
    private final JdbcConnectionFactory connectionFactory;

    public JdbcPendingOrphanStore(JdbcConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void append(PendingOrphanEntryView entry) {
        withTransaction(connection -> {
            deleteByEventRef(connection, entry.eventRef());
            try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO oms_pending_orphan (
                    event_ref, account_id, order_id, event_type, reason, payload, event_at, recorded_at, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
                statement.setString(1, entry.eventRef());
                statement.setString(2, entry.accountId());
                statement.setString(3, entry.orderId());
                statement.setString(4, entry.eventType());
                statement.setString(5, entry.reason());
                statement.setString(6, entry.rawLine());
                statement.setLong(7, entry.eventAt());
                statement.setLong(8, entry.recordedAt());
                statement.setString(9, entry.source());
                statement.executeUpdate();
            }
        }, "failed_to_append_oms_pending_orphan:" + entry.eventRef());
    }

    @Override
    public List<PendingOrphanEntryView> find(String orderId, int limit) {
        String sql = """
            SELECT id, event_ref, account_id, order_id, event_type, reason, payload, event_at, recorded_at, source
              FROM oms_pending_orphan
             WHERE (? IS NULL OR ? = '' OR order_id = ?)
             ORDER BY event_at ASC, id ASC
             LIMIT ?
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            bindOrderFilter(statement, orderId);
            statement.setInt(4, Math.max(1, limit));
            try (ResultSet resultSet = statement.executeQuery()) {
                List<PendingOrphanEntryView> entries = new ArrayList<>();
                while (resultSet.next()) {
                    entries.add(mapPendingOrphan(resultSet));
                }
                return entries;
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_query_oms_pending_orphans", exception);
        }
    }

    @Override
    public void removeByEventRef(String eventRef) {
        withTransaction(connection -> deleteByEventRef(connection, eventRef), "failed_to_remove_oms_pending_orphan:" + eventRef);
    }

    @Override
    public int size() {
        return count("SELECT COUNT(*) FROM oms_pending_orphan", "failed_to_count_oms_pending_orphans");
    }

    @Override
    public void reset() {
        withTransaction(connection -> execute(connection, "DELETE FROM oms_pending_orphan"), "failed_to_reset_oms_pending_orphans");
    }

    private static void bindOrderFilter(PreparedStatement statement, String orderId) throws SQLException {
        statement.setString(1, orderId);
        statement.setString(2, orderId);
        statement.setString(3, orderId);
    }

    private static PendingOrphanEntryView mapPendingOrphan(ResultSet resultSet) throws SQLException {
        return new PendingOrphanEntryView(
            String.valueOf(resultSet.getLong("id")),
            resultSet.getString("event_ref"),
            resultSet.getString("account_id"),
            resultSet.getString("order_id"),
            resultSet.getString("event_type"),
            resultSet.getString("reason"),
            resultSet.getString("payload"),
            resultSet.getLong("event_at"),
            resultSet.getLong("recorded_at"),
            resultSet.getString("source")
        );
    }

    private int count(String sql, String errorMessage) {
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery()
        ) {
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException exception) {
            throw new IllegalStateException(errorMessage, exception);
        }
    }

    private static void deleteByEventRef(Connection connection, String eventRef) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("DELETE FROM oms_pending_orphan WHERE event_ref = ?")) {
            statement.setString(1, eventRef);
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

    @FunctionalInterface
    private interface SqlConsumer {
        void accept(Connection connection) throws Exception;
    }
}
