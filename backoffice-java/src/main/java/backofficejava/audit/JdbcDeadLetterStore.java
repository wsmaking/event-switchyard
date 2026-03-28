package backofficejava.audit;

import backofficejava.persistence.JdbcConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class JdbcDeadLetterStore implements DeadLetterStore {
    private final JdbcConnectionFactory connectionFactory;

    public JdbcDeadLetterStore(JdbcConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void append(DeadLetterEntryView entry) {
        withTransaction(connection -> {
            deleteByEventRef(connection, entry.eventRef());
            try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO bo_dead_letter (
                    event_ref, account_id, order_id, event_type, reason, detail, payload, event_at, recorded_at, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
                statement.setString(1, entry.eventRef());
                statement.setString(2, entry.accountId());
                statement.setString(3, entry.orderId());
                statement.setString(4, entry.eventType());
                statement.setString(5, entry.reason());
                statement.setString(6, entry.detail());
                statement.setString(7, entry.rawLine());
                statement.setLong(8, entry.eventAt());
                statement.setLong(9, entry.recordedAt());
                statement.setString(10, entry.source());
                statement.executeUpdate();
            }
        }, "failed_to_append_backoffice_dead_letter:" + entry.eventRef());
    }

    @Override
    public List<DeadLetterEntryView> find(String orderId, int limit) {
        String sql = """
            SELECT id, event_ref, account_id, order_id, event_type, reason, detail, payload, event_at, recorded_at, source
              FROM bo_dead_letter
             WHERE (? IS NULL OR ? = '' OR order_id = ?)
             ORDER BY recorded_at DESC, id DESC
             LIMIT ?
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            bindOrderFilter(statement, orderId);
            statement.setInt(4, Math.max(1, limit));
            try (ResultSet resultSet = statement.executeQuery()) {
                List<DeadLetterEntryView> entries = new ArrayList<>();
                while (resultSet.next()) {
                    entries.add(mapDeadLetter(resultSet));
                }
                return entries;
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_query_backoffice_dead_letters", exception);
        }
    }

    @Override
    public int size() {
        return count("SELECT COUNT(*) FROM bo_dead_letter", "failed_to_count_backoffice_dead_letters");
    }

    @Override
    public DeadLetterEntryView findByEventRef(String eventRef) {
        String sql = """
            SELECT id, event_ref, account_id, order_id, event_type, reason, detail, payload, event_at, recorded_at, source
              FROM bo_dead_letter
             WHERE event_ref = ?
             ORDER BY recorded_at DESC, id DESC
             LIMIT 1
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, eventRef);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? mapDeadLetter(resultSet) : null;
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_find_backoffice_dead_letter:" + eventRef, exception);
        }
    }

    @Override
    public void removeByEventRef(String eventRef) {
        withTransaction(connection -> deleteByEventRef(connection, eventRef), "failed_to_remove_backoffice_dead_letter:" + eventRef);
    }

    @Override
    public void reset() {
        withTransaction(connection -> execute(connection, "DELETE FROM bo_dead_letter"), "failed_to_reset_backoffice_dead_letters");
    }

    private static void bindOrderFilter(PreparedStatement statement, String orderId) throws SQLException {
        statement.setString(1, orderId);
        statement.setString(2, orderId);
        statement.setString(3, orderId);
    }

    private static DeadLetterEntryView mapDeadLetter(ResultSet resultSet) throws SQLException {
        return new DeadLetterEntryView(
            String.valueOf(resultSet.getLong("id")),
            resultSet.getString("event_ref"),
            resultSet.getString("account_id"),
            resultSet.getString("order_id"),
            resultSet.getString("event_type"),
            resultSet.getString("reason"),
            resultSet.getString("detail"),
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
        try (PreparedStatement statement = connection.prepareStatement("DELETE FROM bo_dead_letter WHERE event_ref = ?")) {
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
