package oms.audit;

import oms.persistence.JdbcConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class JdbcAggregateSequenceStore implements AggregateSequenceStore {
    private final JdbcConnectionFactory connectionFactory;

    public JdbcAggregateSequenceStore(JdbcConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public long readLastApplied(String aggregateId) {
        if (aggregateId == null || aggregateId.isBlank()) {
            return 0L;
        }
        String sql = """
            SELECT aggregate_seq
              FROM oms_aggregate_progress
             WHERE aggregate_id = ?
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, aggregateId);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getLong("aggregate_seq") : 0L;
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_read_oms_aggregate_progress:" + aggregateId, exception);
        }
    }

    @Override
    public void markApplied(String aggregateId, long aggregateSeq, String eventRef, long eventAt) {
        if (aggregateId == null || aggregateId.isBlank() || aggregateSeq <= 0L) {
            return;
        }
        String sql = """
            INSERT INTO oms_aggregate_progress (aggregate_id, aggregate_seq, event_ref, event_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (aggregate_id)
            DO UPDATE SET
                aggregate_seq = GREATEST(oms_aggregate_progress.aggregate_seq, EXCLUDED.aggregate_seq),
                event_ref = CASE
                    WHEN EXCLUDED.aggregate_seq >= oms_aggregate_progress.aggregate_seq THEN EXCLUDED.event_ref
                    ELSE oms_aggregate_progress.event_ref
                END,
                event_at = CASE
                    WHEN EXCLUDED.aggregate_seq >= oms_aggregate_progress.aggregate_seq THEN EXCLUDED.event_at
                    ELSE oms_aggregate_progress.event_at
                END,
                updated_at = EXCLUDED.updated_at
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, aggregateId);
            statement.setLong(2, aggregateSeq);
            statement.setString(3, eventRef);
            statement.setLong(4, eventAt);
            statement.setLong(5, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_mark_oms_aggregate_progress:" + aggregateId + ":" + aggregateSeq, exception);
        }
    }

    @Override
    public int size() {
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM oms_aggregate_progress");
            ResultSet resultSet = statement.executeQuery()
        ) {
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_count_oms_aggregate_progress", exception);
        }
    }

    @Override
    public void reset() {
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement("DELETE FROM oms_aggregate_progress")
        ) {
            statement.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_reset_oms_aggregate_progress", exception);
        }
    }
}
