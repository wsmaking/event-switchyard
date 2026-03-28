package backofficejava.audit;

import backofficejava.persistence.JdbcConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class JdbcAuditOffsetStore implements AuditOffsetStore {
    private final JdbcConnectionFactory connectionFactory;
    private final String consumerName;

    public JdbcAuditOffsetStore(JdbcConnectionFactory connectionFactory, String consumerName) {
        this.connectionFactory = connectionFactory;
        this.consumerName = consumerName;
    }

    @Override
    public long readOffset() {
        String sql = "SELECT current_offset FROM bo_consumer_checkpoint WHERE consumer_name = ?";
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, consumerName);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getLong(1) : 0L;
            }
        } catch (SQLException exception) {
            return 0L;
        }
    }

    @Override
    public void writeOffset(long offset) {
        String sql = """
            INSERT INTO bo_consumer_checkpoint (consumer_name, current_offset, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT (consumer_name)
            DO UPDATE SET current_offset = EXCLUDED.current_offset, updated_at = EXCLUDED.updated_at
            """;
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, consumerName);
            statement.setLong(2, offset);
            statement.setLong(3, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_write_backoffice_checkpoint:" + consumerName, exception);
        }
    }

    @Override
    public String describe() {
        return "db:bo_consumer_checkpoint/" + consumerName;
    }
}
