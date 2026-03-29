package backofficejava.business;

import backofficejava.persistence.JdbcConnectionFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public final class JdbcBusinessPackageStore {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final JdbcConnectionFactory connectionFactory;

    public JdbcBusinessPackageStore(JdbcConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public ExecutionPackageReadModel executionPackageReadModel() {
        return new ExecutionPackageAdapter(this);
    }

    public PostTradePackageReadModel postTradePackageReadModel() {
        return new PostTradePackageAdapter(this);
    }

    private Optional<ExecutionPackageView> loadExecutionPackage(String orderId) {
        return load(orderId, "bo_execution_packages", ExecutionPackageView.class);
    }

    private void upsertExecutionPackage(ExecutionPackageView view) {
        upsert("bo_execution_packages", view.orderId(), view.accountId(), view.symbol(), view.generatedAt(), view);
    }

    private Optional<PostTradePackageView> loadPostTradePackage(String orderId) {
        return load(orderId, "bo_post_trade_packages", PostTradePackageView.class);
    }

    private void upsertPostTradePackage(PostTradePackageView view) {
        upsert("bo_post_trade_packages", view.orderId(), view.accountId(), view.symbol(), view.generatedAt(), view);
    }

    private <T> Optional<T> load(String orderId, String tableName, Class<T> type) {
        String sql = "SELECT payload FROM " + tableName + " WHERE order_id = ?";
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, orderId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(OBJECT_MAPPER.readValue(resultSet.getString("payload"), type));
            }
        } catch (Exception exception) {
            throw new IllegalStateException("failed_to_load_business_package:" + tableName + ":" + orderId, exception);
        }
    }

    private void upsert(String tableName, String orderId, String accountId, String symbol, long generatedAt, Object payload) {
        String sql = """
            INSERT INTO %s (order_id, account_id, symbol, generated_at, payload)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (order_id)
            DO UPDATE SET
                account_id = EXCLUDED.account_id,
                symbol = EXCLUDED.symbol,
                generated_at = EXCLUDED.generated_at,
                payload = EXCLUDED.payload
            """.formatted(tableName);
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, orderId);
            statement.setString(2, accountId);
            statement.setString(3, symbol);
            statement.setLong(4, generatedAt);
            statement.setString(5, OBJECT_MAPPER.writeValueAsString(payload));
            statement.executeUpdate();
        } catch (Exception exception) {
            throw new IllegalStateException("failed_to_upsert_business_package:" + tableName + ":" + orderId, exception);
        }
    }

    private void reset() {
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement executionDelete = connection.prepareStatement("DELETE FROM bo_execution_packages");
            PreparedStatement postTradeDelete = connection.prepareStatement("DELETE FROM bo_post_trade_packages")
        ) {
            executionDelete.executeUpdate();
            postTradeDelete.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_reset_business_packages", exception);
        }
    }

    private record ExecutionPackageAdapter(JdbcBusinessPackageStore store) implements ExecutionPackageReadModel {
        @Override
        public Optional<ExecutionPackageView> findByOrderId(String orderId) {
            return store.loadExecutionPackage(orderId);
        }

        @Override
        public void upsert(ExecutionPackageView view) {
            store.upsertExecutionPackage(view);
        }

        @Override
        public void reset() {
            store.reset();
        }
    }

    private record PostTradePackageAdapter(JdbcBusinessPackageStore store) implements PostTradePackageReadModel {
        @Override
        public Optional<PostTradePackageView> findByOrderId(String orderId) {
            return store.loadPostTradePackage(orderId);
        }

        @Override
        public void upsert(PostTradePackageView view) {
            store.upsertPostTradePackage(view);
        }

        @Override
        public void reset() {
            store.reset();
        }
    }
}
