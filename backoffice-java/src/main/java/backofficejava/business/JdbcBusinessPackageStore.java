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

    public ParentExecutionStateReadModel parentExecutionStateReadModel() {
        return new ParentExecutionStateAdapter(this);
    }

    public AllocationStateReadModel allocationStateReadModel() {
        return new AllocationStateAdapter(this);
    }

    public SettlementProjectionReadModel settlementProjectionReadModel() {
        return new SettlementProjectionAdapter(this);
    }

    public StatementProjectionReadModel statementProjectionReadModel() {
        return new StatementProjectionAdapter(this);
    }

    public RiskSnapshotReadModel riskSnapshotReadModel() {
        return new RiskSnapshotAdapter(this);
    }

    public SettlementExceptionWorkflowReadModel settlementExceptionWorkflowReadModel() {
        return new SettlementExceptionWorkflowAdapter(this);
    }

    public CorporateActionWorkflowReadModel corporateActionWorkflowReadModel() {
        return new CorporateActionWorkflowAdapter(this);
    }

    public MarginProjectionReadModel marginProjectionReadModel() {
        return new MarginProjectionAdapter(this);
    }

    public ScenarioEvaluationHistoryReadModel scenarioEvaluationHistoryReadModel() {
        return new ScenarioEvaluationHistoryAdapter(this);
    }

    public BacktestHistoryReadModel backtestHistoryReadModel() {
        return new BacktestHistoryAdapter(this);
    }

    public AccountHierarchyReadModel accountHierarchyReadModel() {
        return new AccountHierarchyAdapter(this);
    }

    public OperatorControlStateReadModel operatorControlStateReadModel() {
        return new OperatorControlStateAdapter(this);
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

    private Optional<ParentExecutionStateView> loadParentExecutionState(String orderId) {
        return load(orderId, "bo_parent_execution_states", ParentExecutionStateView.class);
    }

    private void upsertParentExecutionState(ParentExecutionStateView view) {
        upsert("bo_parent_execution_states", view.orderId(), view.accountId(), view.symbol(), view.generatedAt(), view);
    }

    private Optional<AllocationStateView> loadAllocationState(String orderId) {
        return load(orderId, "bo_allocation_states", AllocationStateView.class);
    }

    private void upsertAllocationState(AllocationStateView view) {
        upsert("bo_allocation_states", view.orderId(), view.accountId(), view.symbol(), view.generatedAt(), view);
    }

    private Optional<SettlementProjectionView> loadSettlementProjection(String orderId) {
        return load(orderId, "bo_settlement_projections", SettlementProjectionView.class);
    }

    private void upsertSettlementProjection(SettlementProjectionView view) {
        upsert("bo_settlement_projections", view.orderId(), view.accountId(), view.symbol(), view.generatedAt(), view);
    }

    private Optional<StatementProjectionView> loadStatementProjection(String orderId) {
        return load(orderId, "bo_statement_projections", StatementProjectionView.class);
    }

    private void upsertStatementProjection(StatementProjectionView view) {
        upsert("bo_statement_projections", view.orderId(), view.accountId(), view.symbol(), view.generatedAt(), view);
    }

    private Optional<RiskSnapshotView> loadRiskSnapshot(String accountId) {
        String sql = "SELECT payload FROM bo_risk_snapshots WHERE account_id = ?";
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, accountId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(OBJECT_MAPPER.readValue(resultSet.getString("payload"), RiskSnapshotView.class));
            }
        } catch (Exception exception) {
            throw new IllegalStateException("failed_to_load_risk_snapshot:" + accountId, exception);
        }
    }

    private void upsertRiskSnapshot(RiskSnapshotView view) {
        upsertByAccountId("bo_risk_snapshots", view.accountId(), view.generatedAt(), view);
    }

    private Optional<SettlementExceptionWorkflowView> loadSettlementExceptionWorkflow(String orderId) {
        return load(orderId, "bo_settlement_exception_workflows", SettlementExceptionWorkflowView.class);
    }

    private void upsertSettlementExceptionWorkflow(SettlementExceptionWorkflowView view) {
        upsert("bo_settlement_exception_workflows", view.orderId(), view.accountId(), view.symbol(), view.generatedAt(), view);
    }

    private Optional<CorporateActionWorkflowView> loadCorporateActionWorkflow(String orderId) {
        return load(orderId, "bo_corporate_action_workflows", CorporateActionWorkflowView.class);
    }

    private void upsertCorporateActionWorkflow(CorporateActionWorkflowView view) {
        upsert("bo_corporate_action_workflows", view.orderId(), view.accountId(), view.symbol(), view.generatedAt(), view);
    }

    private Optional<MarginProjectionView> loadMarginProjection(String accountId) {
        return loadByAccountId(accountId, "bo_margin_projections", MarginProjectionView.class);
    }

    private void upsertMarginProjection(MarginProjectionView view) {
        upsertByAccountId("bo_margin_projections", view.accountId(), view.generatedAt(), view);
    }

    private Optional<ScenarioEvaluationHistoryView> loadScenarioEvaluationHistory(String accountId) {
        return loadByAccountId(accountId, "bo_scenario_evaluation_histories", ScenarioEvaluationHistoryView.class);
    }

    private void upsertScenarioEvaluationHistory(ScenarioEvaluationHistoryView view) {
        upsertByAccountId("bo_scenario_evaluation_histories", view.accountId(), view.generatedAt(), view);
    }

    private Optional<BacktestHistoryView> loadBacktestHistory(String accountId) {
        return loadByAccountId(accountId, "bo_backtest_histories", BacktestHistoryView.class);
    }

    private void upsertBacktestHistory(BacktestHistoryView view) {
        upsertByAccountId("bo_backtest_histories", view.accountId(), view.generatedAt(), view);
    }

    private Optional<AccountHierarchyView> loadAccountHierarchy(String accountId) {
        return loadByAccountId(accountId, "bo_account_hierarchies", AccountHierarchyView.class);
    }

    private void upsertAccountHierarchy(AccountHierarchyView view) {
        upsertByAccountId("bo_account_hierarchies", view.accountId(), view.generatedAt(), view);
    }

    private Optional<OperatorControlStateView> loadOperatorControlState(String orderId) {
        return load(orderId, "bo_operator_control_states", OperatorControlStateView.class);
    }

    private void upsertOperatorControlState(OperatorControlStateView view) {
        upsert("bo_operator_control_states", view.orderId(), view.accountId(), "__control__", view.generatedAt(), view);
    }

    private <T> Optional<T> loadByAccountId(String accountId, String tableName, Class<T> type) {
        String sql = "SELECT payload FROM " + tableName + " WHERE account_id = ?";
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, accountId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(OBJECT_MAPPER.readValue(resultSet.getString("payload"), type));
            }
        } catch (Exception exception) {
            throw new IllegalStateException("failed_to_load_business_package:" + tableName + ":" + accountId, exception);
        }
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

    private void upsertByAccountId(String tableName, String accountId, long generatedAt, Object payload) {
        String sql = """
            INSERT INTO %s (account_id, generated_at, payload)
            VALUES (?, ?, ?)
            ON CONFLICT (account_id)
            DO UPDATE SET
                generated_at = EXCLUDED.generated_at,
                payload = EXCLUDED.payload
            """.formatted(tableName);
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, accountId);
            statement.setLong(2, generatedAt);
            statement.setString(3, OBJECT_MAPPER.writeValueAsString(payload));
            statement.executeUpdate();
        } catch (Exception exception) {
            throw new IllegalStateException("failed_to_upsert_business_package:" + tableName + ":" + accountId, exception);
        }
    }

    private void reset() {
        try (
            Connection connection = connectionFactory.openConnection();
            PreparedStatement executionDelete = connection.prepareStatement("DELETE FROM bo_execution_packages");
            PreparedStatement postTradeDelete = connection.prepareStatement("DELETE FROM bo_post_trade_packages");
            PreparedStatement parentExecutionDelete = connection.prepareStatement("DELETE FROM bo_parent_execution_states");
            PreparedStatement allocationDelete = connection.prepareStatement("DELETE FROM bo_allocation_states");
            PreparedStatement settlementDelete = connection.prepareStatement("DELETE FROM bo_settlement_projections");
            PreparedStatement statementDelete = connection.prepareStatement("DELETE FROM bo_statement_projections");
            PreparedStatement riskDelete = connection.prepareStatement("DELETE FROM bo_risk_snapshots");
            PreparedStatement settlementExceptionDelete = connection.prepareStatement("DELETE FROM bo_settlement_exception_workflows");
            PreparedStatement corporateActionDelete = connection.prepareStatement("DELETE FROM bo_corporate_action_workflows");
            PreparedStatement marginProjectionDelete = connection.prepareStatement("DELETE FROM bo_margin_projections");
            PreparedStatement scenarioHistoryDelete = connection.prepareStatement("DELETE FROM bo_scenario_evaluation_histories");
            PreparedStatement backtestHistoryDelete = connection.prepareStatement("DELETE FROM bo_backtest_histories");
            PreparedStatement accountHierarchyDelete = connection.prepareStatement("DELETE FROM bo_account_hierarchies");
            PreparedStatement operatorControlDelete = connection.prepareStatement("DELETE FROM bo_operator_control_states")
        ) {
            executionDelete.executeUpdate();
            postTradeDelete.executeUpdate();
            parentExecutionDelete.executeUpdate();
            allocationDelete.executeUpdate();
            settlementDelete.executeUpdate();
            statementDelete.executeUpdate();
            riskDelete.executeUpdate();
            settlementExceptionDelete.executeUpdate();
            corporateActionDelete.executeUpdate();
            marginProjectionDelete.executeUpdate();
            scenarioHistoryDelete.executeUpdate();
            backtestHistoryDelete.executeUpdate();
            accountHierarchyDelete.executeUpdate();
            operatorControlDelete.executeUpdate();
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

    private record ParentExecutionStateAdapter(JdbcBusinessPackageStore store) implements ParentExecutionStateReadModel {
        @Override
        public Optional<ParentExecutionStateView> findByOrderId(String orderId) {
            return store.loadParentExecutionState(orderId);
        }

        @Override
        public void upsert(ParentExecutionStateView view) {
            store.upsertParentExecutionState(view);
        }

        @Override
        public void reset() {
            store.reset();
        }
    }

    private record AllocationStateAdapter(JdbcBusinessPackageStore store) implements AllocationStateReadModel {
        @Override
        public Optional<AllocationStateView> findByOrderId(String orderId) {
            return store.loadAllocationState(orderId);
        }

        @Override
        public void upsert(AllocationStateView view) {
            store.upsertAllocationState(view);
        }

        @Override
        public void reset() {
            store.reset();
        }
    }

    private record SettlementProjectionAdapter(JdbcBusinessPackageStore store) implements SettlementProjectionReadModel {
        @Override
        public Optional<SettlementProjectionView> findByOrderId(String orderId) {
            return store.loadSettlementProjection(orderId);
        }

        @Override
        public void upsert(SettlementProjectionView view) {
            store.upsertSettlementProjection(view);
        }

        @Override
        public void reset() {
            store.reset();
        }
    }

    private record StatementProjectionAdapter(JdbcBusinessPackageStore store) implements StatementProjectionReadModel {
        @Override
        public Optional<StatementProjectionView> findByOrderId(String orderId) {
            return store.loadStatementProjection(orderId);
        }

        @Override
        public void upsert(StatementProjectionView view) {
            store.upsertStatementProjection(view);
        }

        @Override
        public void reset() {
            store.reset();
        }
    }

    private record RiskSnapshotAdapter(JdbcBusinessPackageStore store) implements RiskSnapshotReadModel {
        @Override
        public Optional<RiskSnapshotView> findByAccountId(String accountId) {
            return store.loadRiskSnapshot(accountId);
        }

        @Override
        public void upsert(RiskSnapshotView view) {
            store.upsertRiskSnapshot(view);
        }

        @Override
        public void reset() {
            store.reset();
        }
    }

    private record SettlementExceptionWorkflowAdapter(JdbcBusinessPackageStore store) implements SettlementExceptionWorkflowReadModel {
        @Override
        public Optional<SettlementExceptionWorkflowView> findByOrderId(String orderId) {
            return store.loadSettlementExceptionWorkflow(orderId);
        }

        @Override
        public void upsert(SettlementExceptionWorkflowView view) {
            store.upsertSettlementExceptionWorkflow(view);
        }

        @Override
        public void reset() {
            store.reset();
        }
    }

    private record CorporateActionWorkflowAdapter(JdbcBusinessPackageStore store) implements CorporateActionWorkflowReadModel {
        @Override
        public Optional<CorporateActionWorkflowView> findByOrderId(String orderId) {
            return store.loadCorporateActionWorkflow(orderId);
        }

        @Override
        public void upsert(CorporateActionWorkflowView view) {
            store.upsertCorporateActionWorkflow(view);
        }

        @Override
        public void reset() {
            store.reset();
        }
    }

    private record MarginProjectionAdapter(JdbcBusinessPackageStore store) implements MarginProjectionReadModel {
        @Override
        public Optional<MarginProjectionView> findByAccountId(String accountId) {
            return store.loadMarginProjection(accountId);
        }

        @Override
        public void upsert(MarginProjectionView view) {
            store.upsertMarginProjection(view);
        }

        @Override
        public void reset() {
            store.reset();
        }
    }

    private record ScenarioEvaluationHistoryAdapter(JdbcBusinessPackageStore store) implements ScenarioEvaluationHistoryReadModel {
        @Override
        public Optional<ScenarioEvaluationHistoryView> findByAccountId(String accountId) {
            return store.loadScenarioEvaluationHistory(accountId);
        }

        @Override
        public void upsert(ScenarioEvaluationHistoryView view) {
            store.upsertScenarioEvaluationHistory(view);
        }

        @Override
        public void reset() {
            store.reset();
        }
    }

    private record BacktestHistoryAdapter(JdbcBusinessPackageStore store) implements BacktestHistoryReadModel {
        @Override
        public Optional<BacktestHistoryView> findByAccountId(String accountId) {
            return store.loadBacktestHistory(accountId);
        }

        @Override
        public void upsert(BacktestHistoryView view) {
            store.upsertBacktestHistory(view);
        }

        @Override
        public void reset() {
            store.reset();
        }
    }

    private record AccountHierarchyAdapter(JdbcBusinessPackageStore store) implements AccountHierarchyReadModel {
        @Override
        public Optional<AccountHierarchyView> findByAccountId(String accountId) {
            return store.loadAccountHierarchy(accountId);
        }

        @Override
        public void upsert(AccountHierarchyView view) {
            store.upsertAccountHierarchy(view);
        }

        @Override
        public void reset() {
            store.reset();
        }
    }

    private record OperatorControlStateAdapter(JdbcBusinessPackageStore store) implements OperatorControlStateReadModel {
        @Override
        public Optional<OperatorControlStateView> findByOrderId(String orderId) {
            return store.loadOperatorControlState(orderId);
        }

        @Override
        public void upsert(OperatorControlStateView view) {
            store.upsertOperatorControlState(view);
        }

        @Override
        public void reset() {
            store.reset();
        }
    }
}
