package app.strategy

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

class StrategyConfigStore(
    jdbcUrl: String = System.getenv("APP_DB_URL") ?: "jdbc:postgresql://localhost:5432/backoffice",
    user: String = System.getenv("APP_DB_USER") ?: "backoffice",
    password: String = System.getenv("APP_DB_PASSWORD") ?: "backoffice",
    maxPoolSize: Int = (System.getenv("APP_DB_POOL") ?: "4").toInt()
) {
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
                poolName = "app-strategy-db"
            }
        dataSource = HikariDataSource(config)
        ensureSchema()
    }

    fun load(accountId: String): StrategyConfig? {
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                """
                SELECT account_id, enabled, symbols, tick_ms, max_orders_per_min, cooldown_ms, updated_at_ms
                FROM app.strategy_configs
                WHERE account_id = ?
                """.trimIndent()
            ).use { stmt ->
                stmt.setString(1, accountId)
                stmt.executeQuery().use { rs ->
                    if (!rs.next()) return null
                    return StrategyConfig(
                        accountId = rs.getString("account_id"),
                        enabled = rs.getBoolean("enabled"),
                        symbols = rs.getString("symbols").split(',').map { it.trim() }.filter { it.isNotEmpty() },
                        tickMs = rs.getLong("tick_ms"),
                        maxOrdersPerMin = rs.getInt("max_orders_per_min"),
                        cooldownMs = rs.getLong("cooldown_ms"),
                        updatedAtMs = rs.getLong("updated_at_ms")
                    )
                }
            }
        }
    }

    fun upsert(config: StrategyConfig) {
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                """
                INSERT INTO app.strategy_configs
                    (account_id, enabled, symbols, tick_ms, max_orders_per_min, cooldown_ms, updated_at_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (account_id) DO UPDATE SET
                    enabled = EXCLUDED.enabled,
                    symbols = EXCLUDED.symbols,
                    tick_ms = EXCLUDED.tick_ms,
                    max_orders_per_min = EXCLUDED.max_orders_per_min,
                    cooldown_ms = EXCLUDED.cooldown_ms,
                    updated_at_ms = EXCLUDED.updated_at_ms
                """.trimIndent()
            ).use { stmt ->
                stmt.setString(1, config.accountId)
                stmt.setBoolean(2, config.enabled)
                stmt.setString(3, config.symbolsCsv())
                stmt.setLong(4, config.tickMs)
                stmt.setInt(5, config.maxOrdersPerMin)
                stmt.setLong(6, config.cooldownMs)
                stmt.setLong(7, config.updatedAtMs)
                stmt.executeUpdate()
            }
        }
    }

    private fun ensureSchema() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE SCHEMA IF NOT EXISTS app")
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS app.strategy_configs (
                        account_id TEXT PRIMARY KEY,
                        enabled BOOLEAN NOT NULL,
                        symbols TEXT NOT NULL,
                        tick_ms BIGINT NOT NULL,
                        max_orders_per_min INTEGER NOT NULL,
                        cooldown_ms BIGINT NOT NULL,
                        updated_at_ms BIGINT NOT NULL
                    )
                    """.trimIndent()
                )
            }
        }
    }
}
