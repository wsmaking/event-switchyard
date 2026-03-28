package backofficejava.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public final class SqlMigrationRunner {
    private SqlMigrationRunner() {
    }

    public static void run(JdbcConnectionFactory connectionFactory, String... resources) {
        try (Connection connection = connectionFactory.openConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                for (String resource : resources) {
                    for (String sql : loadStatements(resource)) {
                        statement.execute(sql);
                    }
                }
            }
            connection.commit();
        } catch (SQLException exception) {
            throw new IllegalStateException("failed_to_run_backoffice_sql_migrations", exception);
        }
    }

    static List<String> loadStatements(String resource) {
        try (InputStream inputStream = SqlMigrationRunner.class.getClassLoader().getResourceAsStream(resource)) {
            if (inputStream == null) {
                throw new IllegalStateException("migration_not_found:" + resource);
            }
            String sql = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            List<String> statements = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            for (String line : sql.split("\\R")) {
                String trimmed = line.trim();
                if (trimmed.startsWith("--")) {
                    continue;
                }
                current.append(line).append('\n');
                if (trimmed.endsWith(";")) {
                    String statement = current.toString().trim();
                    if (!statement.isBlank()) {
                        statements.add(statement.substring(0, statement.length() - 1));
                    }
                    current.setLength(0);
                }
            }
            String tail = current.toString().trim();
            if (!tail.isBlank()) {
                statements.add(tail);
            }
            return statements;
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_read_backoffice_migration:" + resource, exception);
        }
    }
}
