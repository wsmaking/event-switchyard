package oms.persistence;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public final class JdbcConnectionFactory {
    private final String jdbcUrl;
    private final String user;
    private final String password;

    public JdbcConnectionFactory(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException exception) {
            throw new IllegalStateException("postgres_driver_not_available", exception);
        }
    }

    public static JdbcConnectionFactory fromEnvironment(String prefix, String defaultDbName) {
        String upper = prefix.toUpperCase();
        String url = System.getenv().getOrDefault(upper + "_DB_URL", "jdbc:postgresql://localhost:5432/" + defaultDbName);
        String user = System.getenv().getOrDefault(upper + "_DB_USER", defaultDbName);
        String password = System.getenv().getOrDefault(upper + "_DB_PASSWORD", defaultDbName);
        return new JdbcConnectionFactory(url, user, password);
    }

    public Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }

    public String jdbcUrl() {
        return jdbcUrl;
    }
}
