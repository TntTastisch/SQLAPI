package de.tnttastisch.helpers;

import com.zaxxer.hikari.pool.*;
import de.tnttastisch.IPoolProvider;
import org.slf4j.*;

import java.sql.*;
import java.util.concurrent.*;

public class ConnectionAuthenticator {

    private final Logger logger;
    private final HikariPool pool;
    private final ExecutorService service = Executors.newCachedThreadPool();
    private final DatabaseCommand command;

    /**
     * Constructs an SQL instance with the provided logger and connection pool provider.
     * @param logger   The logger instance to log errors and messages.
     * @param provider The pool provider for creating the connection pool.
     * @throws SQLException         If a database access error occurs.
     * @throws InterruptedException If a thread is interrupted while waiting.
     */
    public ConnectionAuthenticator(final Logger logger, final IPoolProvider provider) throws SQLException, InterruptedException {
        this.logger = logger;
        this.pool = provider.createPool();
        this.command = new DatabaseCommand(getConnection(), logger, service);

        try (Connection connection = pool.getConnection(15000)) {
            final PreparedStatement statement = connection.prepareStatement("/* ping */ SELECT 1");
            statement.setQueryTimeout(15);
            statement.executeQuery();
        } catch (final SQLException exp) {
            shutdown();
            getLogger().error("Connection test failed", exp);
        }
    }

    /**
     * Retrieves a connection from the connection pool.
     * @return The obtained Connection.
     * @throws SQLException If a database access error occurs.
     */
    public Connection getConnection() throws SQLException {
        return getPool().getConnection();
    }

    /**
     * Gets the Hikari connection pool associated with this SQL instance.
     * @return The HikariPool instance.
     */
    public HikariPool getPool() {
        return pool;
    }

    /**
     * Shuts down the connection pool associated with this SQL instance.
     * @throws SQLException If a database access error occurs.
     * @throws InterruptedException If a thread is interrupted while waiting.
     */
    public void shutdown() throws SQLException, InterruptedException {
        if (getConnection() != null) getPool().shutdown();
    }

    /**
     * @return the command class
     */
    public DatabaseCommand getCommand() {
        return command;
    }

    private Logger getLogger() {
        return logger;
    }
}
