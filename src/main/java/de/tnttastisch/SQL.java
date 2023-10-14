package de.tnttastisch;

import com.zaxxer.hikari.pool.*;
import org.slf4j.*;

import java.sql.*;
import java.util.concurrent.*;

/**
 * The SQL class provides an interface to execute SQL queries and updates asynchronously using JDBC.
 */
public class SQL {

    private final Logger logger;
    private final IPoolProvider provider;
    private final HikariPool pool;
    private final ExecutorService service = Executors.newCachedThreadPool();

    /**
     * Constructs an SQL instance with the provided logger and connection pool provider.
     * @param logger The logger instance to log errors and messages.
     * @param provider The pool provider for creating the connection pool.
     * @throws SQLException If a database access error occurs.
     * @throws InterruptedException If a thread is interrupted while waiting.
     */
    public SQL(final Logger logger, final IPoolProvider provider) throws SQLException, InterruptedException {
        this.logger = logger;
        this.provider = provider;
        this.pool = provider.createPool();

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
     * Gets the logger instance associated with this SQL instance.
     * @return The Logger instance.
     */
    public Logger getLogger() {
        return logger;
    }

    /**
     * Gets the ExecutorService instance associated with this SQL instance.
     * @return The ExecutorService instance.
     */
    public ExecutorService getService() {
        return service;
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
     * Executes a SQL query asynchronously.
     * @param query The SQL query to execute.
     * @param arguments The arguments to be set in the prepared statement.
     * @return A CompletableFuture containing the query result as a ResultSet.
     */
    public CompletableFuture<ResultSet> query(String query, Object... arguments) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection()) {
                PreparedStatement statement = connection.prepareStatement(query);
                setArgs(arguments, statement);
                ResultSet result = statement.executeQuery();
                if (!(result.next())) return null;
                return result;
            } catch (SQLException e) {
                getLogger().error("A Sql error occurred while catching a query: ", e);
                return null;
            }
        }, getService());
    }

    /**
     * Executes a SQL update asynchronously.
     * @param update The SQL update statement to execute.
     * @param arguments The arguments to be set in the prepared statement.
     * @return A CompletableFuture representing the completion of the update operation.
     */
    public CompletableFuture<Void> update(String update, Object... arguments) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection()) {
                PreparedStatement statement = connection.prepareStatement(update);
                setArgs(arguments, statement);
                statement.executeUpdate();
                return null;
            } catch (SQLException e) {
                getLogger().error("A Sql error occurred while execute an update: ", e);
                return null;
            }
        }, getService());
    }

    /**
     * Executes a SQL update asynchronously.
     * @param update The SQL update statement to execute.
     * @return A CompletableFuture representing the completion of the update operation.
     */
    public CompletableFuture<Void> update(String update) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection()) {
                PreparedStatement statement = connection.prepareStatement(update);
                statement.executeUpdate();
                return null;
            } catch (SQLException e) {
                getLogger().error("A Sql error occurred while execute an update (single): ", e);
                return null;
            }
        }, getService());
    }


    /**
     * Executes a large SQL update asynchronously.
     * @param update The SQL update statement to execute.
     * @param arguments The arguments to be set in the prepared statement.
     * @return A CompletableFuture representing the completion of the large update operation.
     */
    public CompletableFuture<Void> largeUpdate(String update, Object... arguments) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection()) {
                PreparedStatement statement = connection.prepareStatement(update);
                setArgs(arguments, statement);
                statement.executeLargeUpdate();
                return null;
            } catch (SQLException e) {
                getLogger().error("A Sql error occurred while execute a large update: ", e);
                return null;
            }
        }, getService());
    }

    /**
     * Executes a large SQL update asynchronously.
     * @param update The SQL update statement to execute.
     * @return A CompletableFuture representing the completion of the large update operation.
     */
    public CompletableFuture<Void> largeUpdate(String update) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection()) {
                PreparedStatement statement = connection.prepareStatement(update);
                statement.executeLargeUpdate();
                return null;
            } catch (SQLException e) {
                getLogger().error("A Sql error occurred while execute a large update: ", e);
                return null;
            }
        }, getService());
    }

    private void setArgs(Object[] args, PreparedStatement statement) throws SQLException {
        for (int i = 0; i != args.length; i++) {
            Object o = args[i];
            int arg = i+1;

            if (o instanceof String) {
                statement.setString(arg, String.valueOf(o));
                continue;
            }

            if (o instanceof Integer) {
                statement.setInt(arg, Integer.parseInt(String.valueOf(o)));
                continue;
            }

            if (o instanceof Long) {
                statement.setLong(arg, Long.parseLong(String.valueOf(o)));
                continue;
            }

            if (o instanceof Double) {
                statement.setDouble(arg, Double.parseDouble(String.valueOf(o)));
                continue;
            }

            if (o instanceof Float) {
                statement.setFloat(arg, Float.parseFloat(String.valueOf(o)));
                continue;
            }

            if (o instanceof Boolean) {
                statement.setBoolean(arg, Boolean.parseBoolean(String.valueOf(o)));
                continue;
            }

            if (o instanceof Byte) {
                statement.setByte(arg, Byte.parseByte(String.valueOf(o)));
                continue;
            }

            if(o instanceof Date) {
                statement.setDate(arg, Date.valueOf(String.valueOf(o)));
                continue;
            }

            if (o instanceof Time) {
                statement.setTime(arg, Time.valueOf(String.valueOf(o)));
                continue;
            }

            if (o instanceof Timestamp) {
                statement.setTimestamp(arg, Timestamp.valueOf(String.valueOf(o)));
                continue;
            }

            // Check
            if (o instanceof Array) {
                statement.setArray(arg, (Array) o);
                continue;
            }

            if (o instanceof Byte[]) {
                statement.setBytes(arg, (byte[]) o);
                continue;
            }

            if (o != null) {
                statement.setObject(arg, o);
            }
        }
    }
}
