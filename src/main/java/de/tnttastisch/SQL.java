package de.tnttastisch;

import com.zaxxer.hikari.pool.*;
import org.slf4j.*;
import sun.security.util.ByteArrays;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.*;
import java.util.Arrays;
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

            switch (o.getClass().getTypeName().toLowerCase()) {
                case "string": {
                    statement.setString(i+1, String.valueOf(o));
                    break;
                }
                case "integer": {
                    statement.setInt(i+1, Integer.parseInt(String.valueOf(o)));
                    break;
                }
                case "boolean": {
                    statement.setBoolean(i+1, Boolean.parseBoolean(String.valueOf(o)));
                    break;
                }
                case "long": {
                    statement.setLong(i+1, Long.parseLong(String.valueOf(o)));
                    break;
                }
                case "byte": {
                    statement.setByte(i+1, Byte.parseByte(String.valueOf(o)));
                    break;
                }
                case "double": {
                    statement.setDouble(i+1, Double.parseDouble(String.valueOf(o)));
                    break;
                }
                case "float": {
                    statement.setFloat(i+1, Float.parseFloat(String.valueOf(o )));
                    break;
                }
                case "array": {
                    statement.setArray(i+1, (Array) Arrays.asList(String.valueOf(o)));
                    break;
                }
                case "date": {
                    statement.setDate(i+1, Date.valueOf(String.valueOf(o)));
                    break;
                }
                case "time": {
                    statement.setTime(i+1, Time.valueOf(String.valueOf(o)));
                    break;
                }
                case "timestamp": {
                    statement.setTimestamp(i+1, Timestamp.valueOf(String.valueOf(o)));
                    break;
                }
                default: {
                    statement.setObject(i+1, o);
                }
            }
        }
    }
}
