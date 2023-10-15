package de.tnttastisch.helpers;

import org.slf4j.Logger;

import java.sql.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Since 2.0-RELEASE
 */
public class DatabaseCommand {

    private Connection connection;
    private Logger logger;
    private ExecutorService service;

    public DatabaseCommand(Connection connection, Logger logger, ExecutorService service) {
        this.connection = connection;
        this.logger = logger;
        this.service = service;
    }

    /**
     * Executes a SQL query asynchronously.
     * @param query The SQL query to execute.
     * @param arguments The arguments to be set in the prepared statement.
     * @return A CompletableFuture containing the query result as a ResultSet.
     */
    public CompletableFuture<ResultSet> query(String query, Object... arguments) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = this.connection) {
                PreparedStatement statement = connection.prepareStatement(query);
                setArgs(arguments, statement);
                ResultSet result = statement.executeQuery();
                if (!(result.next())) return null;
                return result;
            } catch (SQLException e) {
                this.logger.error("A Sql error occurred while catching a query: ", e);
                return null;
            }
        }, this.service);
    }

    /**
     * Executes a SQL query asynchronously.
     * @param query The SQL query to execute.
     * @return A CompletableFuture containing the query result as a ResultSet.
     */
    public CompletableFuture<ResultSet> query(String query) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = this.connection) {
                PreparedStatement statement = connection.prepareStatement(query);
                ResultSet result = statement.executeQuery();
                if (!(result.next())) return null;
                return result;
            } catch (SQLException e) {
                this.logger.error("A Sql error occurred while catching a query (single): ", e);
                return null;
            }
        }, this.service);
    }

    /**
     * Executes a SQL update asynchronously.
     * @param update The SQL update statement to execute.
     * @param arguments The arguments to be set in the prepared statement.
     * @return A CompletableFuture representing the completion of the update operation.
     */
    public CompletableFuture<Void> update(String update, Object... arguments) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = this.connection) {
                PreparedStatement statement = connection.prepareStatement(update);
                setArgs(arguments, statement);
                statement.executeUpdate();
                return null;
            } catch (SQLException e) {
                this.logger.error("A Sql error occurred while execute an update: ", e);
                return null;
            }
        }, this.service);
    }

    /**
     * Executes a SQL update asynchronously.
     * @param update The SQL update statement to execute.
     * @return A CompletableFuture representing the completion of the update operation.
     */
    public CompletableFuture<Void> update(String update) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = this.connection) {
                PreparedStatement statement = connection.prepareStatement(update);
                statement.executeUpdate();
                return null;
            } catch (SQLException e) {
                this.logger.error("A Sql error occurred while execute an update (single): ", e);
                return null;
            }
        }, this.service);
    }


    /**
     * Executes a large SQL update asynchronously.
     * @param update The SQL update statement to execute.
     * @param arguments The arguments to be set in the prepared statement.
     * @return A CompletableFuture representing the completion of the large update operation.
     */
    public CompletableFuture<Void> largeUpdate(String update, Object... arguments) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = this.connection) {
                PreparedStatement statement = connection.prepareStatement(update);
                setArgs(arguments, statement);
                statement.executeLargeUpdate();
                return null;
            } catch (SQLException e) {
                this.logger.error("A Sql error occurred while execute a large update: ", e);
                return null;
            }
        }, this.service);
    }

    /**
     * Executes a large SQL update asynchronously.
     * @param update The SQL update statement to execute.
     * @return A CompletableFuture representing the completion of the large update operation.
     */
    public CompletableFuture<Void> largeUpdate(String update) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = this.connection) {
                PreparedStatement statement = connection.prepareStatement(update);
                statement.executeLargeUpdate();
                return null;
            } catch (SQLException e) {
                this.logger.error("A Sql error occurred while execute a large update (single): ", e);
                return null;
            }
        }, this.service);
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
