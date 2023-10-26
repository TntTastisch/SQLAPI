package de.tnttastisch;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;
import de.tnttastisch.helpers.ConnectionAuthenticator;
import de.tnttastisch.helpers.DatabaseCommand;
import de.tnttastisch.helpers.DatabaseType;
import de.tnttastisch.migration.Migration;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @since = 2.0-RELEASE
 */
public class SQLFactory {

    private Logger logger;
    private ArrayList<Migration> migrations;
    private ConnectionAuthenticator connectionAuthenticator;
    private String migrationPrefix = "[Migration] ";

    /**
     * Initializes an instance of SQLFactory with the provided logger.
     * @param logger The logger to be used for logging events and messages.
     */
    public SQLFactory(Logger logger) {
        this.logger = logger;
    }

    private Logger getLogger() {
        return logger;
    }

    /**
     * Creates a database connection with the provided parameters.
     * @param maximumPoolSize The maximum number of connections in the connection pool.
     * @param minIdle         The minimum number of idle connections in the connection pool.
     * @param arguments       Additional arguments required for establishing the database connection.
     * @return The ConnectionAuthenticator for the created database connection.
     */
    public ConnectionAuthenticator createDatabaseConnection(int maximumPoolSize, int minIdle, String... arguments) {
        return connect(maximumPoolSize, minIdle, arguments);
    }

    /**
     * Creates a database connection with the provided parameters.
     * @param arguments       Additional arguments required for establishing the database connection.
     * @return The ConnectionAuthenticator for the created database connection.
     */
    public ConnectionAuthenticator createDatabaseConnection(String... arguments) {
        return connect(8, 1, arguments);
    }

    /**
     * Retrieves the active database connection if available.
     * @return The active database connection if available; otherwise, null.
     */
    public ConnectionAuthenticator getDatabaseConnection() {
        if (this.connectionAuthenticator == null) return null;
        return this.connectionAuthenticator;
    }

    /**
     * Creates a new migration instance and adds it to the list of migrations.
     * @param tableName The name of the table for the migration.
     * @param old_table The old version of the table.
     * @param new_table The new version of the table.
     * @return The created Migration object.
     */
    public Migration createMigration(String tableName, String old_table, String new_table) {
        Migration migration = new Migration(tableName, old_table, new_table);
        this.migrations.add(migration);
        return migration;
    }

    /**
     * Executes the migration process for all stored migration instances.
     * @param connection The connection to the database for executing the migrations.
     */
    public void migrate(Connection connection) {
        if(migrations.isEmpty()) {
            getLogger().warn(migrationPrefix + "There is nothing to Migrate!");
            return;
        }
        this.migrations.forEach(migration -> {
            try {
                migration.migrate(connection);
            } catch (SQLException e) {
                getLogger().error(String.format(migrationPrefix + "An error occurred while trying to migrate table %s: ", migration.getTable()), e);
            }
        });
    }

    private ConnectionAuthenticator connect(int maximumPoolSize, int minIdle, String... arguments) {
        try {
            switch (DatabaseType.getType()) {
                case MYSQL:
                    this.connectionAuthenticator = new ConnectionAuthenticator(getLogger(), () -> {
                        HikariConfig conf = new HikariConfig();
                        conf.setConnectionTimeout(7500);
                        conf.setMaximumPoolSize(maximumPoolSize);
                        conf.setMinimumIdle(minIdle);
                        conf.setDriverClassName("com.mysql.jdbc.Driver");
                        if (arguments[5] == null) {
                            conf.setJdbcUrl(String.format("jdbc:mysql://%s:%s/%s", arguments[0], (Integer.parseInt(arguments[1]) == 0 ? "3306" : arguments[1]), arguments[2]));
                        } else {
                            conf.setJdbcUrl(String.format("jdbc:mysql://%s:%s/%s?%s", arguments[0], (Integer.parseInt(arguments[1]) == 0 ? "3306" : arguments[1]), arguments[2], arguments[5]));
                        }
                        conf.setUsername(arguments[3]);
                        conf.setPassword(arguments[4]);
                        return new HikariPool(conf);
                    });
                    break;
                case SQLITE:
                    this.connectionAuthenticator = new ConnectionAuthenticator(getLogger(), () -> {
                        HikariConfig conf = new HikariConfig();
                        conf.setPoolName("SQLiteConnectionPool");
                        conf.setDriverClassName("org.sqlite.JDBC");
                        conf.setConnectionTimeout(7500);
                        File confDb;
                        if(arguments[1] != null) {
                            confDb = new File(arguments[0], arguments[1]);
                        } else {
                            confDb = new File(arguments[0]);
                        }
                        if (!confDb.exists()) {
                            try {
                                confDb.createNewFile();
                            } catch (IOException e) {
                                getLogger().error("An error occurred while trying to create sqlite database file", e);
                                return null;
                            }
                        }
                        conf.setJdbcUrl("jdbc:sqlite://" + confDb.getAbsolutePath());
                        return new HikariPool(conf);
                    });
                    break;
                default:
                    getLogger().error("There was no driver found with this name");
                    break;
            }
            return this.connectionAuthenticator;
        } catch (SQLException | InterruptedException e) {
            getLogger().error("An error occurred while trying to connect to database: ", e);
            return null;
        }
    }
}
