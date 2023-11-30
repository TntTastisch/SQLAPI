package de.tnttastisch.migration;

import de.tnttastisch.SQLFactory;

import java.sql.*;

/**
 * Migration class handles the migration process for a specific table in the database.
 * @since = 2.0-RELEASE
 */
public class Migration {

    private final String table;
    private final String tableName;
    private final String outdatedTable;

    /**
     * Constructs a new Migration object for the specified table.
     * @param tableName   The name of the table.
     * @param old_table   The old version of the table.
     * @param new_table   The new version of the table.
     */
    public Migration(String tableName, String old_table, String new_table) {
        this.tableName = tableName;
        if(hasToMigrate(old_table, new_table)) {
            this.table = new_table;
            this.outdatedTable = old_table;
            return;
        }
        outdatedTable = old_table;
        table = old_table;
    }

    /**
     * Executes the migration process for the specified connection.
     * @param connection The connection to the database.
     * @throws SQLException if a database access error occurs or the method is called on a closed connection.
     */
    public void migrate(Connection connection) throws SQLException {
        if (this.outdatedTable.equals("")) return;
        if (!hasToMigrate(table, outdatedTable)) {
            System.out.println("[Migration] There are no Changes!");
            return;
        }
        System.out.println("[Migration] Creating legacy Table!");
        String createLegacyQuery = "CREATE TABLE IF NOT EXISTS legacy_" + tableName + "(" + outdatedTable + ")";
        PreparedStatement statement = connection.prepareStatement(createLegacyQuery);
        statement.executeUpdate();

        System.out.println("[Migration] Inserting legacy Table!");
        String copyLegacyDataQuery = "INSERT INTO legacy_" + tableName + " SELECT * FROM " + tableName;
        statement = connection.prepareStatement(copyLegacyDataQuery);
        statement.execute();

        System.out.println("[Migration] Drop old Table!");
        String dropOldTableQuery = "DROP TABLE " + tableName;
        statement = connection.prepareStatement(dropOldTableQuery);
        statement.executeUpdate();

        System.out.println("[Migration] Creating new Table!");
        String createNewTableQuery = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + table + ")";
        statement = connection.prepareStatement(createNewTableQuery);
        statement.executeUpdate();

        System.out.println("[Migration] Migrating!");
        String selectQuery = "SELECT * FROM legacy_" + tableName;
        PreparedStatement selectStatement = connection.prepareStatement(selectQuery);
        ResultSet set = selectStatement.executeQuery();

        ResultSetMetaData metaData = set.getMetaData();
        int columnCount = metaData.getColumnCount();

        StringBuilder builder = new StringBuilder("INSERT INTO " + tableName + "(");
        for(int i = 1; i <= columnCount; i++) {
            if(i > 1) {
                builder.append(", ");
            }
            builder.append(metaData.getColumnName(i));
        }
        builder.append(") VALUES (");
        for(int i = 1; i <= columnCount; i++) {
            if(i > 1) {
                builder.append(", ");
            }
            builder.append("?");
        }
        builder.append(")");
        statement = connection.prepareStatement(builder.toString());
        while(set.next()) {
            for (int i = 1; i <= columnCount; i++) {
                Object value = set.getObject(i);
                statement.setObject(i, value);
            }
            statement.executeUpdate();
        }
        statement.execute();

        System.out.println("[Migration] Drop legacy Table!");
        String dropLegacyTableQuery = "DROP TABLE legacy_" + tableName;
        statement = connection.prepareStatement(dropLegacyTableQuery);
        statement.executeUpdate();
        System.out.println("[Migration] Migration Succeed!");
    }

    private boolean hasToMigrate(String t1, String t2) {
        return !t1.equals(t2);
    }

    /**
     * Retrieves the table name associated with the migration.
     * This method is not really necessary for users!
     * @return The name of the table.
     */
    public String getTable() {
        return table;
    }
}

