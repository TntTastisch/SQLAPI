package de.tnttastisch.helpers;

public enum DatabaseType {

    SQLITE,
    MYSQL;

    private static DatabaseType type;

    /**
     * Gets the current database type
     * @return The current DatabaseType.
     */
    public static DatabaseType getType() {
        return type;
    }

    /**
     * Sets the database type
     */
    public static void setType(DatabaseType type) {
        DatabaseType.type = type;
    }
}
