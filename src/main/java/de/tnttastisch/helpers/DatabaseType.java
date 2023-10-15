package de.tnttastisch.helpers;

public enum DatabaseType {

    SQLITE,
    MYSQL,
    MARIADB;

    public DatabaseType type;

    /**
     * Retrieves the current database type.
     * @return The current DatabaseType.
     */
    public DatabaseType getType() {
        return type;
    }
}
