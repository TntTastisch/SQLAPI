package de.tnttastisch.helpers;

public enum DatabaseType {

    SQLITE,
    MYSQL;

    public DatabaseType type;

    /**
     * Gets the current database type
     * @return The current DatabaseType.
     */
    public DatabaseType getType() {
        return type;
    }

    /**
     * Sets the database type
     */
    public void setType(DatabaseType type) {
        this.type = type;
    }
}
