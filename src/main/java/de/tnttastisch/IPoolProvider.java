package de.tnttastisch;

import com.zaxxer.hikari.pool.*;

/**
 * The IPoolProvider interface provides a method for creating a Hikari connection pool.
 * @since = 1.0-RELEASE
 */
public interface IPoolProvider {

    /**
     * Creates a Hikari connection pool.
     * @return The HikariPool instance representing the created connection pool.
     */
    HikariPool createPool();
}
