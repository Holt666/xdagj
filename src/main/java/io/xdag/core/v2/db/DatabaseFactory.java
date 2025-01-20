package io.xdag.core.v2.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public interface DatabaseFactory {
    /**
     * Returns a KVDB instance for the specified database.
     *
     * @param name
     * @return
     */
    Database getDB(DatabaseName name);

    /**
     * Close all opened resources.
     */
    void close();

    /**
     * Returns the data directory of created databases.
     *
     * @return
     */
    Path getDataDir();

    /**
     * @param path
     *            the destination path.
     */
    default void moveTo(Path path) throws IOException {
        Files.move(getDataDir(), path, REPLACE_EXISTING, ATOMIC_MOVE);
    }
}

