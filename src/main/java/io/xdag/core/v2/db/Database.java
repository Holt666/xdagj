package io.xdag.core.v2.db;

import org.apache.commons.lang3.tuple.Pair;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public interface Database {

    /**
     * Returns the value that is mapped to the specified key.
     *
     * @param key
     * @return
     */
    byte[] get(byte[] key);

    /**
     * Associates a value to the specified key.
     *
     *
     *
     * @param key
     * @param value
     *            can not be null
     */
    void put(byte[] key, byte[] value);

    /**
     * Deletes the specified key value pair if present.
     *
     *
     * @param key
     */
    void delete(byte[] key);

    /**
     * Updates a list of key value pairs.
     *
     * @param pairs
     *            key value pairs; pair with null value, will be deleted
     */
    void updateBatch(List<Pair<byte[], byte[]>> pairs);

    /**
     * Returns all the keys.<br>
     * <br>
     * NOTE: be sure to close the iterator after iteration.
     *
     * @return
     */
    ClosableIterator<Map.Entry<byte[], byte[]>> iterator();

    /**
     * Returns all the keys which has the given prefix.<br>
     * <br>
     * NOTE: be sure to close the iterator after iteration.
     *
     * @return
     */
    ClosableIterator<Map.Entry<byte[], byte[]>> iterator(byte[] prefix);

    /**
     * Closes the database.
     */
    void close();

    /**
     * Destroys this DB.
     *
     */
    void destroy();

    /**
     * Returns the data directory of this database.
     *
     * @return
     */
    Path getDataDir();
}
