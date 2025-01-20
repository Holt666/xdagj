package io.xdag.core.v2;

import io.xdag.utils.BytesUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a block header data.
 */
@Slf4j
public class BlockHeaderData {

    public static final int MAX_SIZE = 32;

    private final byte[] raw;

    public BlockHeaderData() {
        this(BytesUtils.EMPTY_BYTES);
    }

    public BlockHeaderData(byte[] raw) {
        if (raw == null || raw.length > MAX_SIZE) {
            throw new IllegalArgumentException("Invalid header data");
        }
        this.raw = raw.clone();
    }

    public byte[] toBytes() {
        return this.raw.clone();
    }
}
