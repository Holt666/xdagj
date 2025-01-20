package io.xdag.core.v2;

import java.util.ArrayList;
import java.util.List;

public enum BlockPart {

    HEADER(1), TRANSACTIONS(1 << 1), RESULTS(1 << 2);

    private final int code;

    BlockPart(int code) {
        this.code = code;
    }

    public static int encode(BlockPart... parts) {
        int result = 0;
        for (BlockPart part : parts) {
            result |= part.code;
        }
        return result;
    }

    public static List<BlockPart> decode(int parts) {
        List<BlockPart> result = new ArrayList<>();
        // NOTE: values() returns an array containing all the values of the enum type
        // in the order they are declared.
        for (BlockPart bp : BlockPart.values()) {
            if ((parts & bp.code) != 0) {
                result.add(bp);
            }
        }

        return result;
    }

}
