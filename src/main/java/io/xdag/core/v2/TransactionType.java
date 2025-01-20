package io.xdag.core.v2;

import lombok.Getter;

@Getter
public enum TransactionType {

    /**
     * (0x00) Coinbase transaction
     */
    COINBASE(0x00),

    /**
     * (0x01) Balance transfer.
     */
    TRANSFER(0x01);

    private static final TransactionType[] map = new TransactionType[256];
    static {
        for (TransactionType tt : TransactionType.values()) {
            map[tt.code] = tt;
        }
    }

    public static TransactionType of(byte code) {
        return map[0xff & code];
    }

    private final int code;

    TransactionType(int code) {
        this.code = code;
    }

    public byte toByte() {
        return (byte) code;
    }
}
