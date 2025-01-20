package io.xdag.core.v2;

import io.xdag.utils.BytesUtils;
import io.xdag.utils.SimpleDecoder;
import io.xdag.utils.SimpleEncoder;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.binary.Hex;

@Setter
@Getter
public class TransactionResult {

    /**
     * Transaction execution result code.
     */
    public enum Code {

        /**
         * Success. The values have to be 0x01 for compatibility.
         */
        SUCCESS(0x01),

        /**
         * VM failure, e.g. REVERT, STACK_OVERFLOW, OUT_OF_GAS, etc.
         */
        FAILURE(0x02),

        /**
         * The transaction hash is invalid (should NOT be included on chain).
         */
        INVALID(0x20),

        /**
         * The transaction format is invalid.
         */
        INVALID_FORMAT(0x21),

        /**
         * The transaction timestamp is incorrect.
         */
        INVALID_TIMESTAMP(0x22),

        /**
         * The transaction type is invalid.
         */
        INVALID_TYPE(0x23),

        /**
         * The transaction nonce does not match the account nonce.
         */
        INVALID_NONCE(0x24),

        /**
         * The transaction fee (or gas * gasPrice) doesn't meet the minimum.
         */
        INVALID_FEE(0x25),

        /**
         * The transaction data is invalid, typically too large.
         */
        INVALID_DATA(0x27),

        /**
         * Insufficient available balance.
         */
        INSUFFICIENT_AVAILABLE(0x28),

        /**
         * Insufficient locked balance.
         */
        INSUFFICIENT_LOCKED(0x29);

        private static final Code[] map = new Code[256];

        static {
            for (Code code : Code.values()) {
                map[code.v] = code;
            }
        }

        private final byte v;

        Code(int c) {
            this.v = (byte) c;
        }

        public static Code of(int c) {
            return map[c];
        }

        public byte toByte() {
            return v;
        }

        public boolean isSuccess() {
            return this == SUCCESS;
        }

        public boolean isFailure() {
            return this == FAILURE;
        }

        public boolean isRejected() {
            return !isSuccess() && !isFailure();
        }

        public boolean isAcceptable() {
            return isSuccess() || isFailure();
        }
    }

    /**
     * Transaction execution result code.
     */
    protected Code code;

    /**
     * Transaction returns.
     */
    protected byte[] returnData;

    /**
     * Create a transaction result.
     */
    public TransactionResult() {
        this(Code.SUCCESS);
    }

    public TransactionResult(Code code) {
        this(code, BytesUtils.EMPTY_BYTES);
    }

    public TransactionResult(Code code, byte[] returnData) {
        this.code = code;
        this.returnData = returnData;
    }

    public static TransactionResult fromBytes(byte[] bytes) {
        TransactionResult result = new TransactionResult();

        SimpleDecoder dec = new SimpleDecoder(bytes);
        Code code = Code.of(dec.readByte());
        result.setCode(code);

        byte[] returnData = dec.readBytes();
        result.setReturnData(returnData);

        return result;
    }

    public byte[] toBytes() {
        SimpleEncoder enc = new SimpleEncoder();
        enc.writeByte(code.toByte());
        enc.writeBytes(returnData);
        return enc.toBytes();
    }

    public byte[] toBytesForMerkle() {
        SimpleEncoder enc = new SimpleEncoder();
        enc.writeByte(code.toByte());
        enc.writeBytes(returnData);
        return enc.toBytes();
    }

    @Override
    public String toString() {
        return "TransactionResult{" +
                "code=" + code +
                ", returnData=" + Hex.encodeHexString(returnData) +
                '}';
    }

}
