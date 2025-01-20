package io.xdag.core.v2;

import io.xdag.utils.SimpleDecoder;
import io.xdag.utils.SimpleEncoder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TransactionIndex {

    byte[] blockHash;
    int transactionOffset;
    int resultOffset;

    public TransactionIndex(byte[] blockHash, int transactionOffset, int resultOffset) {
        this.blockHash = blockHash;
        this.transactionOffset = transactionOffset;
        this.resultOffset = resultOffset;
    }

    public byte[] toBytes() {
        SimpleEncoder enc = new SimpleEncoder();
        enc.writeBytes(blockHash);
        enc.writeInt(transactionOffset);
        enc.writeInt(resultOffset);
        return enc.toBytes();
    }

    public static TransactionIndex fromBytes(byte[] bytes) {
        SimpleDecoder dec = new SimpleDecoder(bytes);
        byte[] blockHash = dec.readBytes();
        int transactionOffset = dec.readInt();
        int resultOffset = dec.readInt();
        return new TransactionIndex(blockHash, transactionOffset, resultOffset);
    }

}
