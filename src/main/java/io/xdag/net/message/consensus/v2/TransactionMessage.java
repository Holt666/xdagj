package io.xdag.net.message.consensus.v2;

import io.xdag.core.v2.Transaction;
import io.xdag.net.message.Message;
import io.xdag.net.message.MessageCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TransactionMessage extends Message {

    private final Transaction transaction;

    /**
     * Create a TRANSACTION message.
     *
     */
    public TransactionMessage(Transaction transaction) {
        super(MessageCode.TRANSACTION, null);

        this.transaction = transaction;
        this.body = transaction.toBytes();
    }

    /**
     * Parse a TRANSACTION message from byte array.
     *
     * @param body
     */
    public TransactionMessage(byte[] body) {
        super(MessageCode.TRANSACTION, null);

        this.transaction = Transaction.fromBytes(body);

        this.body = body;
    }

    @Override
    public String toString() {
        return "TransactionMessage [tx=" + transaction + "]";
    }

}
