package io.xdag.core.v2;

import io.xdag.config.Config;
import io.xdag.config.spec.ChainSpec;
import io.xdag.core.XAmount;
import io.xdag.core.v2.state.Account;
import io.xdag.core.v2.state.AccountState;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import static io.xdag.core.v2.TransactionResult.Code;


@Slf4j
public class TransactionExecutor {

    private final ChainSpec spec;

    /**
     * Creates a new transaction executor.
     */
    public TransactionExecutor(Config config) {
        this.spec = config.getChainSpec();
    }

    /**
     * Execute a list of transactions.
     */
    public List<TransactionResult> execute(List<Transaction> txs, AccountState as) {
        List<TransactionResult> results = new ArrayList<>();

        for (Transaction tx : txs) {
            TransactionResult result = new TransactionResult();
            results.add(result);

            TransactionType type = tx.getType();
            byte[] from = tx.getFrom();
            byte[] to = tx.getTo();
            XAmount value = tx.getValue();
            long nonce = tx.getNonce();
            XAmount fee = tx.getFee();
            byte[] data = tx.getData();

            Account acc = as.getAccount(from);
            XAmount available = acc.getAvailable();

            try {
                // check nonce
                if (nonce != acc.getNonce()) {
                    result.setCode(TransactionResult.Code.INVALID_NONCE);
                    continue;
                }


                if (fee.lessThan(spec.getMinTransactionFee())) {
                    result.setCode(Code.INVALID_FEE);
                    continue;
                }


                // check data length
                if (data.length > spec.getMaxTransactionDataSize(type)) {
                    result.setCode(Code.INVALID_DATA);
                    continue;
                }

                switch (type) {
                    case TRANSFER: {
                        if (fee.lessThanOrEqual(available) && value.lessThanOrEqual(available)
                                && value.add(fee).lessThanOrEqual(available)) {
                            as.adjustAvailable(from, value.add(fee).negate());
                            as.adjustAvailable(to, value);
                        } else {
                            result.setCode(Code.INSUFFICIENT_AVAILABLE);
                        }
                        break;
                    }
                    default:
                        // unsupported transaction type
                        result.setCode(Code.INVALID_TYPE);
                        break;
                }
            } catch (ArithmeticException ae) {
                log.warn("An arithmetic exception occurred during transaction execution: {}", tx);
                result.setCode(Code.INVALID);
            }

            if (result.getCode().isAcceptable()) {
                as.increaseNonce(from);
            }

        }

        return results;
    }

    /**
     * Execute one transaction.
     */
    public TransactionResult execute(Transaction tx, AccountState as) {
        return execute(Collections.singletonList(tx), as).get(0);
    }

}
