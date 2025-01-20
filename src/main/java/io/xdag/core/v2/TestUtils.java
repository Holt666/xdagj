package io.xdag.core.v2;

import io.xdag.Network;
import io.xdag.config.Config;
import io.xdag.core.XAmount;
import io.xdag.crypto.Keys;
import io.xdag.utils.BytesUtils;
import io.xdag.utils.MerkleUtils;
import io.xdag.utils.TimeUtils;
import org.hyperledger.besu.crypto.KeyPair;
import java.util.Collections;
import java.util.List;

public class TestUtils {

    public static Block createBlock(long number, long timestamp, byte[] prevHash, List<byte[]> witnessHashs, KeyPair coinbase, List<Transaction> txs,
                                              List<TransactionResult> res) {
        return createMainBlock(number, timestamp, prevHash, coinbase, txs, res);
    }

    public static Block createMainBlock(long number, long timestamp, byte[] prevHash, KeyPair coinbase, List<Transaction> txs, List<TransactionResult> res) {
        byte[] nbits = BytesUtils.EMPTY_HASH;
        byte[] nonce = BytesUtils.EMPTY_HASH;
        List<byte[]> witnessHashs = Collections.emptyList();

        byte[] transactionsRoot = MerkleUtils.computeTransactionsRoot(txs);
        byte[] resultsRoot = MerkleUtils.computeResultsRoot(res);

        BlockHeader header = new BlockHeader(number, Keys.toBytesAddress(coinbase), prevHash, timestamp,
                transactionsRoot, resultsRoot, BytesUtils.EMPTY_HASH, BytesUtils.EMPTY_BYTES, nbits, nonce, witnessHashs);
        return new Block(header, txs, res);
    }

    public static Block createEmptyMainBlock(long number) {
        return createMainBlock(0, 0, BytesUtils.EMPTY_HASH, null, Collections.emptyList(), Collections.emptyList());
    }

//    public static Transaction createTransaction(Config config) {
//        return createTransaction(config, new Key(), new Key(), XAmount.ZERO);
//    }

    public static Transaction createTransaction(Config config, KeyPair from, byte[] to, XAmount value) {
        return createTransaction(config, from, to, value, 0);
    }

    public static Transaction createTransaction(Config config, KeyPair from, byte[] to, XAmount value, long nonce) {
        return createTransaction(config, TransactionType.TRANSFER, from, to, value, 0);
    }

    public static Transaction createTransaction(Config config, TransactionType type, KeyPair from, byte[] to, XAmount value,
                                                long nonce) {
        Network network = config.getNetwork();
        XAmount fee = config.getChainSpec().getMinTransactionFee();
        long timestamp = TimeUtils.currentTimeMillis();
        byte[] data = {};

        return new Transaction(network, type, to, value, fee, nonce, timestamp, data).sign(from);
    }
}
