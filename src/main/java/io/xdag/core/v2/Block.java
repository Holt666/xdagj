package io.xdag.core.v2;

import io.xdag.Network;
import io.xdag.config.Config;
import io.xdag.core.XAmount;
import io.xdag.utils.MerkleUtils;
import io.xdag.utils.SimpleDecoder;
import io.xdag.utils.SimpleEncoder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static io.xdag.core.XAmount.ZERO;

@Slf4j
@Getter
@Setter
public class Block {

    private final BlockHeader header;
    private final List<Transaction> transactions;
    private List<TransactionResult> results;

    public Block(BlockHeader header, List<Transaction> transactions, List<TransactionResult> results) {
        this.header = header;

        this.transactions = transactions;
        this.results = results;
    }

    public Block(BlockHeader header, List<Transaction> transactions) {
        this(header, transactions, new ArrayList<>());
    }

    public boolean validateHeader(BlockHeader header) {
        if (header == null) {
            log.warn("Header was null.");
            return false;
        }

        if (!header.validate()) {
            log.warn("Header was invalid.");
            return false;
        }

//        if (header.getNumber() != parentHeader.getNumber() + 1) {
//            log.warn("Header number was not one greater than previous block.");
//            return false;
//        }

//        if (!Arrays.equals(header.getParentHash(), parentHeader.getHash())) {
//            log.warn("Header parent hash was not equal to previous block hash.");
//            return false;
//        }

//        if (header.getTimestamp() <= parentHeader.getTimestamp()) {
//            log.warn("Header timestamp was before previous block.");
//            return false;
//        }

        if (Arrays.equals(header.getCoinbase(), Constants.COINBASE_ADDRESS)) {
            log.warn("Header coinbase was a reserved address");
            return false;
        }

        return true;
    }

    public boolean validateTransactions(BlockHeader header, List<Transaction> transactions, Network network) {
        return validateTransactions(header, transactions, transactions, network);
    }

    public boolean validateTransactions(BlockHeader header, Collection<Transaction> unvalidatedTransactions,
                                        List<Transaction> allTransactions, Network network) {

        // validate transactions
        if (!unvalidatedTransactions.parallelStream().allMatch(tx -> tx.validate(network))) {
            return false;
        }

        // validate transactions root
        byte[] root = MerkleUtils.computeTransactionsRoot(allTransactions);
        return Arrays.equals(root, header.getTransactionsRoot());
    }

    public boolean validateResults(BlockHeader header, List<TransactionResult> results) {
        long number = header.getNumber();
        byte[] hash = header.getHash();

        // validate results
        for (int i = 0; i < results.size(); i++) {
            TransactionResult result = results.get(i);
            if (result.getCode().isRejected()) {
                log.warn("Transaction #{} in block #{} {} rejected: code = {}", i, number, Hex.encodeHexString(hash), result.getCode());
                return false;
            }
        }

        // validate results root
        byte[] root = MerkleUtils.computeResultsRoot(results);
        boolean rootMatches = Arrays.equals(root, header.getResultsRoot());
        if (!rootMatches) {
            log.warn("Transaction result root doesn't match in block #{} {}", number, Hex.encodeHexString(hash));
        }

        return rootMatches;
    }

    public static XAmount getBlockReward(Block block, Config config) {
        XAmount txsReward = block.getTransactions().stream().map(Transaction::getFee).reduce(ZERO, XAmount::sum);
        XAmount gasReward = getFeeReward(block);
        return config.getChainSpec().getBlockReward(block.getNumber()).add(txsReward).add(gasReward);
    }

    private static XAmount getFeeReward(Block block) {
        List<Transaction> transactions = block.getTransactions();
        XAmount sum = ZERO;
        for (Transaction transaction : transactions) {
            sum = sum.add(transaction.getFee());
        }
        return sum;
    }

    public List<Transaction> getTransactions() {
        return new ArrayList<>(transactions);
    }

    public List<TransactionResult> getResults() {
        return new ArrayList<>(results);
    }

    public byte[] getHash() {
        return header.getHash();
    }

    public byte[] getCoinbase() {
        return header.getCoinbase();
    }

    public byte[] getParentHash() {
        return header.getParentHash();
    }

    public long getTimestamp() {
        return header.getTimestamp();
    }

    public byte[] getTransactionsRoot() {
        return header.getTransactionsRoot();
    }

    public byte[] getResultsRoot() {
        return header.getResultsRoot();
    }

    public byte[] getEncodedHeader() {
        return header.toBytes();
    }

    public long getNumber() {
        return header.getNumber();
    }

    public byte[] getEncodedTransactions() {
        return getEncodedTransactionsAndIndices().getLeft();
    }

    public Pair<byte[], List<Integer>> getEncodedTransactionsAndIndices() {
        List<Integer> indices = new ArrayList<>();

        SimpleEncoder enc = new SimpleEncoder();
        enc.writeInt(transactions.size());
        for (Transaction transaction : transactions) {
            int index = enc.getWriteIndex();
            enc.writeBytes(transaction.toBytes());
            indices.add(index);
        }

        return Pair.of(enc.toBytes(), indices);
    }

    public byte[] getEncodedResults() {
        return getEncodedResultsAndIndices().getLeft();
    }

    public Pair<byte[], List<Integer>> getEncodedResultsAndIndices() {
        List<Integer> indices = new ArrayList<>();

        SimpleEncoder enc = new SimpleEncoder();
        enc.writeInt(results.size());
        for (TransactionResult result : results) {
            int index = enc.getWriteIndex();
            enc.writeBytes(result.toBytes());
            indices.add(index);
        }

        return Pair.of(enc.toBytes(), indices);
    }

    public static Block fromComponents(byte[] h, byte[] t, byte[] r) {
        if (h == null) {
            throw new IllegalArgumentException("Block header can't be null");
        }
        if (t == null) {
            throw new IllegalArgumentException("Block transactions can't be null");
        }

        BlockHeader header = BlockHeader.fromBytes(h);

        SimpleDecoder dec = new SimpleDecoder(t);
        List<Transaction> transactions = new ArrayList<>();
        int n = dec.readInt();
        for (int i = 0; i < n; i++) {
            transactions.add(Transaction.fromBytes(dec.readBytes()));
        }

        List<TransactionResult> results = new ArrayList<>();
        if (r != null) {
            dec = new SimpleDecoder(r);
            n = dec.readInt();
            for (int i = 0; i < n; i++) {
                results.add(TransactionResult.fromBytes(dec.readBytes()));
            }
        }

        return new Block(header, transactions, results);
    }

    public byte[] toBytes() {
        SimpleEncoder enc = new SimpleEncoder();
        enc.writeBytes(getEncodedHeader());
        enc.writeBytes(getEncodedTransactions());
        enc.writeBytes(getEncodedResults());

        return enc.toBytes();
    }

    public static Block fromBytes(byte[] bytes) {
        SimpleDecoder dec = new SimpleDecoder(bytes);
        byte[] header = dec.readBytes();
        byte[] transactions = dec.readBytes();
        byte[] results = dec.readBytes();

        return Block.fromComponents(header, transactions, results);
    }

    public int size() {
        return toBytes().length;
    }

    @Override
    public String toString() {
        return "Block [hash = " + Hex.encodeHexString(getHash()) + ", # txs = " + transactions.size() + "]";
    }

}
