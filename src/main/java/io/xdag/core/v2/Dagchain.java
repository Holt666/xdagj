package io.xdag.core.v2;

import io.xdag.core.ImportResult;
import io.xdag.core.v2.state.AccountState;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public interface Dagchain {

    /** Block */
//    void addBlock(Block block);
//    Block getBlock(byte[] hash);
//    BlockHeader getBlockHeader(byte[] hash);

    /** Transaction */
    Transaction getTransaction(byte[] txHash);
    TransactionResult getTransactionResult(byte[] txHash);
    boolean hasTransaction(byte[] txHash);
    int getTransactionCount(byte[] address);
    List<Transaction> getTransactions(byte[] address, int from, int to);

    /** XDAG Main Chain*/
    boolean hasMainBlock(long number);
    ImportResult importBlock(Block B);
    Block getLatestMainBlock();
    long getLatestMainBlockNumber();
    byte[] getMainBlockHash(long number);
    byte[] getMainBlockParentHash(long number);
    List<byte[]> getMainBlockWitnessBlockHashes(long number);
    Transaction getMainBlockCoinbaseTransaction(long number);

    /** Account State */
    AccountState getAccountState();
    void addListener(DagchainListener listener);


    ReentrantReadWriteLock getStateLock();

}
