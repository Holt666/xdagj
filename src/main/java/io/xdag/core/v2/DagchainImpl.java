package io.xdag.core.v2;

import io.xdag.config.Config;
import io.xdag.core.ImportResult;
import io.xdag.core.XAmount;
import io.xdag.core.v2.db.*;
import io.xdag.core.v2.state.AccountState;
import io.xdag.core.v2.store.DagStore;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import io.xdag.utils.DifficultyUtils;


@Slf4j
@Getter
@Setter
public class DagchainImpl implements Dagchain {
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final List<DagchainListener> listeners = new ArrayList<>();

    private final Config config;
    private final Genesis genesis;
    private final DagStore dagStore;

    private Block preLastMainblock;
    private Block lastMainBlock;

    public DagchainImpl(Config config, DatabaseFactory dbFactory) {
        this(config, Genesis.load(config.getNetwork()), dbFactory);
    }

    public DagchainImpl(Config config, Genesis genesis, DatabaseFactory dbFactory) {
        this.config = config;
        this.genesis = genesis;
        dagStore = new DagStore(config, genesis, dbFactory);
    }

    @Override
    public Transaction getTransaction(byte[] hash) {
        return dagStore.getTransaction(hash);
    }

    @Override
    public Transaction getMainBlockCoinbaseTransaction(long number) {
        return dagStore.getCoinbaseTransaction(number);
    }

    @Override
    public boolean hasTransaction(byte[] hash) {
        return dagStore.hasTransaction(hash);
    }

    @Override
    public TransactionResult getTransactionResult(byte[] hash) {
        return dagStore.getTransactionResult(hash);
    }

    @Override
    public int getTransactionCount(byte[] address) {
        return dagStore.getTransactionCount(address);
    }

    @Override
    public List<Transaction> getTransactions(byte[] address, int from, int to) {
        return dagStore.getTransactions(address, from, to);
    }

    @Override
    public boolean hasMainBlock(long number) {
        return dagStore.getMainBlockHash(number) != null;
    }

    @Override
    public AccountState getAccountState() {
        return dagStore.getAccountState();
    }

    @Override
    public void addListener(DagchainListener listener) {
        listeners.add(listener);
    }

    @Override
    public ReentrantReadWriteLock getStateLock() {
        return stateLock;
    }

    @Override
    public synchronized ImportResult importBlock(Block block) {
        AccountState asTrack = this.getAccountState().track();
        
        // 1. 基础验证
        if (!validateBlock(block, asTrack)) {
            return ImportResult.INVALID_BLOCK;
        }

        // 2. 检查最低难度要求
        if (!DifficultyUtils.meetsMinimumDifficulty(block)) {
            return ImportResult.DIFFICULTY_TOO_LOW;
        }

        // 3. 检查时间戳有效性
        if (!DifficultyUtils.isValidBlockTime(block, System.currentTimeMillis() / 1000)) {
            return ImportResult.INVALID_TIMESTAMP;
        }

        // 4. 处理不同的父块情况
        if (Arrays.equals(block.getParentHash(), lastMainBlock.getHash())) {
            return processNewBlock(block, asTrack);
        } else if (Arrays.equals(block.getParentHash(), lastMainBlock.getParentHash())) {
            // 检查是否可以成为主块
            if (DifficultyUtils.canBeMainBlock(block, lastMainBlock)) {
                return processCompetingBlock(block, asTrack);
            } else {
                // 存储为见证块
                if (validateBlock(block, asTrack)) {
                    return ImportResult.IMPORTED_WITNESS;
                }
                return ImportResult.INVALID_BLOCK;
            }
        } else {
            // 基于其他块的分叉块
            return processForkBlock(block, asTrack);
        }
    }

    private ImportResult processNewBlock(Block block, AccountState asTrack) {
        boolean validateResult = validateBlock(block, asTrack);
        boolean applyResult = applyBlock(block, asTrack);

        if (validateResult && applyResult) {
            preLastMainblock = lastMainBlock;
            lastMainBlock = block;

            // 通知监听器清理交易池等
            for (DagchainListener listener : listeners) {
                listener.onMainBlockAdded(block);
            }
            return ImportResult.IMPORTED_BEST;
        }
        return ImportResult.ERROR;
    }

    private ImportResult processCompetingBlock(Block block, AccountState asTrack) {
        // 比较难度
        if (block.getCumulativeDifficulty() <= lastMainBlock.getCumulativeDifficulty()) {
            // 难度较小，作为见证块存储
            if (validateBlock(block, asTrack)) {
                dagStore.addWitnessBlock(block);
                return ImportResult.IMPORTED_WITNESS;
            }
            return ImportResult.INVALID_BLOCK;
        } else {
            // 难度更大，需要替换当前主块
            // 1. 回滚当前主块
            rollbackBlock(lastMainBlock, asTrack);
            
            // 2. 应用新块
            boolean validateResult = validateBlock(block, asTrack);
            boolean applyResult = applyBlock(block, asTrack);
            
            if (validateResult && applyResult) {
                // 将原主块降级为见证块
                dagStore.addWitnessBlock(lastMainBlock);
                
                // 更新主块
                preLastMainblock = lastMainBlock.getParentBlock();
                lastMainBlock = block;
                
                // 通知监听器
                for (DagchainListener listener : listeners) {
                    listener.onMainBlockReplaced(block);
                }
                return ImportResult.IMPORTED_BEST;
            }
            return ImportResult.ERROR;
        }
    }

    private ImportResult processForkBlock(Block block, AccountState asTrack) {
        // 1. 检查块是否太旧
        if (block.getTimestamp() / 64 <= getLatestMainBlockNumber() / 64 - 128) {
            return ImportResult.TOO_OLD;
        }

        // 2. 获取父块
        Block parent = dagStore.getBlock(block.getParentHash());
        if (parent == null) {
            // 找不到父块，存储为孤块
            dagStore.addOrphanBlock(block);
            return ImportResult.IMPORTED_NOT_BEST;
        }

        // 3. 检查是否形成更优的链
        if (DifficultyUtils.compareChainDifficulty(block, lastMainBlock, dagStore) > 0) {
            // 找到分叉点
            Block forkPoint = DifficultyUtils.findForkPoint(block, lastMainBlock, dagStore);
            if (forkPoint == null) {
                return ImportResult.INVALID_BLOCK;
            }

            // 获取需要回滚和应用的块
            List<Block> blocksToRollback = DifficultyUtils.getBlocksBetween(lastMainBlock, forkPoint, dagStore);
            List<Block> blocksToApply = DifficultyUtils.getBlocksBetween(block, forkPoint, dagStore);

            // 回滚到分叉点
            for (Block blockToRollback : blocksToRollback) {
                rollbackBlock(blockToRollback, asTrack);
                // 将回滚的块降级为见证块
                dagStore.addWitnessBlock(blockToRollback);
            }

            // 应用新的链
            for (Block blockToApply : blocksToApply) {
                if (!validateBlock(blockToApply, asTrack) || !applyBlock(blockToApply, asTrack)) {
                    return ImportResult.INVALID_BLOCK;
                }
            }

            // 更新主块
            preLastMainblock = blocksToApply.get(blocksToApply.size() - 2);
            lastMainBlock = block;

            // 通知监听器
            for (DagchainListener listener : listeners) {
                listener.onChainReorganized(block, blocksToApply, blocksToRollback);
            }
            return ImportResult.IMPORTED_BEST;
        } else {
            // 难度不够，存储为见证块
            if (validateBlock(block, asTrack)) {
                dagStore.addWitnessBlock(block);
                return ImportResult.IMPORTED_WITNESS;
            }
            return ImportResult.INVALID_BLOCK;
        }
    }

    @Override
    public Block getLatestMainBlock() {
        return lastMainBlock;
    }

    @Override
    public long getLatestMainBlockNumber() {
        return lastMainBlock.getNumber();
    }

    @Override
    public byte[] getMainBlockHash(long number) {
        return dagStore.getMainBlockHash(number);
    }

    @Override
    public byte[] getMainBlockParentHash(long number) {
        return dagStore.getMainBlockParentHash(number);
    }

    @Override
    public List<byte[]> getMainBlockWitnessBlockHashes(long number) {
        return dagStore.getMainBlockWitnessBlockHashes(number);
    }

    protected boolean validateBlock(Block block, AccountState asTrack) {
        try {
            BlockHeader header = block.getHeader();
            List<Transaction> transactions = block.getTransactions();

            // [1] check block header
            Block latest = this.getLatestMainBlock();
            if (!block.validateHeader(header)) {
                log.error("Invalid block header");
                return false;
            }

            // [?] additional checks by block importer
            // - check points
//            if (config.checkpoints().containsKey(header.getNumber()) &&
//                    !Arrays.equals(header.getHash(), config.checkpoints().get(header.getNumber()))) {
//                log.error("Checkpoint validation failed, checkpoint is {} => {}, getting {}", header.getNumber(),
//                        Hex.encode0x(config.checkpoints().get(header.getNumber())),
//                        Hex.encode0x(header.getHash()));
//                return false;
//            }

            // [2] check transactions
            if (!block.validateTransactions(header, transactions, config.getNetwork())) {
                log.error("Invalid transactions");
                return false;
            }

//            if (transactions.stream().anyMatch(tx -> this.hasTransaction(tx.getHash()))) {
//                log.error("Duplicated transaction hash is not allowed");
//                return false;
//            }
            // filter not  exist transactions
            List<Transaction> txs = transactions.stream().filter(tx -> !this.hasTransaction(tx.getHash())).toList();

            // [3] evaluate transactions
            TransactionExecutor transactionExecutor = new TransactionExecutor(config);
            List<TransactionResult> results = transactionExecutor.execute(txs, asTrack);
            if (!block.validateResults(header, results)) {
                log.error("Invalid transaction results");
                return false;
            }
            block.setResults(results); // overwrite the results

            return true;
        } catch (Exception e) {
            log.error("Unexpected exception during block validation", e);
            return false;
        }
    }

    protected boolean applyBlock(Block block, AccountState asTrack) {
        // [5] apply block reward and tx fees
        XAmount reward = Block.getBlockReward(block, config);

        if (reward.isPositive()) {
            asTrack.adjustAvailable(block.getCoinbase(), reward);
        }

        // [6] commit the updates
        asTrack.commit();
        ReentrantReadWriteLock.WriteLock writeLock = this.stateLock.writeLock();
        writeLock.lock();
        try {
            // [7] flush state to disk
            this.getAccountState().commit();

            // [8] add block to chain
            dagStore.addBlock(block);
        } finally {
            writeLock.unlock();
        }
        return true;
    }

    private Block getBlock(byte[] hash) {
        return dagStore.getBlock(hash);
    }

    private BlockHeader getBlockHeader(byte[] hash) {
        return dagStore.getBlockHeader(hash);
    }

    public boolean hasBlock(byte[] hash) {
        return dagStore.hasBlock(hash);
    }
}
