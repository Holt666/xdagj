package io.xdag.core.v2.store;

import com.google.common.collect.Lists;
import io.xdag.config.Config;
import io.xdag.core.ImportResult;
import io.xdag.core.XAmount;
import io.xdag.core.v2.*;
import io.xdag.core.v2.db.*;
import io.xdag.core.v2.state.AccountState;
import io.xdag.core.v2.state.AccountStateImpl;
import io.xdag.utils.BytesUtils;
import io.xdag.utils.SimpleDecoder;
import io.xdag.utils.TimeUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 /**
 * Dag chain implementation.
 *
 * <pre>
 * index DB structure:
 *
 * [0x00] => [latest_main_block_number]
 *
 * Transaction Index:
 * [0x01, transaction_hash] => [block_number, block_hash, from, to]
 *
 * Address Index:
 * [0x02, address, n] => [transaction_hash]
 *
 * Main Block Index:
 * [0x04, main_block_number]  => [block_hash]
 * [0x05, main_block_number]  => [block_parent_hash]
 * [0x06, main_block_number]  => [witnessBlockHashes]
 * [0x07, main_block_number]  => [coinbase_transaction]
 *
 * [0xff] => [database version]
 * </pre>
 *
 * <pre>
 * Dag block DB structure:
 * [0x00, hash] => [block_header]
 * [0x01, hash] => [block_transactions]
 * [0x02, hash] => [block_results]
 * [0x03, hash] => [block_chain_work]
 * </pre>
 */
@Slf4j
@Getter
public class DagStore {

    protected static final int DATABASE_VERSION = 1;

    protected static final byte TYPE_LATEST_MAIN_BLOCK_NUMBER = 0x00;
    protected static final byte TYPE_TRANSACTION_INDEX_BY_HASH = 0x01;
    protected static final byte TYPE_TRANSACTION_COUNT_BY_ADDRESS = 0x02;
    protected static final byte TYPE_TRANSACTION_HASH_BY_ADDRESS_AND_INDEX = 0x02;

    // main chain query for dag design
    protected static final byte TYPE_MAINBLOCK_HASH_BY_NUMBER = 0x04;
    protected static final byte TYPE_MAINBLOCK_PARENTHASH_BY_NUMBER = 0x05;
    protected static final byte TYPE_MAINBLOCK_WITNESSBLOCKHASHES_BY_NUMBER = 0x06;
    protected static final byte TYPE_MAINBLOCK_COINBASE_BY_NUMBER = 0x08;

    protected static final byte TYPE_BLOCK_HEADER_BY_HASH = 0x00;
    protected static final byte TYPE_BLOCK_TRANSACTIONS_BY_HASH = 0x01;
    protected static final byte TYPE_BLOCK_RESULTS_BY_HASH = 0x02;
    protected static final byte TYPE_BLOCK_CHAINWORK_BY_HASH = 0x03;

    protected static final byte TYPE_DATABASE_VERSION = (byte) 0xff;

    private final Config config;
    private final Genesis genesis;

    private Database indexDB;
    private Database blockDB;
    private Database accountDB;

    @Getter
    private AccountState accountState;
    @Getter
    private Block latestMainBlock;

    public DagStore(Config config, DatabaseFactory dbFactory) {
        this(config, Genesis.load(config.getNetwork()), dbFactory);
    }

    public DagStore(Config config, Genesis genesis, DatabaseFactory dbFactory) {
        this.config = config;
        this.genesis = genesis;
        openDb(config, dbFactory);
    }


    private void initializeDb() {
        // initialize database version
        indexDB.put(BytesUtils.of(TYPE_DATABASE_VERSION), BytesUtils.of(DATABASE_VERSION));

        // pre-allocation
        for (Genesis.Snapshot s : genesis.getSnapshots().values()) {
            accountState.adjustAvailable(s.getAddress(), s.getAmount());
        }
        accountState.commit();

        // add block
        addBlock(genesis);
    }

    private static void upgradeDatabase(Config config, DatabaseFactory dbFactory) {
        if (getLatestMainBlockNumber(dbFactory.getDB(DatabaseName.INDEX)) != null
                && getDatabaseVersion(dbFactory.getDB(DatabaseName.INDEX)) < DATABASE_VERSION) {
            upgrade(config, dbFactory, Long.MAX_VALUE);
        }
    }

    public static void upgrade(Config config, DatabaseFactory dbFactory, long to) {
        try {
            log.info("Upgrading the database... DO NOT CLOSE THE WALLET!");
            Instant begin = Instant.now();

            Path dataDir = dbFactory.getDataDir();
            String dataDirName = dataDir.getFileName().toString();

            // setup temp chain
            Path tempPath = dataDir.resolveSibling(dataDirName + "-temp");
            delete(tempPath);
            LeveldbDatabase.LeveldbFactory tempDbFactory = new LeveldbDatabase.LeveldbFactory(tempPath.toFile());
            DagchainImpl tempChain = new DagchainImpl(config, tempDbFactory);

            // import all blocks
            long imported = 0;
            Database indexDB = dbFactory.getDB(DatabaseName.INDEX);
            Database blockDB = dbFactory.getDB(DatabaseName.BLOCK);

            byte[] bytes = getLatestMainBlockNumber(indexDB);
            long latestMainBlockNumber = (bytes == null) ? 0 : BytesUtils.toLong(bytes);

            long target = Math.min(latestMainBlockNumber, to);
            for (long i = 1; i <= target; i++) {
                Block block = getMainBlock(indexDB, blockDB, i, true);
                ImportResult result = tempChain.importBlock(block);
                if(result != ImportResult.IMPORTED_BEST) {
                    break;
                }
//                boolean result = tempChain.importBlock(block);
//                if (!result) {
//                    break;
//                }

                if (i % 1000 == 0) {
                    //PubSubFactory.getDefault().publish(new BlockchainDatabaseUpgradingEvent(i, latestBlockNumber));
                    log.info("Loaded {} / {} blocks", i, target);
                }
                imported++;
            }

            // close both database factory
            dbFactory.close();
            tempDbFactory.close();

            // swap the database folders
            Path backupPath = dataDir.resolveSibling(dataDirName + "-backup");
            dbFactory.moveTo(backupPath);
            tempDbFactory.moveTo(dataDir);
            delete(backupPath); // delete old database to save space.

            Instant end = Instant.now();
            log.info("Database upgraded: found blocks = {}, imported = {}, took = {}", latestMainBlockNumber, imported,
                    TimeUtils.formatDuration(Duration.between(begin, end)));
        } catch (IOException e) {
            log.error("Failed to upgrade database", e);
        }
    }

    private synchronized void openDb(Config config, DatabaseFactory dbFactory) {
        // upgrade if possible
        upgradeDatabase(config, dbFactory);

        this.indexDB = dbFactory.getDB(DatabaseName.INDEX);
        this.blockDB = dbFactory.getDB(DatabaseName.BLOCK);
        this.accountDB = dbFactory.getDB(DatabaseName.ACCOUNT);

        this.accountState = new AccountStateImpl(accountDB);

        // checks if the database needs to be initialized
        byte[] number = indexDB.get(BytesUtils.of(TYPE_LATEST_MAIN_BLOCK_NUMBER));

        if (number == null || number.length == 0) {
            // initialize the database for the first time
            initializeDb();
        } else {
            // load the latest block
            byte[] lastBlockHash = getLatestMainBlockHash();
            latestMainBlock = getBlock(lastBlockHash);
        }
    }

    public byte[] getLatestMainBlockHash() {
        return this.latestMainBlock.getHash();
    }

    public long getLatestMainBlockNumber() {
        return this.latestMainBlock.getNumber();
    }

    public byte[] getMainBlockHash(long number) {
        return indexDB.get(BytesUtils.merge(TYPE_MAINBLOCK_HASH_BY_NUMBER, BytesUtils.of(number)));
    }

    public byte[] getMainBlockParentHash(long number) {
        return indexDB.get(BytesUtils.merge(TYPE_MAINBLOCK_PARENTHASH_BY_NUMBER, BytesUtils.of(number)));
    }

    public List<byte[]> getMainBlockWitnessBlockHashes(long number) {
        List<byte[]> witnessHashes = new ArrayList<>();
        byte[] hashes = indexDB.get(BytesUtils.merge(TYPE_MAINBLOCK_WITNESSBLOCKHASHES_BY_NUMBER, BytesUtils.of(number)));

        for(int i = 0; hashes != null && i < hashes.length; i += 32) {
            byte[] hash = new byte[32];
            System.arraycopy(hashes, i, hash, 0, hash.length);
            witnessHashes.add(hash);
        }

        return witnessHashes;
    }

//    public long getBlockNumber(byte[] hash) {
//        byte[] number = indexDB.get(BytesUtils.merge(TYPE_BLOCK_NUMBER_BY_HASH, hash));
//        return (number == null) ? -1 : BytesUtils.toLong(number);
//    }

    public Block getBlock(byte[] hash) {
        return getBlock(blockDB, hash, false);
    }

    public BlockHeader getBlockHeader(byte[] hash) {
        byte[] header =blockDB.get(BytesUtils.merge(TYPE_BLOCK_HEADER_BY_HASH, hash));
        return header == null? null:BlockHeader.fromBytes(header);
    }

    public boolean hasBlock(byte[] hash) {
        return blockDB.get(BytesUtils.merge(TYPE_BLOCK_HEADER_BY_HASH, hash)) != null;
    }

    public Transaction getTransaction(byte[] hash) {
        byte[] bytes = indexDB.get(BytesUtils.merge(TYPE_TRANSACTION_INDEX_BY_HASH, hash));
        if (bytes != null) {
            // coinbase transaction
            if (bytes.length > 64) {
                return Transaction.fromBytes(bytes);
            }

            TransactionIndex index = TransactionIndex.fromBytes(bytes);
            byte[] transactions = blockDB
                    .get(BytesUtils.merge(TYPE_BLOCK_TRANSACTIONS_BY_HASH, index.getBlockHash()));
            SimpleDecoder dec = new SimpleDecoder(transactions, index.getResultOffset());
            return Transaction.fromBytes(dec.readBytes());
        }

        return null;
    }

    public Transaction getCoinbaseTransaction(long number) {
        return number == 0
                ? null
                : getTransaction(indexDB.get(BytesUtils.merge(TYPE_MAINBLOCK_COINBASE_BY_NUMBER, BytesUtils.of(number))));
    }

    public boolean hasTransaction(byte[] hash) {
        return indexDB.get(BytesUtils.merge(TYPE_TRANSACTION_INDEX_BY_HASH, hash)) != null;
    }

    public TransactionResult getTransactionResult(byte[] hash) {
        byte[] bytes = indexDB.get(BytesUtils.merge(TYPE_TRANSACTION_INDEX_BY_HASH, hash));
        if (bytes != null) {
            // coinbase transaction
            if (bytes.length > 64) {
                return new TransactionResult();
            }

            TransactionIndex index = TransactionIndex.fromBytes(bytes);
            byte[] results = blockDB.get(BytesUtils.merge(TYPE_BLOCK_RESULTS_BY_HASH, index.getBlockHash()));
            SimpleDecoder dec = new SimpleDecoder(results, index.getResultOffset());
            return TransactionResult.fromBytes(dec.readBytes());
        }

        return null;
    }

    public long getTransactionIndexBlockHash(byte[] hash) {
        Transaction tx = getTransaction(hash);
        if (tx.getType() == TransactionType.COINBASE) {
            return tx.getNonce();
        }

        byte[] bytes = indexDB.get(BytesUtils.merge(TYPE_TRANSACTION_INDEX_BY_HASH, hash));
        if (bytes != null) {
            SimpleDecoder dec = new SimpleDecoder(bytes);
            return dec.readLong();
        }

        return -1;
    }

    public int getTransactionCount(byte[] address) {
        byte[] cnt = indexDB.get(BytesUtils.merge(TYPE_TRANSACTION_COUNT_BY_ADDRESS, address));
        return (cnt == null) ? 0 : BytesUtils.toInt(cnt);
    }

    public List<Transaction> getTransactions(byte[] address, int from, int to) {
        List<Transaction> list = new ArrayList<>();

        int total = getTransactionCount(address);
        for (int i = from; i < total && i < to; i++) {
            byte[] key = getNthTransactionIndexKey(address, i);
            byte[] value = indexDB.get(key);
            list.add(getTransaction(value));
        }

        return list;
    }

    protected byte[] getNthTransactionIndexKey(byte[] address, int n) {
        return BytesUtils.merge(BytesUtils.of(TYPE_TRANSACTION_HASH_BY_ADDRESS_AND_INDEX), address, BytesUtils.of(n));
    }

    public void addBlock(Block block) {
        long number = block.getNumber();
        byte[] hash = block.getHash();

        if (number != genesis.getNumber() && number != latestMainBlock.getNumber() + 1) {
            log.error("Adding wrong block: number = {}, expected = {}", number, latestMainBlock.getNumber() + 1);
            throw new DagchainException("Blocks can only be added sequentially");
        }

        // [1] update block
        blockDB.put(BytesUtils.merge(TYPE_BLOCK_HEADER_BY_HASH, block.getHash()), block.getEncodedHeader());
        blockDB.put(BytesUtils.merge(TYPE_BLOCK_TRANSACTIONS_BY_HASH, block.getHash()), block.getEncodedTransactions());
        blockDB.put(BytesUtils.merge(TYPE_BLOCK_RESULTS_BY_HASH, block.getHash()), block.getEncodedResults());

//        indexDB.put(BytesUtils.merge(TYPE_BLOCK_NUMBER_BY_HASH, hash), BytesUtils.of(number));

        // [2] update transaction indices
        List<Transaction> txs = block.getTransactions();
        Pair<byte[], List<Integer>> transactionIndices = block.getEncodedTransactionsAndIndices();
        Pair<byte[], List<Integer>> resultIndices = block.getEncodedResultsAndIndices();
        XAmount reward = Block.getBlockReward(block, config);

        for (int i = 0; i < txs.size(); i++) {
            Transaction tx = txs.get(i);
            TransactionResult result = block.getResults().get(i);

            TransactionIndex index = new TransactionIndex(hash, transactionIndices.getRight().get(i),
                    resultIndices.getRight().get(i));
            indexDB.put(BytesUtils.merge(TYPE_TRANSACTION_INDEX_BY_HASH, tx.getHash()), index.toBytes());

            // [3] update transaction_by_account index
            addTransactionToAccount(tx, tx.getFrom());
            if (!Arrays.equals(tx.getFrom(), tx.getTo())) {
                addTransactionToAccount(tx, tx.getTo());
            }
        }

        if (number != genesis.getNumber()) {
            // [4] coinbase transaction
            Transaction tx = new Transaction(config.getNetwork(),
                    TransactionType.COINBASE,
                    block.getCoinbase(),
                    reward,
                    XAmount.ZERO,
                    number,
                    block.getTimestamp(),
                    BytesUtils.EMPTY_BYTES);
            tx.sign(Constants.COINBASE_KEY);
            indexDB.put(BytesUtils.merge(TYPE_TRANSACTION_INDEX_BY_HASH, tx.getHash()), tx.toBytes());
            indexDB.put(BytesUtils.merge(TYPE_MAINBLOCK_COINBASE_BY_NUMBER, BytesUtils.of(block.getNumber())), tx.getHash());
            addTransactionToAccount(tx, block.getCoinbase());
        }

        // [5] update latest_block
//        latestBlock = block;
//        indexDB.put(BytesUtils.of(TYPE_LATEST_MAIN_BLOCK_NUMBER), BytesUtils.of(number));
    }

    private static Block getBlock(Database blockDB, byte[] hash, boolean skipResults) {
        byte[] header = blockDB.get(BytesUtils.merge(TYPE_BLOCK_HEADER_BY_HASH, hash));
        byte[] transactions = blockDB.get(BytesUtils.merge(TYPE_BLOCK_TRANSACTIONS_BY_HASH, hash));
        byte[] results = skipResults ? null : blockDB.get(BytesUtils.merge(TYPE_BLOCK_RESULTS_BY_HASH, hash));

        return (header == null) ? null : Block.fromComponents(header, transactions, results);
    }

    private static Block getMainBlock(Database indexDB, Database blockDB, long number, boolean skipResults) {
        // Get Main Block from indexDB
        byte[] mainBlockHash = indexDB.get(BytesUtils.merge(TYPE_MAINBLOCK_HASH_BY_NUMBER, BytesUtils.of(number)));

        // Get Block from blockDB
        byte[] header = blockDB.get(BytesUtils.merge(TYPE_BLOCK_HEADER_BY_HASH, mainBlockHash, BytesUtils.of(number)));
        byte[] transactions = blockDB.get(BytesUtils.merge(TYPE_BLOCK_TRANSACTIONS_BY_HASH, mainBlockHash, BytesUtils.of(number)));
        byte[] results = skipResults ? null : blockDB.get(BytesUtils.merge(TYPE_BLOCK_RESULTS_BY_HASH, mainBlockHash, BytesUtils.of(number)));

        return (header == null) ? null : Block.fromComponents(header, transactions, results);
    }

    protected void addTransactionToAccount(Transaction tx, byte[] address) {
        int total = getTransactionCount(address);
        indexDB.put(getNthTransactionIndexKey(address, total), tx.getHash());
        setTransactionCount(address, total + 1);
    }

    protected void setTransactionCount(byte[] address, int total) {
        indexDB.put(BytesUtils.merge(TYPE_TRANSACTION_COUNT_BY_ADDRESS, address), BytesUtils.of(total));
    }

    private static byte[] getLatestMainBlockNumber(Database indexDB) {
        return indexDB.get(BytesUtils.of(TYPE_LATEST_MAIN_BLOCK_NUMBER));
    }

    private static int getDatabaseVersion(Database indexDB) {
        byte[] version = indexDB.get(BytesUtils.of(TYPE_DATABASE_VERSION));
        return version == null ? 0 : BytesUtils.toInt(version);
    }

    private static void delete(Path directory) throws IOException {
        if (!directory.toFile().exists()) {
            return;
        }

        Files.walkFileTree(directory, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
