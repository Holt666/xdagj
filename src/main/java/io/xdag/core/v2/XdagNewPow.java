package io.xdag.core.v2;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.xdag.Network;
import io.xdag.config.Config;
import io.xdag.core.v2.state.AccountState;
import io.xdag.crypto.Keys;
import io.xdag.net.message.Message;
import io.xdag.net.message.ReasonCode;
import io.xdag.net.message.consensus.v2.NewBlockMessage;
import io.xdag.net.message.consensus.v2.TransactionMessage;
import io.xdag.utils.*;
import io.xdag.core.v2.XdagNewPow.Event.Type;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hyperledger.besu.crypto.KeyPair;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class XdagNewPow {

    protected KernelV2 kernel;
    protected Config config;

    protected Dagchain chain;

    protected XdagChannelManager channelMgr;
    protected PendingManager pendingMgr;
    protected SyncManagerV2 syncMgr;

    protected KeyPair coinbase;

    protected Timer timer;
    protected Broadcaster broadcaster;
    protected BlockingQueue<Event> events = new LinkedBlockingQueue<>();

    protected Status status;
    protected State state;

    protected long height;

    protected List<String> seedNodesAddresses;
    protected List<XdagChannel> activeSeedNodes;

//    protected Proposal proposal;

    protected Cache<ByteArray, Block> validBlocks = Caffeine.newBuilder().maximumSize(8).build();

    protected long lastUpdate;

    public XdagNewPow(KernelV2 kernel) {
        this.kernel = kernel;
        this.config = kernel.getConfig();

        this.chain = kernel.getDagchain();
        this.channelMgr = kernel.getChannelMgr();
        this.pendingMgr = kernel.getPendingMgr();
        this.syncMgr = kernel.getSync();
        this.coinbase = kernel.getCoinbase();

        this.timer = new Timer();
        this.broadcaster = new Broadcaster();

        this.status = Status.STOPPED;
        this.state = State.NEW_HEIGHT;

        this.seedNodesAddresses = config.getNodeSpec().getSeedNodesAddresses();
    }

    /**
     * Pause the bft manager, and do synchronization.
     */
    protected void sync(long target) {
        if (status == Status.RUNNING) {
            // change status
            status = Status.SYNCING;

            // reset timer, and events
            clearTimerAndEvents();

            // start syncing
            syncMgr.start(target);

            // restore status if not stopped
            if (status != Status.STOPPED) {
                status = Status.RUNNING;

                // enter new height
                enterNewHeight();
            }
        }
    }

    public void start() {
        if (status == Status.STOPPED) {
            status = Status.RUNNING;
            timer.start();
            broadcaster.start();
            log.info("Xdag Pow manager started");

            enterNewHeight();
            eventLoop();

            log.info("Xdag Pow manager stopped");
        }
    }

    /**
     * Main loop that processes all the BFT events.
     */
    protected void eventLoop() {
        while (!Thread.currentThread().isInterrupted() && status != Status.STOPPED) {
            try {
                Event ev = events.take();
                if (status != Status.RUNNING) {
                    continue;
                }

                // in case we get stuck at one height for too long
                if (lastUpdate + 2 * 60 * 1000L < TimeUtils.currentTimeMillis()) {
                    updateSeedNodes();
                }

                switch (ev.getType()) {
                    case STOP:
                        return;
                    case TIMEOUT:
                        onTimeout();
                        break;
                    case MAIN_BLOCK:
                        onBlock(ev.getData());
                        break;
                    default:
                        break;
                }
            } catch (InterruptedException e) {
                log.info("BftManager got interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.warn("Unexpected exception in event loop", e);
            }
        }
    }

    /**
     * Enter the NEW_HEIGHT state
     */
    protected void enterNewHeight() {
        state = State.NEW_HEIGHT;

        // update previous block
        Block prevBlock = chain.getLatestMainBlock();

        // update view state
        height = prevBlock.getNumber() + 1;

        // update seed nodes
        updateSeedNodes();

        // reset events
        clearTimerAndEvents();

        log.info("Entered new_height: height = {}, # activeSeedNodes = {}", height, activeSeedNodes.size());
        if (isSeedNode()) {
            if (this.config.getNetwork()== Network.MAINNET && !SystemUtils.bench()) {
                log.error("You need to upgrade your computer to join the XDAG consensus!");
                SystemUtils.exitAsync(SystemUtils.Code.HARDWARE_UPGRADE_NEEDED);
            }
            resetTimeout(TimeUnit.SECONDS.toMillis(64));
        }
    }

    protected boolean isSeedNode() {
        return seedNodesAddresses.contains(WalletUtils.toBase58(coinbase));
    }

    /**
     * Reset timer and events.
     */
    protected void clearTimerAndEvents() {
        timer.clear();
        events.clear();
    }

    protected void resetTimeout(long timeout) {
        timer.timeout(timeout);

        events.removeIf(e -> e.type == Type.TIMEOUT);
    }

    /**
     * Enter the PROPOSE state
     */
    protected void enterPropose() {
        state = State.PROPOSE;
        resetTimeout(0);
        updateSeedNodes();
        log.info("Entered propose: height = {}, activeSeedNodes = {}, # connected validators = 1 + {}", height, isSeedNode(), activeSeedNodes.size());

        if (isSeedNode()) {
            Block block = proposalBlock();
            chain.importBlock(block);
            broadcaster.broadcast(new NewBlockMessage(block));
        }
    }

    /**
     * Timeout handler
     */
    protected void onTimeout() {
        switch (state) {
            case NEW_HEIGHT:
                enterPropose();
                break;
            case PROPOSE:
                enterNewHeight();
                break;
        }
    }

    /** on New Block Message */
    protected void onBlock(Block mb) {
        if(validateBlockProposal(mb)) {

        }
    }

    public boolean isRunning() {
        return status == Status.RUNNING;
    }

    public void onMessage(XdagChannel channel, Message msg) {
        if (!isRunning()) {
            return;
        }

        switch (msg.getCode()) {
            case TRANSACTION: {
                TransactionMessage m = (TransactionMessage) msg;

                // update peer height state
                events.add(new Event(Type.TRANSACTION, m.getTransaction()));
                break;
            }
            case NEW_MAIN_BLOCK: {
                NewBlockMessage m = (NewBlockMessage) msg;
                Block mb = m.getBlock();
                if (mb.getHeader().validate()) {
                    events.add(new Event(Type.MAIN_BLOCK, mb));
                } else {
                    log.debug("Invalid Block from {}", channel.getRemotePeer().getPeerId());
                    channel.getMessageQueue().disconnect(ReasonCode.BAD_PEER);
                }

            }
            default: {
                break;
            }
        }
    }

    /**
     * Update the validator sets.
     */
    protected void updateSeedNodes() {
        int maxSeedNodes = config.getNodeSpec().getMaxSeedNodes();

        seedNodesAddresses = config.getNodeSpec().getSeedNodesAddresses();
        if (seedNodesAddresses.size() > maxSeedNodes) {
            seedNodesAddresses = seedNodesAddresses.subList(0, maxSeedNodes);
        }
        activeSeedNodes = channelMgr.getActiveChannels(seedNodesAddresses);
        lastUpdate = TimeUtils.currentTimeMillis();
    }

    /**
     * Create a block for proposal.
     *
     * @return the proposed block
     */
    protected Block proposalBlock() {
        AccountState asTrack = chain.getAccountState().track();
        long t1 = TimeUtils.currentTimeMillis();

        // construct block template
        BlockHeader parent = chain.getLatestMainBlock().getHeader();
        long number = height;
        byte[] prevHash = parent.getHash();
        long timestamp = TimeUtils.currentTimeMillis();
        timestamp = timestamp > parent.getTimestamp() ? timestamp : parent.getTimestamp() + 1;
        //byte[] data = chain.constructMainBlockHeaderDataField();

        // fetch pending transactions
        final List<PendingManager.PendingTransaction> pendingTxs = pendingMgr.getPendingTransactions(100);
        final List<Transaction> includedTxs = new ArrayList<>();
        final List<TransactionResult> includedResults = new ArrayList<>();

        TransactionExecutor exec = new TransactionExecutor(config);

        for (PendingManager.PendingTransaction pendingTx : pendingTxs) {
            Transaction tx = pendingTx.transaction;

            // re-evaluate the transaction
            TransactionResult result = exec.execute(tx, asTrack);
            if (result.getCode().isAcceptable()) {
                includedTxs.add(tx);
                includedResults.add(result);
            }
        }

        // compute roots
        byte[] transactionsRoot = MerkleUtils.computeTransactionsRoot(includedTxs);
        byte[] resultsRoot = MerkleUtils.computeResultsRoot(includedResults);
        byte[] stateRoot = BytesUtils.EMPTY_HASH;
        byte[] data = BytesUtils.EMPTY_BYTES;

        byte[] nbits = BytesUtils.EMPTY_BYTES;
        byte[] nonce = BytesUtils.EMPTY_BYTES;
        List<byte[]> witnessBlockHashes = Collections.emptyList();


        BlockHeader header = new BlockHeader(number, Keys.toBytesAddress(coinbase), prevHash, timestamp, transactionsRoot, resultsRoot, stateRoot, data, nbits, nonce, witnessBlockHashes);
        Block block = new Block(header, includedTxs, includedResults);

        long t2 = TimeUtils.currentTimeMillis();
        log.debug("Block creation: # txs = {}, time = {} ms", includedTxs.size(), t2 - t1);

        return block;
    }

    /**
     * Check if a block proposal is valid.
     */
    protected boolean validateBlockProposal(Block block) {
        try {
            AccountState asTrack = chain.getAccountState().track();
            long t1 = TimeUtils.currentTimeMillis();

            BlockHeader header = block.getHeader();
            List<Transaction> transactions = block.getTransactions();

            // [1] check block header
            // Block latestBlock = chain.getLatestMainBlock();
            if (header == null || !header.validate()) {
                log.warn("Invalid block header");
                return false;
            }

            // [?] additional checks by consensus
            // - disallow block time drifting;
            // - restrict the coinbase to be the proposer
            if (header.getTimestamp() - TimeUtils.currentTimeMillis() > TimeUnit.SECONDS.toMillis(300)) {
                log.warn("A block in the future is not allowed");
                return false;
            }
//            if (!Arrays.equals(header.getCoinbase(), proposal.getSignature().getAddress())) {
//                log.warn("The coinbase should always equal to the proposer's address");
//                return false;
//            }

            // [2] check transactions
            List<Transaction> unvalidatedTransactions = getUnvalidatedTransactions(transactions);
            if (!block.validateTransactions(header, unvalidatedTransactions, transactions, config.getNetwork())) {
                log.warn("Invalid transactions");
                return false;
            }

            // [3] filter un existing transactions
            List<Transaction> unExcludeTxs = transactions.stream().filter(tx -> !chain.hasTransaction(tx.getHash())).toList();
//            if (transactions.stream().anyMatch(tx -> chain.hasTransaction(tx.getHash()))) {
//                log.warn("Duplicated transaction hash is not allowed");
//                return false;
//            }

            // [4] evaluate transactions
            TransactionExecutor transactionExecutor = new TransactionExecutor(config);

            List<TransactionResult> results = transactionExecutor.execute(unExcludeTxs, asTrack);
            if (!block.validateResults(header, results)) {
                log.error("Invalid transaction results");
                return false;
            }
            block.setResults(results); // overwrite the results

            long t2 = TimeUtils.currentTimeMillis();
            log.debug("Block validation: # txs = {}, time = {} ms", transactions.size(), t2 - t1);

            validBlocks.put(ByteArray.of(block.getHash()), block);
            return true;
        } catch (Exception e) {
            log.error("Unexpected exception during block proposal validation", e);
            return false;
        }
    }

    protected List<Transaction> getUnvalidatedTransactions(List<Transaction> transactions) {
        Set<Transaction> pendingValidatedTransactions = pendingMgr.getPendingTransactions()
                .stream()
                .map(pendingTx -> pendingTx.transaction)
                .collect(Collectors.toSet());

        return transactions
                .stream()
                .filter(it -> !pendingValidatedTransactions.contains(it))
                .collect(Collectors.toList());
    }

    public enum State {
        NEW_HEIGHT, PROPOSE
    }

    public class Timer implements Runnable {
        private long timeout;

        private Thread t;

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                synchronized (this) {
                    if (timeout != -1 && timeout < TimeUtils.currentTimeMillis()) {
                        events.add(new Event(Type.TIMEOUT));
                        timeout = -1;
                        continue;
                    }
                }

                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        public synchronized void start() {
            if (t == null) {
                t = new Thread(this, "bft-timer");
                t.start();
            }
        }

        public synchronized void stop() {
            if (t != null) {
                try {
                    t.interrupt();
                    t.join(10000);
                } catch (InterruptedException e) {
                    log.warn("Failed to stop consensus timer");
                    Thread.currentThread().interrupt();
                }
                t = null;
            }
        }

        public synchronized void timeout(long milliseconds) {
            if (milliseconds < 0) {
                throw new IllegalArgumentException("Timeout can not be negative");
            }
            timeout = TimeUtils.currentTimeMillis() + milliseconds;
        }

        public synchronized void clear() {
            timeout = -1;
        }
    }

    public class Broadcaster implements Runnable {
        private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();

        private Thread t;

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Message msg = queue.take();

                    // thread-safety via volatile
                    List<XdagChannel> channels = activeSeedNodes;
                    if (channels != null) {
                        int[] indices = ArrayUtils.permutation(channels.size());
                        for (int i = 0; i < indices.length && i < config.getNodeSpec().getNetRelayRedundancy(); i++) {
                            XdagChannel c = channels.get(indices[i]);
                            if (c.isActive()) {
                                c.getMessageQueue().sendMessage(msg);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        public synchronized void start() {
            if (t == null) {
                t = new Thread(this, "bft-relay");
                t.start();
            }
        }

        public synchronized void stop() {
            if (t != null) {
                try {
                    t.interrupt();
                    t.join();
                } catch (InterruptedException e) {
                    log.error("Failed to stop consensus broadcaster");
                    Thread.currentThread().interrupt();
                }
                t = null;
            }
        }

        public void broadcast(Message msg) {
            if (!queue.offer(msg)) {
                log.error("Failed to add a message to the broadcast queue: msg = {}", msg);
            }
        }
    }

    public static class Event {
        public enum Type {
            /**
             * Stop signal
             */
            STOP,

            /**
             * Received a timeout signal.
             */
            TIMEOUT,


            /**
             * Received a transaction message.
             */
            TRANSACTION,

            /**
             * Received a main block message.
             */
            MAIN_BLOCK;
        }

        @Getter
        private final Type type;
        private final Object data;

        public Event(Type type) {
            this(type, null);
        }

        public Event(Type type, Object data) {
            this.type = type;
            this.data = data;
        }

        @SuppressWarnings("unchecked")
        public <T> T getData() {
            return (T) data;
        }

        @Override
        public String toString() {
            return "Event [type=" + type + ", data=" + data + "]";
        }
    }

    public enum Status {
        STOPPED, RUNNING, SYNCING
    }

}
