package io.xdag.core.v2;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.xdag.core.v2.state.AccountState;
import io.xdag.net.message.consensus.v2.TransactionMessage;
import io.xdag.utils.BytesUtils;
import io.xdag.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class PendingManager implements Runnable, DagchainListener {

    private static final ThreadFactory factory = new ThreadFactory() {

        private final AtomicInteger cnt = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "pending-" + cnt.getAndIncrement());
        }
    };

    public static final long ALLOWED_TIME_DRIFT = TimeUnit.HOURS.toMillis(2);

    private static final int QUEUE_SIZE_LIMIT = 128 * 1024;
    private static final int VALID_TXS_LIMIT = 16 * 1024;
    private static final int LARGE_NONCE_TXS_LIMIT = 32 * 1024;
    private static final int PROCESSED_TXS_LIMIT = 128 * 1024;

    private final KernelV2 kernel;
    //private final DagStore dagStore;
    private AccountState pendingAS;

    // Transactions that haven't been processed
    private final LinkedHashMap<ByteArray, Transaction> queue = new LinkedHashMap<>();

    // Transactions that have been processed and are valid for block production
    private final ArrayList<PendingTransaction> validTxs = new ArrayList<>();

    // Transactions whose nonce is too large, compared to the sender's nonce
    private final Cache<ByteArray, Transaction> largeNonceTxs = Caffeine.newBuilder().maximumSize(LARGE_NONCE_TXS_LIMIT).build();

    // Transactions that have been processed, including both valid and invalid ones
    private final Cache<ByteArray, Long> processedTxs = Caffeine.newBuilder().maximumSize(PROCESSED_TXS_LIMIT).build();

    private final ScheduledExecutorService exec;

    private ScheduledFuture<?> validateFuture;

    private volatile boolean isRunning;

    /**
     * Creates a pending manager.
     */
    public PendingManager(KernelV2 kernel) {
        this.kernel = kernel;
        this.pendingAS = kernel.getDagchain().getAccountState().track();
        this.exec = Executors.newSingleThreadScheduledExecutor(factory);
    }

    /**
     * Starts this pending manager.
     */
    public synchronized void start() {
        if (!isRunning) {
            /*
             * NOTE: a rate smaller than the message queue sending rate should be used to
             * prevent message queues from hitting the NET_MAX_QUEUE_SIZE, especially when
             * the network load is heavy.
             */
            this.validateFuture = exec.scheduleAtFixedRate(this, 2, 2, TimeUnit.MILLISECONDS);

            kernel.getDagchain().addListener(this);

            log.debug("Pending manager started");
            this.isRunning = true;
        }
    }

    /**
     * Shuts down this pending manager.
     */
    public synchronized void stop() {
        if (isRunning) {
            validateFuture.cancel(true);

            log.debug("Pending manager stopped");
            isRunning = false;
        }
    }

    public synchronized boolean isRunning() {
        return isRunning;
    }

    public synchronized List<Transaction> getQueue() {
        return new ArrayList<>(queue.values());
    }

    public synchronized void addTransaction(Transaction tx) {
        ByteArray hash = ByteArray.of(tx.getHash());

        if (queue.size() < QUEUE_SIZE_LIMIT
                && processedTxs.getIfPresent(hash) == null
                && tx.validate(kernel.getConfig().getNetwork())) {
            // NOTE: re-insertion doesn't affect item order
            queue.put(ByteArray.of(tx.getHash()), tx);
        }
    }

    public synchronized ProcessingResult addTransactionSync(Transaction tx) {
        // nonce check for transactions from this client
        if (tx.getNonce() != getNonce(tx.getFrom())) {
            return new ProcessingResult(0, TransactionResult.Code.INVALID_NONCE);
        }

        if (tx.validate(kernel.getConfig().getNetwork())) {
            // proceed with the tx, ignoring transaction queue size limit
            return processTransaction(tx, false, true);
        } else {
            return new ProcessingResult(0, TransactionResult.Code.INVALID_FORMAT);
        }
    }

    public synchronized long getNonce(byte[] address) {
        return pendingAS.getAccount(address).getNonce();
    }

    public synchronized List<PendingTransaction> getPendingTransactions(int size) {
        List<PendingTransaction> txs = new ArrayList<>();
        Iterator<PendingTransaction> it = validTxs.iterator();

        while (it.hasNext() && size > 0) {
            PendingTransaction tx = it.next();
            txs.add(tx);
            size--;
        }

        return txs;
    }

    public synchronized List<PendingTransaction> getPendingTransactions() {
        return getPendingTransactions(100);
    }

    public synchronized List<PendingTransaction> reset() {
        // reset state
        pendingAS = kernel.getDagchain().getAccountState().track();
        //dummyBlock = kernel.createEmptyBlock();

        // clear transaction pool
        List<PendingTransaction> txs = new ArrayList<>(validTxs);
        validTxs.clear();

        return txs;
    }

    @Override
    public synchronized void onMainBlockAdded(Block block) {
        if (isRunning) {
            long t1 = TimeUtils.currentTimeMillis();

            // clear transaction pool
            List<PendingTransaction> txs = reset();

            // update pending state
            long accepted = 0;
            for (PendingTransaction tx : txs) {
                accepted += processTransaction(tx.transaction, true, false).accepted;
            }

            long t2 = TimeUtils.currentTimeMillis();
            log.debug("Execute pending transactions: # txs = {} / {},  time = {} ms", accepted, txs.size(), t2 - t1);
        }
    }

    @Override
    public synchronized void run() {
        Iterator<Map.Entry<ByteArray, Transaction>> iterator = queue.entrySet().iterator();

        while (validTxs.size() < VALID_TXS_LIMIT && iterator.hasNext()) {
            // the eldest entry
            Map.Entry<ByteArray, Transaction> entry = iterator.next();
            iterator.remove();

            // reject already executed transactions
            if (processedTxs.getIfPresent(entry.getKey()) != null) {
                continue;
            }

            // process the transaction
            int accepted = processTransaction(entry.getValue(), false, false).accepted;
            processedTxs.put(entry.getKey(), TimeUtils.currentTimeMillis());

            // include one tx per call
            if (accepted > 0) {
                break;
            }
        }
    }

    /**
     * Validates the given transaction and add to pool if success.
     */
    protected ProcessingResult processTransaction(Transaction tx, boolean isIncludedBefore, boolean isFromThisNode) {
        int cnt = 0;
        long now = TimeUtils.currentTimeMillis();

        // reject VM transactions that come in before fork
//        if (tx.isVMTransaction() && !kernel.getBlockchain().isForkActivated(Fork.VIRTUAL_MACHINE)) {
//            return new ProcessingResult(0, TransactionResult.Code.INVALID_TYPE);
//        }

        // reject VM transaction with low gas price or high gas limit
//        if (tx.isVMTransaction()) {
//            if (tx.getGasPrice().lessThan(kernel.getConfig().poolMinGasPrice())
//                    || tx.getGas() > kernel.getConfig().poolMaxTxGasLimit()) {
//                return new ProcessingResult(0, TransactionResult.Code.INVALID_FEE);
//            }
//        }

        // reject transactions with a duplicated tx hash
        if (kernel.getDagchain().hasTransaction(tx.getHash())) {
            return new ProcessingResult(0, TransactionResult.Code.INVALID);
        }

        // check transaction timestamp if this is a fresh transaction:
        // a time drift of 2 hours is allowed by default
//        if (tx.getTimestamp() < now - kernel.getConfig().poolMaxTxTimeDrift()
//                || tx.getTimestamp() > now + kernel.getConfig().poolMaxTxTimeDrift()) {
//            return new ProcessingResult(0, TransactionResult.Code.INVALID_TIMESTAMP);
//        }

        // report INVALID_NONCE error to prevent the transaction from being
        // silently ignored due to a low nonce
        if (tx.getNonce() < getNonce(tx.getFrom())) {
            return new ProcessingResult(0, TransactionResult.Code.INVALID_NONCE);
        }

        // Check transaction nonce: pending transactions must be executed sequentially
        // by nonce in ascending order. In case of a nonce jump, the transaction is
        // delayed for the next event loop of PendingManager.
        while (tx != null && tx.getNonce() == getNonce(tx.getFrom())) {

            // execute transactions
            AccountState as = pendingAS.track();
            TransactionResult result = new TransactionExecutor(kernel.getConfig()).execute(tx, as);

            if (result.getCode().isAcceptable()) {
                // commit state updates
                as.commit();

                // Add the successfully processed transaction into the pool of transactions
                // which are ready to be proposed to the network.
                PendingTransaction pendingTransaction = new PendingTransaction(tx, result);
                validTxs.add(pendingTransaction);
                cnt++;

                // If a transaction is not included before, send it to the network now
                if (!isIncludedBefore) {
                    // if it is from myself, broadcast it to everyone
                    broadcastTransaction(tx, isFromThisNode);
                }
            } else {
                // exit immediately if invalid
                return new ProcessingResult(cnt, result.getCode());
            }

            tx = largeNonceTxs.getIfPresent(createKey(tx.getFrom(), getNonce(tx.getFrom())));
            isIncludedBefore = false; // A large-nonce transaction is not included before
        }

        // Delay the transaction for the next event loop of PendingManager. The delayed
        // transaction is expected to be processed once PendingManager has received
        // all of its preceding transactions from the same address.
        if (tx != null && tx.getNonce() > getNonce(tx.getFrom())) {
            largeNonceTxs.put(createKey(tx), tx);
        }

        return new ProcessingResult(cnt);
    }

    private void broadcastTransaction(Transaction tx, boolean toAllPeers) {
        List<XdagChannel> channels = kernel.getChannelMgr().getActiveChannels();

        // If not to all peers, randomly pick n channels
        int n = kernel.getConfig().getNodeSpec().getNetRelayRedundancy();
        if (!toAllPeers && channels.size() > n) {
            Collections.shuffle(channels);
            channels = channels.subList(0, n);
        }

        // Send the message
        TransactionMessage msg = new TransactionMessage(tx);
        for (XdagChannel c : channels) {
            if (c.isActive()) {
                c.getMessageQueue().sendMessage(msg);
            }
        }
    }

    private ByteArray createKey(Transaction tx) {
        return ByteArray.of(BytesUtils.merge(tx.getFrom(), BytesUtils.of(tx.getNonce())));
    }

    private ByteArray createKey(byte[] acc, long nonce) {
        return ByteArray.of(BytesUtils.merge(acc, BytesUtils.of(nonce)));
    }

    public static class PendingTransaction {

        public final Transaction transaction;

        public final TransactionResult result;

        public PendingTransaction(Transaction transaction, TransactionResult result) {
            this.transaction = transaction;
            this.result = result;
        }
    }

    public static class ProcessingResult {

        public final int accepted;

        public final TransactionResult.Code error;

        public ProcessingResult(int accepted, TransactionResult.Code error) {
            this.accepted = accepted;
            this.error = error;
        }

        public ProcessingResult(int accepted) {
            this.accepted = accepted;
            this.error = null;
        }
    }

}
