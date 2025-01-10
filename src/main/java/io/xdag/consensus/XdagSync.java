/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020-2030 The XdagJ Developers
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package io.xdag.consensus;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.SettableFuture;
import io.xdag.Kernel;
import io.xdag.config.*;
import io.xdag.core.*;
import io.xdag.db.BlockStore;
import io.xdag.db.TransactionHistoryStore;
import io.xdag.net.Channel;
import io.xdag.net.ChannelManager;
import io.xdag.net.NodeManager;
import io.xdag.net.Peer;
import io.xdag.net.message.Message;
import io.xdag.net.message.ReasonCode;
import io.xdag.net.message.consensus.*;
import io.xdag.utils.XdagTime;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;
import org.apache.tuweni.bytes.MutableBytes32;
import org.hyperledger.besu.crypto.SecureRandomProvider;

import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.xdag.config.Constants.REQUEST_BLOCKS_MAX_TIME;
import static io.xdag.config.Constants.REQUEST_WAIT;
import static io.xdag.core.ImportResult.*;
import static io.xdag.core.XdagState.*;
import static io.xdag.utils.XdagTime.msToXdagtimestamp;

@Slf4j
@Getter
@Setter
public class XdagSync extends AbstractXdagLifecycle implements SyncManager {

    private static final ThreadFactory factory = new BasicThreadFactory.Builder()
            .namingPattern("sync-%d")
            .daemon(true)
            .build();

    private final Kernel kernel;
    private final Config config;
    private final Blockchain chain;
    private final ChannelManager channelMgr;

    private final Set<String> badPeers = new HashSet<>();
    private final BlockStore blockStore;

    private final ConcurrentHashMap<Long, SettableFuture<Bytes>> sumsRequestMap;
    private final ConcurrentHashMap<Long, SettableFuture<Bytes>> blocksRequestMap;

    private final LinkedList<Long> syncWindow = new LinkedList<>();
    private Status status;

    private final ScheduledExecutorService sendTask;
    private ScheduledFuture<?> sendFuture;
    private long lastRequestTime;

    // Maximum size of syncMap
    public static final int MAX_SIZE = 500000;
    // Number of keys to remove when syncMap exceeds MAX_SIZE
    public static final int DELETE_NUM = 5000;

    private AtomicBoolean syncDone = new AtomicBoolean(false);
    private AtomicBoolean isUpdateXdagStats = new AtomicBoolean(false);

    // Monitor whether to start itself
    private StateListener stateListener;

    /**
     * Queue for blocks with missing links
     */
    private ConcurrentHashMap<Bytes32, Queue<BlockWrapper>> syncMap = new ConcurrentHashMap<>();

    private ScheduledExecutorService checkStateTask;
    private ScheduledFuture<?> checkStateFuture;
    private final TransactionHistoryStore txHistoryStore;

    public XdagSync(Kernel kernel) {
        this.kernel = kernel;
        this.config = kernel.getConfig();

        this.chain = kernel.getBlockchain();
        this.channelMgr = kernel.getChannelMgr();

        this.sendTask = new ScheduledThreadPoolExecutor(1, factory);
        this.sumsRequestMap = new ConcurrentHashMap<>();
        this.blocksRequestMap = new ConcurrentHashMap<>();

        this.stateListener = new StateListener();
        this.checkStateTask = new ScheduledThreadPoolExecutor(1, factory);

        this.blockStore = kernel.getBlockStore();
        this.txHistoryStore = kernel.getTxHistoryStore();
    }

    @Override
    protected void doStart() {
        if (status != Status.SYNCING) {
            status = Status.SYNCING;
            // TODO: Set sync start time/snapshot time
            sendFuture = sendTask.scheduleAtFixedRate(this::syncLoop, 32, 10, TimeUnit.SECONDS);
        }

        log.debug("Download receiveBlock run...");
        new Thread(this.stateListener, "xdag-stateListener").start();
        checkStateFuture = checkStateTask.scheduleAtFixedRate(this::checkState, 64, 5, TimeUnit.SECONDS);
    }

    @Override
    protected void doStop() {

        if (this.stateListener.isRunning) {
            this.stateListener.isRunning = false;
        }

        if (sendFuture != null) {
            sendFuture.cancel(true);
        }

        // Shutdown thread pool
        sendTask.shutdownNow();

        stopStateTask();
        log.debug("sync stop done");
    }

    private void syncLoop() {
        try {
            if (syncWindow.isEmpty()) {
                log.debug("start finding different time periods");
                requestBlocks(0, 1L << 48);
            }

            log.debug("start getting blocks");
            getBlocks();
        } catch (Throwable e) {
            log.error("error when requestBlocks {}", e.getMessage());
        }
    }

    private void checkState() {
        if (!isUpdateXdagStats.get()) {
            return;
        }
        if (syncDone.get()) {
            stopStateTask();
            return;
        }

        XdagStats xdagStats = chain.getXdagStats();
        XdagTopStatus xdagTopStatus = chain.getXdagTopStatus();
        long lastTime = getLastTime();
        long curTime = msToXdagtimestamp(System.currentTimeMillis());
        long curHeight = xdagStats.getNmain();
        long maxHeight = xdagStats.getTotalnmain();
        // Exit the syncOld state based on time and height.
        if (!isSync() && (curHeight >= maxHeight - 512 || lastTime >= curTime - 32 * REQUEST_BLOCKS_MAX_TIME)) {
            log.debug("our node height:{} the max height:{}, set sync state", curHeight, maxHeight);
            setSyncState();
        }
        // Confirm whether the synchronization is complete based on time and height.
        if (curHeight >= maxHeight || xdagTopStatus.getTopDiff().compareTo(xdagStats.maxdifficulty) >= 0) {
            log.debug("our node height:{} the max height:{}, our diff:{} max diff:{}, make sync done",
                    curHeight, maxHeight, xdagTopStatus.getTopDiff(), xdagStats.maxdifficulty);
            makeSyncDone();
        }

    }

    /**
     * Monitor kernel state to determine if it's time to start
     */
    public boolean isTimeToStart() {
        boolean res = false;
        int waitEpoch = config.getNodeSpec().getWaitEpoch();
        if (!isSync() && !isSyncOld() && (XdagTime.getCurrentEpoch() > kernel.getStartEpoch() + waitEpoch)) {
            res = true;
        }
        if (res) {
            log.debug("Waiting time exceeded,starting pow");
        }
        return res;
    }

    /**
     * Process blocks in queue and add them to the chain
     */
    // TODO: Modify consensus
    public ImportResult importBlock(BlockWrapper blockWrapper) {
        log.debug("importBlock:{}", blockWrapper.getBlock().getHashLow());
        ImportResult importResult = chain.tryToConnect(new Block(new XdagBlock(blockWrapper.getBlock().getXdagBlock().getData().toArray())));

        if (importResult == EXIST) {
            log.debug("Block have exist:{}", blockWrapper.getBlock().getHashLow());
        }

        if (!blockWrapper.isOld() && (importResult == IMPORTED_BEST || importResult == IMPORTED_NOT_BEST)) {
            Peer blockPeer = blockWrapper.getRemotePeer();
            NodeManager.Node node = kernel.getClient().getNode();
            if (blockPeer == null || !StringUtils.equals(blockPeer.getIp(), node.getIp()) || blockPeer.getPort() != node.getPort()) {
                if (blockWrapper.getTtl() > 0) {
                    distributeBlock(blockWrapper);
                }
            }
        }
        return importResult;
    }

    public synchronized ImportResult validateAndAddNewBlock(BlockWrapper blockWrapper) {
        blockWrapper.getBlock().parse();
        ImportResult result = importBlock(blockWrapper);
        log.debug("validateAndAddNewBlock:{}, {}", blockWrapper.getBlock().getHashLow(), result);
        switch (result) {
            case EXIST, IMPORTED_BEST, IMPORTED_NOT_BEST, IN_MEM -> syncPopBlock(blockWrapper);
            case NO_PARENT -> {
                if (syncPushBlock(blockWrapper, result.getHashlow())) {
                    log.debug("push block:{}, NO_PARENT {}", blockWrapper.getBlock().getHashLow(), result);
                    List<Channel> channels = channelMgr.getActiveChannels();
                    for (Channel channel : channels) {
                        // if (channel.getRemotePeer().equals(blockWrapper.getRemotePeer())) {
                        sendGetBlock(channel, result.getHashlow(), blockWrapper.isOld());
                        //}
                    }

                }
            }
            case INVALID_BLOCK -> {
//                log.error("invalid block:{}", Hex.toHexString(blockWrapper.getBlock().getHashLow()));
            }
            default -> {
            }
        }
        return result;
    }

    /**
     * Synchronize missing blocks
     *
     * @param blockWrapper New block
     * @param hashLow Hash of missing parent block
     */
    public boolean syncPushBlock(BlockWrapper blockWrapper, Bytes32 hashLow) {
        if (syncMap.size() >= MAX_SIZE) {
            for (int j = 0; j < DELETE_NUM; j++) {
                List<Bytes32> keyList = new ArrayList<>(syncMap.keySet());
                Bytes32 key = keyList.get(SecureRandomProvider.publicSecureRandom().nextInt(keyList.size()));
                assert key != null;
                if (syncMap.remove(key) != null) chain.getXdagStats().nwaitsync--;
            }
        }
        AtomicBoolean r = new AtomicBoolean(true);
        long now = System.currentTimeMillis();

        Queue<BlockWrapper> newQueue = Queues.newConcurrentLinkedQueue();
        blockWrapper.setTime(now);
        newQueue.add(blockWrapper);
        chain.getXdagStats().nwaitsync++;

        syncMap.merge(hashLow, newQueue,
                (oldQ, newQ) -> {
                    chain.getXdagStats().nwaitsync--;
                    for (BlockWrapper b : oldQ) {
                        if (b.getBlock().getHashLow().equals(blockWrapper.getBlock().getHashLow())) {
                            // after 64 sec must resend block request
                            if (now - b.getTime() > 64 * 1000) {
                                b.setTime(now);
                                r.set(true);
                            } else {
                                // TODO: Consider timeout for unreceived request block
                                r.set(false);
                            }
                            return oldQ;
                        }
                    }
                    oldQ.add(blockWrapper);
                    r.set(true);
                    return oldQ;
                });
        return r.get();
    }

    /**
     * Release child blocks based on received block
     */
    public void syncPopBlock(BlockWrapper blockWrapper) {
        Block block = blockWrapper.getBlock();

        Queue<BlockWrapper> queue = syncMap.getOrDefault(block.getHashLow(), null);
        if (queue != null) {
            syncMap.remove(block.getHashLow());
            chain.getXdagStats().nwaitsync--;
            queue.forEach(bw -> {
                ImportResult importResult = importBlock(bw);
                switch (importResult) {
                    case EXIST, IN_MEM, IMPORTED_BEST, IMPORTED_NOT_BEST -> {
                        // TODO: Need to remove after successful import
                        syncPopBlock(bw);
                        queue.remove(bw);
                    }
                    case NO_PARENT -> {
                        if (syncPushBlock(bw, importResult.getHashlow())) {
                            log.debug("push block:{}, NO_PARENT {}", bw.getBlock().getHashLow(),
                                    importResult.getHashlow().toHexString());
                            List<Channel> channels = channelMgr.getActiveChannels();
                            for (Channel channel : channels) {
//                            Peer remotePeer = channel.getRemotePeer();
//                            Peer blockPeer = bw.getRemotePeer();
                                // if (StringUtils.equals(remotePeer.getIp(), blockPeer.getIp()) && remotePeer.getPort() == blockPeer.getPort() ) {
                                sendGetBlock(channel, importResult.getHashlow(), blockWrapper.isOld());
                                //}
                            }
                        }
                    }
                    default -> {
                    }
                }
            });
        }
    }

    // TODO: Currently stays in sync by default, not responsible for block generation
    public void makeSyncDone() {
        if (syncDone.compareAndSet(false, true)) {
            // Stop state check process
            this.stateListener.isRunning = false;
            if (config instanceof MainnetConfig) {
                if (kernel.getXdagState() != XdagState.SYNC) {
                    kernel.setXdagState(XdagState.SYNC);
                }
            } else if (config instanceof TestnetConfig) {
                if (kernel.getXdagState() != XdagState.STST) {
                    kernel.setXdagState(XdagState.STST);
                }
            } else if (config instanceof DevnetConfig) {
                if (kernel.getXdagState() != XdagState.SDST) {
                    kernel.setXdagState(XdagState.SDST);
                }
            }

            log.info("sync done, the last main block number = {}", chain.getXdagStats().nmain);
            setStatus(XdagSync.Status.SYNC_DONE);
            if (config.getEnableTxHistory() && txHistoryStore != null) {
                // Sync done, batch write remaining history
                txHistoryStore.batchSaveTxHistory(null);
            }

            if (config.getEnableGenerateBlock()) {
                log.info("start pow at:{}",
                        FastDateFormat.getInstance("yyyy-MM-dd 'at' HH:mm:ss z").format(new Date()));
                // Check main chain
//                kernel.getMinerServer().start();
                kernel.getPow().start();
            } else {
                log.info("A non-mining node, will not generate blocks.");
            }
        }
    }

    public void setSyncState() {
        if (config instanceof MainnetConfig) {
            kernel.setXdagState(CONN);
        } else if (config instanceof TestnetConfig) {
            kernel.setXdagState(CTST);
        } else if (config instanceof DevnetConfig) {
            kernel.setXdagState(CDST);
        }
    }

    public boolean isSync() {
        return kernel.getXdagState() == CONN || kernel.getXdagState() == CTST || kernel.getXdagState() == CDST;
    }

    public boolean isSyncOld() {
        return kernel.getXdagState() == CONNP || kernel.getXdagState() == CTSTP || kernel.getXdagState() == CDSTP;
    }

    private void stopStateTask() {
        if (checkStateFuture != null) {
            checkStateFuture.cancel(true);
        }
        // Shutdown thread pool
        checkStateTask.shutdownNow();
    }

    public void distributeBlock(BlockWrapper blockWrapper) {
        List<Channel> activeSeedNodes = channelMgr.getActiveChannels(config.getNodeSpec().getSeedNodesAddresses());
        for (Channel channel : activeSeedNodes) {
            NewBlockMessage msg = new NewBlockMessage(blockWrapper.getBlock(), blockWrapper.getTtl());
            channel.getMsgQueue().sendMessage(msg);
        }
    }

    protected void handleInvalidBlock(Block block, Channel channel) {
        InetSocketAddress a = channel.getRemoteAddress();
        log.info("Invalid block, peer = {}:{}, block # = {}", a.getAddress().getHostAddress(), a.getPort(),
                block.getInfo().getHeight());

        badPeers.add(channel.getRemotePeer().getPeerId());

        if (config.getNodeSpec().getSyncDisconnectOnInvalidBlock()) {
            // disconnect if the peer sends us invalid block
            channel.getMessageQueue().disconnect(ReasonCode.BAD_PEER);
        }
    }

    /**
     * Use syncWindow to request blocks in segments
     */
    private void getBlocks() {
        Channel channel = getHightestAvailableChannel();
        if (channel == null) {
            return;
        }
        SettableFuture<Bytes> sf = SettableFuture.create();
        long lastTime = getLastTime();

        // Remove synchronized time periods
        while (!syncWindow.isEmpty() && syncWindow.get(0) < lastTime) {
            syncWindow.pollFirst();
        }

        // Request blocks in segments, 32 time periods per request
        int size = syncWindow.size();
        for (int i = 0; i < 128; i++) {
            if (i >= size) {
                break;
            }
            // Update channel if sync channel is removed/reset
            if (!channel.isActive()){
                log.debug("sync channel need to update");
                return;
            }

            long time = syncWindow.get(i);
            if (time >= lastRequestTime) {
                sendGetBlocks(channel, time, sf);
                lastRequestTime = time;
            }
        }
    }

    /**
     * Request blocks for a time range
     * @param t start time
     * @param dt time interval
     */
    private void requestBlocks(long t, long dt) {
        // Stop sync if not in SYNCING state
        if (status != Status.SYNCING) {
            stop();
            return;
        }

        Channel channel = getHightestAvailableChannel();
        if (channel == null) {
            return;
        }

        SettableFuture<Bytes> sf = SettableFuture.create();
        if (dt > REQUEST_BLOCKS_MAX_TIME) {
            findGetBlocks(channel, t, dt, sf);
        } else {
            if (!isSyncOld() && !isSync()) {
                log.debug("set sync old");
                setSyncOld();
            }

            if (t > getLastTime()) {
                syncWindow.offerLast(t);
            }
        }
    }

    /**
     * Send request to get blocks from remote node
     * @param t request time
     */
    private void sendGetBlocks(Channel channel, long t, SettableFuture<Bytes> sf) {
        long randomSeq = sendGetBlocks(channel, t, t + REQUEST_BLOCKS_MAX_TIME);
        blocksRequestMap.put(randomSeq, sf);
        try {
            sf.get(REQUEST_WAIT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            blocksRequestMap.remove(randomSeq);
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Recursively find time periods to request blocks
     */
    private void findGetBlocks(Channel channel, long t, long dt, SettableFuture<Bytes> sf) {
        MutableBytes lSums = MutableBytes.create(256);
        Bytes rSums;
        if (blockStore.loadSum(t, t + dt, lSums) <= 0) {
            return;
        }

        long randomSeq = sendGetSums(channel, t, t + dt);
        sumsRequestMap.put(randomSeq, sf);
        try {
            Bytes sums = sf.get(REQUEST_WAIT, TimeUnit.SECONDS);
            rSums = sums.copy();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            sumsRequestMap.remove(randomSeq);
            log.error(e.getMessage(), e);
            return;
        }
        sumsRequestMap.remove(randomSeq);
        dt >>= 4;
        for (int i = 0; i < 16; i++) {
            long lSumsSum = lSums.getLong(i * 16, ByteOrder.LITTLE_ENDIAN);
            long lSumsSize = lSums.getLong(i * 16 + 8, ByteOrder.LITTLE_ENDIAN);
            long rSumsSum = rSums.getLong(i * 16, ByteOrder.LITTLE_ENDIAN);
            long rSumsSize = rSums.getLong(i * 16 + 8, ByteOrder.LITTLE_ENDIAN);

            if (lSumsSize != rSumsSize || lSumsSum != rSumsSum) {
                requestBlocks(t + i * dt, dt);
            }
        }
    }

    public void setSyncOld() {
        if (config instanceof MainnetConfig) {
            if (kernel.getXdagState() != XdagState.CONNP) {
                kernel.setXdagState(XdagState.CONNP);
            }
        } else if (config instanceof TestnetConfig) {
            if (kernel.getXdagState() != XdagState.CTSTP) {
                kernel.setXdagState(XdagState.CTSTP);
            }
        } else if (config instanceof DevnetConfig) {
            if (kernel.getXdagState() != XdagState.CDSTP) {
                kernel.setXdagState(XdagState.CDSTP);
            }
        }
    }

    /**
     * Get timestamp of latest confirmed main block
     */
    public long getLastTime() {
        long height = blockStore.getXdagStatus().nmain;
        if(height == 0) return 0;
        Block lastBlock = blockStore.getBlockByHeight(height);
        if (lastBlock != null) {
            return lastBlock.getTimestamp();
        }
        return 0;
    }

    public List<Channel> getAvailableChannelsByHight() {
        return channelMgr.getActiveChannels(config.getNodeSpec().getSeedNodesAddresses()).stream()
                .filter(channel -> {
                    Peer peer = channel.getRemotePeer();
                    // the peer has the block
                    return peer.getLatestBlockNumber() > chain.getLatestMainBlockNumber()
                            // AND is not banned
                            && !badPeers.contains(peer.getPeerId());
                }).sorted(Comparator.comparingLong(channel -> channel.getRemotePeer().getLatestBlockNumber()))
                .collect(Collectors.toList());
    }

    public Channel getHightestAvailableChannel() {
        List<Channel> channels = getAvailableChannelsByHight();
        if(CollectionUtils.isNotEmpty(channels)) {
            return channels.get(0);
        }
        return null;
    }

    public long sendGetBlocks(Channel channel, long startTime, long endTime) {
        log.debug("Request blocks between {} and {} from node {}",
                FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS").format(XdagTime.xdagTimestampToMs(startTime)),
                FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS").format(XdagTime.xdagTimestampToMs(endTime)),
                channel.getRemoteAddress());
        BlocksRequestMessage msg = new BlocksRequestMessage(startTime, endTime, chain.getXdagStats());
        channel.getMsgQueue().sendMessage(msg);
        return msg.getRandom();
    }

    public long sendGetBlock(Channel channel, MutableBytes32 hash, boolean isOld) {
        XdagMessage msg;
        //        log.debug("sendGetBlock:[{}]", Hex.toHexString(hash));
        msg = isOld ? new SyncBlockRequestMessage(hash, chain.getXdagStats())
                : new BlockRequestMessage(hash, chain.getXdagStats());
        log.debug("Request block {} isold: {} from node {}", hash, isOld, channel.getRemoteAddress());
        channel.getMsgQueue().sendMessage(msg);
        return msg.getRandom();
    }

    public long sendGetSums(Channel channel, long startTime, long endTime) {
        SumRequestMessage msg = new SumRequestMessage(startTime, endTime, chain.getXdagStats());
        channel.getMsgQueue().sendMessage(msg);
        return msg.getRandom();
    }

    @Override
    public void start(long targetHeight) {

    }

    @Override
    public void onMessage(Channel channel, Message msg) {

    }

    @Override
    public Progress getProgress() {
        return null;
    }

    public enum Status {
        /**
         * Sync states
         */
        SYNCING, SYNC_DONE
    }

    private class StateListener implements Runnable {

        volatile boolean isRunning = false;

        @Override
        public void run() {
            this.isRunning = true;
            while (this.isRunning) {
                if (isTimeToStart()) {
                    makeSyncDone();
                }
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }
}
