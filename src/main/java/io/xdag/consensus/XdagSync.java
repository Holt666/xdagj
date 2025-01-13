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
import io.xdag.Kernel;
import io.xdag.config.*;
import io.xdag.core.*;
import io.xdag.db.BlockStore;
import io.xdag.db.TransactionHistoryStore;
import io.xdag.net.Channel;
import io.xdag.net.ChannelManager;
import io.xdag.net.message.Message;
import io.xdag.net.message.ReasonCode;
import io.xdag.net.message.consensus.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes32;
import org.hyperledger.besu.crypto.SecureRandomProvider;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.xdag.core.ImportResult.*;

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

    private long lastRequestTime;

    // Maximum size of syncMap
    public static final int MAX_SIZE = 500000;
    // Number of keys to remove when syncMap exceeds MAX_SIZE
    public static final int DELETE_NUM = 5000;

    private AtomicBoolean syncDone = new AtomicBoolean(false);

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

        this.checkStateTask = new ScheduledThreadPoolExecutor(1, factory);

        this.blockStore = kernel.getBlockStore();
        this.txHistoryStore = kernel.getTxHistoryStore();
    }

    @Override
    protected void doStart() {
        syncDone.set(false);

        log.debug("Download receiveBlock run...");
        checkStateFuture = checkStateTask.scheduleAtFixedRate(this::checkState, 16, 5, TimeUnit.SECONDS);
    }

    @Override
    protected void doStop() {
        stopStateTask();
        log.debug("sync stop done");
    }

    private void checkState() {
        if (syncDone.get()) {
            stopStateTask();
            return;
        }

        List<Channel> seedChannels = kernel.getChannelMgr().getActiveChannels(config.getNodeSpec().getSeedNodesAddresses());
        long maxBlockNumber = 0L;
        if(CollectionUtils.isNotEmpty(seedChannels)) {
            for (Channel c : seedChannels) {
                long latestBlockNumber = c.getRemotePeer().getLatestBlockNumber();
                if(latestBlockNumber > maxBlockNumber) {
                    maxBlockNumber = latestBlockNumber;
                }
            }
            if(chain.getLatestMainBlockNumber() >= maxBlockNumber) {
                makeSyncDone();
            }
        }
    }

    /**
     * Process blocks in queue and add them to the chain
     */
    public ImportResult importBlock(BlockWrapper blockWrapper) {
        log.debug("Sync importBlock:{}", blockWrapper.getBlock().getHashLow());
        ImportResult importResult = chain.tryToConnect(new Block(new XdagBlock(blockWrapper.getBlock().getXdagBlock().getData().toArray())));

        if (importResult == EXIST) {
            log.debug("Sync Block have exist:{}", blockWrapper.getBlock().getHashLow());
        }
        return importResult;
    }

    public synchronized ImportResult validateAndAddNewBlock(BlockWrapper blockWrapper, Channel channel) {
        blockWrapper.getBlock().parse();
        ImportResult result = importBlock(blockWrapper);
        log.debug("Sync validateAndAddNewBlock:{}, {}", blockWrapper.getBlock().getHashLow(), result);
        switch (result) {
            case EXIST, IMPORTED_BEST, IMPORTED_NOT_BEST, IN_MEM -> syncPopBlock(blockWrapper);
            case NO_PARENT -> {
                if (syncPushBlock(blockWrapper, result.getHashlow())) {
                    log.debug("push block:{}, NO_PARENT {}", blockWrapper.getBlock().getHashLow(), result);
                    sendGetBlock(channel, result.getHashlow());
                }
            }
            case INVALID_BLOCK -> handleInvalidBlock(blockWrapper.getBlock(), channel);
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
                            log.debug("push block:{}, NO_PARENT {}", bw.getBlock().getHashLow(), importResult.getHashlow().toHexString());
                            List<Channel> channels = channelMgr.getActiveChannels();
                            for (Channel channel : channels) {
                                sendGetBlock(channel, importResult.getHashlow());
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

            log.info("sync done, the last main block number = {}", chain.getXdagStats().nmain);
            syncDone.set(true);
            if (config.getEnableTxHistory() && txHistoryStore != null) {
                // Sync done, batch write remaining history
                txHistoryStore.batchSaveTxHistory(null);
            }

            if (config.getEnableGenerateBlock()) {
                log.info("start pow at:{}",
                        FastDateFormat.getInstance("yyyy-MM-dd 'at' HH:mm:ss z").format(new Date()));
                // Check main chain
                kernel.getPow().start();
            } else {
                log.info("A non-mining node, will not generate blocks.");
            }
        }
    }

    private void stopStateTask() {
        if (checkStateFuture != null) {
            checkStateFuture.cancel(true);
        }
        // Shutdown thread pool
        checkStateTask.shutdownNow();
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

    public long sendGetBlock(Channel channel, MutableBytes32 hash) {
        XdagMessage msg = new SyncBlockRequestMessage(hash, chain.getXdagStats());
        log.debug("Request block {}, from node {}", hash, channel.getRemoteAddress());
        channel.getMsgQueue().sendMessage(msg);
        return msg.getRandom();
    }

    @Override
    public void start(long targetHeight) {

    }

    @Override
    public void onMessage(Channel channel, Message msg) {
        if(msg instanceof SyncBlockRequestMessage request) {
            Bytes hash = request.getHash();
            Block block = chain.getBlockByHash(Bytes32.wrap(hash), true);
            if (block != null) {
                log.debug("Process sync block request for block: {}, sending to node: {}",
                        Bytes32.wrap(hash).toHexString(),
                        channel.getRemoteAddress());
                channel.getMessageQueue().sendMessage(new SyncBlockResponseMessage(block, 1));
            }
            updateXdagStats(request);
        } else if(msg instanceof SyncBlockResponseMessage response) {
            Block block = response.getBlock();

            log.debug("processSyncBlock:{}  from node {}", block.getHashLow(), channel.getRemoteAddress());
            BlockWrapper bw = new BlockWrapper(block, response.getTtl() - 1, channel.getRemotePeer());

            validateAndAddNewBlock(bw, channel);
        }
    }

    public void updateXdagStats(XdagMessage message) {
        XdagStats remoteXdagStats = message.getXdagStats();
        chain.getXdagStats().update(remoteXdagStats);
    }

    @Override
    public Progress getProgress() {
        return null;
    }
}
