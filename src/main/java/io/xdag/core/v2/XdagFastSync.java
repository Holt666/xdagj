package io.xdag.core.v2;

import io.xdag.config.Config;
import io.xdag.net.Capability;
import io.xdag.net.Peer;
import io.xdag.net.message.Message;
import io.xdag.net.message.consensus.v2.GetBlockPartsMessage;
import io.xdag.net.message.consensus.v2.BlockMessage;
import io.xdag.net.message.consensus.v2.BlockPartsMessage;
import io.xdag.net.message.consensus.v2.GetBlockMessage;
import io.xdag.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class XdagFastSync implements SyncManagerV2 {

    private static final ThreadFactory factory = new ThreadFactory() {
        private final AtomicInteger cnt = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "fast-sync-" + cnt.getAndIncrement());
        }
    };

    private static final ScheduledExecutorService timer1 = Executors.newSingleThreadScheduledExecutor(factory);
    private static final ScheduledExecutorService timer2 = Executors.newSingleThreadScheduledExecutor(factory);
    private static final ScheduledExecutorService timer3 = Executors.newSingleThreadScheduledExecutor(factory);

    private final long DOWNLOAD_TIMEOUT;

    private final int MAX_QUEUED_JOBS;
    private final int MAX_PENDING_JOBS;
    private final int MAX_PENDING_BLOCKS;

    private static final Random random = new Random();

    private final Config config;

    private final Dagchain chain;
    private final XdagChannelManager channelMgr;

    // task queues
    private final AtomicLong latestQueuedTask = new AtomicLong();

    // Blocks to download
    private final TreeSet<Long> toDownload = new TreeSet<>();

    // Blocks which were requested but haven't been received
    private final Map<Long, Long> toReceive = new HashMap<>();

    // Blocks which were received but haven't been validated
    private final TreeSet<Pair<Block, XdagChannel>> toValidate = new TreeSet<>(
            Comparator.comparingLong(o -> o.getKey().getNumber()));

    // Blocks which were validated but haven't been imported
    private final TreeMap<Long, Pair<Block, XdagChannel>> toImport = new TreeMap<>();

    private final Object lock = new Object();

    // current and target heights
    private final AtomicLong begin = new AtomicLong();
    private final AtomicLong current = new AtomicLong();
    private final AtomicLong target = new AtomicLong();
    private final AtomicLong lastObserved = new AtomicLong();

    private final AtomicLong beginningTimestamp = new AtomicLong();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    // reset at the beginning of a sync task
    private final Set<String> badPeers = new HashSet<>();

    public XdagFastSync(Config config, Dagchain chain, XdagChannelManager channelMgr) {
        this.config = config;

        this.chain = chain;
        this.channelMgr = channelMgr;

        this.DOWNLOAD_TIMEOUT = 1_000L;
        this.MAX_QUEUED_JOBS = 10_000;
        this.MAX_PENDING_JOBS = 200;
        this.MAX_PENDING_BLOCKS = 2_000;
    }

    @Override
    public void start(long targetHeight) {
        if (isRunning.compareAndSet(false, true)) {
            beginningTimestamp.set(System.currentTimeMillis());

            badPeers.clear();

            log.info("Syncing started, best known block = {}", targetHeight - 1);

            // [1] set up queues
            synchronized (lock) {
                toDownload.clear();
                toReceive.clear();
                toValidate.clear();
                toImport.clear();

                begin.set(chain.getLatestMainBlockNumber() + 1);
                current.set(chain.getLatestMainBlockNumber() + 1);
                target.set(targetHeight);
                lastObserved.set(chain.getLatestMainBlockNumber());
                latestQueuedTask.set(chain.getLatestMainBlockNumber());
                growToDownloadQueue();
            }

            // [2] start tasks
            ScheduledFuture<?> download = timer1.scheduleAtFixedRate(this::download, 0, 500, TimeUnit.MICROSECONDS);
            ScheduledFuture<?> process = timer2.scheduleAtFixedRate(this::process, 0, 1000, TimeUnit.MICROSECONDS);
            ScheduledFuture<?> reporter = timer3.scheduleAtFixedRate(() -> {
                long newBlockNumber = chain.getLatestMainBlockNumber();
                log.info(
                        "Syncing status: importing {} blocks per second, {} to download, {} to receive, {} to validate, {} to import",
                        (newBlockNumber - lastObserved.get()) / 30,
                        toDownload.size(),
                        toReceive.size(),
                        toValidate.size(),
                        toImport.size());
                lastObserved.set(newBlockNumber);
            }, 30, 30, TimeUnit.SECONDS);

            // [3] wait until the sync is done
            while (isRunning.get()) {
                synchronized (isRunning) {
                    try {
                        isRunning.wait(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.info("Sync manager got interrupted");
                        break;
                    }
                }
            }

            // [4] cancel tasks
            download.cancel(true);
            process.cancel(false);
            reporter.cancel(true);
            Instant end = Instant.now();
            log.info("Syncing finished, took {}",
                    TimeUtils.formatDuration(Duration.between(Instant.ofEpochMilli(beginningTimestamp.get()), end)));
        }
    }

    @Override
    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            synchronized (isRunning) {
                isRunning.notifyAll();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning.get();
    }

    protected void addBlock(Block block, XdagChannel channel) {
        synchronized (lock) {
            if (toDownload.remove(block.getNumber())) {
                growToDownloadQueue();
            }
            toReceive.remove(block.getNumber());
            toValidate.add(Pair.of(block, channel));
        }
    }

    @Override
    public void onMessage(XdagChannel channel, Message msg) {
        if (!isRunning()) {
            return;
        }

        switch (msg.getCode()) {
            case MAIN_BLOCK: {
                BlockMessage blockMsg = (BlockMessage) msg;
                Block block = blockMsg.getBlock();
                addBlock(block, channel);
                break;
            }
            case MAIN_BLOCK_PARTS: {
                // try re-construct a block
                BlockPartsMessage blockPartsMsg = (BlockPartsMessage) msg;
                List<BlockPart> parts = BlockPart.decode(blockPartsMsg.getParts());
                List<byte[]> data = blockPartsMsg.getData();

                // sanity check
                if (parts.size() != data.size()) {
                    log.debug("Part set and data do not match");
                    break;
                }

                // parse the data
                byte[] header = null, transactions = null, results = null;
                for (int i = 0; i < parts.size(); i++) {
                    if (parts.get(i) == BlockPart.HEADER) {
                        header = data.get(i);
                    } else if (parts.get(i) == BlockPart.TRANSACTIONS) {
                        transactions = data.get(i);
                    } else if (parts.get(i) == BlockPart.RESULTS) {
                        results = data.get(i);
                    } else {
                        // unknown
                    }
                }

                // import block
                try {
                    Block block = Block.fromComponents(header, transactions, results);
                    addBlock(block, channel);
                } catch (Exception e) {
                    log.debug("Failed to parse a block from components", e);
                }
                break;
            }
            //case MAIN_BLOCK_HEADER: // deprecated
            default: {
                break;
            }
        }
    }

    private boolean isFastSyncSupported(Peer peer) {
        return Stream.of(peer.getCapabilities()).anyMatch(c -> Capability.FAST_SYNC.name().equals(c));
    }

    private void download() {
        if (!isRunning()) {
            return;
        }

        synchronized (lock) {
            // filter all expired tasks
            long now = TimeUtils.currentTimeMillis();
            Iterator<Map.Entry<Long, Long>> itr = toReceive.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry<Long, Long> entry = itr.next();

                if (entry.getValue() + DOWNLOAD_TIMEOUT < now) {
                    log.debug("Failed to download block #{}, expired", entry.getKey());
                    toDownload.add(entry.getKey());
                    itr.remove();
                }
            }

            // quit if too many unfinished jobs
            if (toReceive.size() > MAX_PENDING_JOBS) {
                log.trace("Max pending jobs reached");
                return;
            }

            // quit if no more tasks
            if (toDownload.isEmpty()) {
                return;
            }
            Long task = toDownload.first();

            // quit if too many pending blocks
            int pendingBlocks = toValidate.size() + toImport.size();
            if (pendingBlocks > MAX_PENDING_BLOCKS && task > toValidate.first().getKey().getNumber()) {
                log.trace("Max pending blocks reached");
                return;
            }

            // get idle channels
            List<XdagChannel> channels = channelMgr.getIdleChannels().stream()
                    .filter(channel -> {
                        Peer peer = channel.getRemotePeer();
                        // the peer has the block
                        return peer.getLatestBlockNumber() >= task
                                // AND is not banned
                                && !badPeers.contains(peer.getPeerId())
                                // AND supports FAST_SYNC if we enabled this protocol
                                && (!config.getNodeSpec().getSyncFastSync() || isFastSyncSupported(peer));
                    })
                    .collect(Collectors.toList());
            log.trace("Qualified idle peers = {}", channels.size());

            // quit if no idle channels.
            if (channels.isEmpty()) {
                return;
            }
            // otherwise, pick a random channel
            XdagChannel c = channels.get(random.nextInt(channels.size()));

            if (config.getNodeSpec().getSyncFastSync()) { // use FAST_SYNC protocol
                log.trace("Requesting block #{} from {}:{}, HEADER + TRANSACTIONS", task,
                        c.getRemoteIp(),
                        c.getRemotePort());
                c.getMessageQueue().sendMessage(new GetBlockPartsMessage(task,
                        BlockPart.encode(BlockPart.HEADER, BlockPart.TRANSACTIONS)));

            } else { // use old protocol
                log.trace("Requesting block #{} from {}:{}, FULL BLOCK", task, c.getRemoteIp(),
                        c.getRemotePort());
                c.getMessageQueue().sendMessage(new GetBlockMessage(task));
            }

            if (toDownload.remove(task)) {
                growToDownloadQueue();
            }
            toReceive.put(task, TimeUtils.currentTimeMillis());
        }
    }

    private void growToDownloadQueue() {
        // To avoid overhead, this method doesn't add new tasks before the queue is less
        // than half-filled
        if (toDownload.size() >= MAX_QUEUED_JOBS / 2) {
            return;
        }

        for (long task = latestQueuedTask.get() + 1; //
             task < target.get() && toDownload.size() < MAX_QUEUED_JOBS; //
             task++) {
            latestQueuedTask.accumulateAndGet(task, (prev, next) -> next > prev ? next : prev);
            if (!chain.hasMainBlock(task)) {
                toDownload.add(task);
            }
        }
    }

    protected void process() {
        if (!isRunning()) {
            return;
        }

        long latest = chain.getLatestMainBlockNumber();
        if (latest + 1 >= target.get()) {
            stop();
            return; // This is important because stop() only notify
        }

        // find the check point
        long checkpoint = latest + 1;
//        while (skipVotes(checkpoint)) {
//            checkpoint++;
//        }

        synchronized (lock) {
            // Move blocks from validate queue to import queue if within range
            Iterator<Pair<Block, XdagChannel>> iterator = toValidate.iterator();
            while (iterator.hasNext()) {
                Pair<Block, XdagChannel> p = iterator.next();
                long n = p.getKey().getNumber();

                if (n <= latest) {
                    iterator.remove();
                } else if (n <= checkpoint) {
                    iterator.remove();
                    toImport.put(n, p);
                } else {
                    break;
                }
            }

            if (toImport.size() >= checkpoint - latest) {
                // Validate the block hashes
                boolean valid = validateBlockHashes(latest + 1, checkpoint);

                if (valid) {
                    for (long n = latest + 1; n <= checkpoint; n++) {
                        Pair<Block, XdagChannel> p = toImport.remove(n);
                        boolean imported = chain.importBlock(p.getKey());
                        if (!imported) {
                            handleInvalidBlock(p.getKey(), p.getValue());
                            break;
                        }

                        if (n == checkpoint) {
                            log.info("{}", p.getLeft());
                        }
                    }
                    current.set(chain.getLatestMainBlockNumber() + 1);
                }
            }
        }
    }

    protected boolean validateBlockHashes(long from, long to) {
        synchronized (lock) {
            for (long n = to - 1; n >= from; n--) {
                Pair<Block, XdagChannel> current = toImport.get(n);
                Pair<Block, XdagChannel> child = toImport.get(n + 1);

                if (!Arrays.equals(current.getKey().getHash(), child.getKey().getParentHash())) {
                    handleInvalidBlock(current.getKey(), current.getValue());
                    return false;
                }
            }

            return true;
        }
    }

    protected void handleInvalidBlock(Block block, XdagChannel channel) {
        InetSocketAddress a = channel.getRemoteAddress();
        log.info("Invalid block, peer = {}:{}, block # = {}", a.getAddress().getHostAddress(), a.getPort(),
                block.getNumber());
        synchronized (lock) {
            // add to the request queue
            toDownload.add(block.getNumber());

            toReceive.remove(block.getNumber());
            toValidate.remove(Pair.of(block, channel));
            toImport.remove(block.getNumber());
        }

        badPeers.add(channel.getRemotePeer().getPeerId());

//        if (config.syncDisconnectOnInvalidBlock()) {
//            // disconnect if the peer sends us invalid block
//            channel.getMessageQueue().disconnect(ReasonCode.BAD_PEER);
//        }
    }

    @Override
    public XdagSyncProgressV2 getProgress() {
        return new XdagSyncProgressV2(
                begin.get(),
                current.get(),
                target.get(),
                Duration.between(Instant.ofEpochMilli(beginningTimestamp.get()), Instant.now()));
    }

    public static class XdagSyncProgressV2 implements Progress {

        final long startingHeight;

        final long currentHeight;

        final long targetHeight;

        final Duration duration;

        public XdagSyncProgressV2(long startingHeight, long currentHeight, long targetHeight, Duration duration) {
            this.startingHeight = startingHeight;
            this.currentHeight = currentHeight;
            this.targetHeight = targetHeight;
            this.duration = duration;
        }

        @Override
        public long getStartingHeight() {
            return startingHeight;
        }

        @Override
        public long getCurrentHeight() {
            return currentHeight;
        }

        @Override
        public long getTargetHeight() {
            return targetHeight;
        }

        @Override
        public Duration getSyncEstimation() {
            long durationInSeconds = duration.toSeconds();
            long imported = currentHeight - startingHeight;
            long remaining = targetHeight - currentHeight;

            if (imported == 0) {
                return null;
            } else {
                return Duration.ofSeconds(remaining * durationInSeconds / imported);
            }
        }
    }
}
