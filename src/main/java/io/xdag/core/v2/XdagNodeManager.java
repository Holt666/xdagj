package io.xdag.core.v2;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.xdag.config.Config;
import io.xdag.core.AbstractXdagLifecycle;
import io.xdag.net.NodeManager;
import io.xdag.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

@Slf4j
public class XdagNodeManager extends AbstractXdagLifecycle  {

    // Thread naming pattern
    private static final String THREAD_NAME_PATTERN = "NodeManager-thread-%d";

    private static final ThreadFactory factory = new BasicThreadFactory.Builder()
            .namingPattern(THREAD_NAME_PATTERN)
            .daemon(true)
            .build();

    // Node queue and cache related constants
    private static final long MAX_QUEUE_SIZE = 1024;
    private static final int LRU_CACHE_SIZE = 1024;
    private static final long RECONNECT_WAIT = 60L * 1000L;

    private final KernelV2 kernel;
    private final Config config;

    private final XdagChannelManager channelMgr;
    private final XdagPeerClient client;

    // Queue for nodes pending connection
    private final Deque<XdagNodeManager.Node> connectionQueue = new ConcurrentLinkedDeque<>();

    private final Cache<XdagNodeManager.Node, Long> lastConnect = Caffeine.newBuilder().maximumSize(LRU_CACHE_SIZE).build();

    private final ScheduledExecutorService exec;
    private ScheduledFuture<?> connectFuture;
    private ScheduledFuture<?> fetchFuture;

    public XdagNodeManager(KernelV2 kernel) {
        this.kernel = kernel;
        this.config = kernel.getConfig();

        this.channelMgr = kernel.getChannelMgr();
        this.client = kernel.getClient();

        this.exec = Executors.newSingleThreadScheduledExecutor(factory);
    }

    @Override
    protected void doStart() {
        connectFuture = exec.scheduleAtFixedRate(this::doConnect, 1000, 500, TimeUnit.MILLISECONDS);
        fetchFuture = exec.scheduleAtFixedRate(this::doFetch, 5, 100, TimeUnit.SECONDS);
        log.info("Node manager started");
    }

    @Override
    protected void doStop() {
        connectFuture.cancel(true);
        fetchFuture.cancel(true);
        log.debug("Node manager stopped");
    }

    public void addNode(XdagNodeManager.Node node) {
        connectionQueue.addFirst(node);
        while (queueSize() > MAX_QUEUE_SIZE) {
            connectionQueue.removeLast();
        }
    }

    public void addNodes(Collection<XdagNodeManager.Node> nodes) {
        for (XdagNodeManager.Node node : nodes) {
            addNode(node);
        }
    }


    public Set<XdagNodeManager.Node> getSeedNodes() {
        Set<XdagNodeManager.Node> nodes = new HashSet<>();

        List<String> seedDnsNames = config.getNodeSpec().getSeedNodesDns();
        seedDnsNames.parallelStream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .map(name -> {
                    try {
                        return InetAddress.getAllByName(name);
                    } catch (UnknownHostException e) {
                        log.warn("Failed to get seed nodes from {}", name);
                        return new InetAddress[0];
                    }
                })
                .flatMap(Stream::of)
                .forEach(address -> nodes.add(new XdagNodeManager.Node(address.getHostAddress(), 8001)));

        return nodes;
    }

    /**
     * Connect to a node in the queue.
     */
    protected void doConnect() {
        Set<InetSocketAddress> activeAddresses = channelMgr.getActiveAddresses();
        XdagNodeManager.Node node;

        while ((node = connectionQueue.pollFirst()) != null && channelMgr.size() < config.getNodeSpec().getNetMaxOutboundConnections()) {
            Long lastTouch = lastConnect.getIfPresent(node);
            long now = TimeUtils.currentTimeMillis();

            if (!client.getNode().equals(node) // self
                    && !(Objects.equals(node.getIp(), client.getIp()) && node.getPort() == client.getPort()) // self
                    && !activeAddresses.contains(node.toAddress()) // connected
                    && (lastTouch == null || lastTouch + RECONNECT_WAIT < now)) {

                XdagChannelInitializerV2 ci = new XdagChannelInitializerV2(kernel, node);
                client.connect(node, ci);
                lastConnect.put(node, now);
                break;
            }
        }
    }

    /**
     * Fetches seed nodes from DNS records or configuration.
     */
    protected void doFetch() {
        addNodes(getSeedNodes());
    }

    /**
     * Get the current size of the connection queue
     */
    public int queueSize() {
        return connectionQueue.size();
    }

    public static class Node {

        private final InetSocketAddress address;

        public Node(InetSocketAddress address) {
            this.address = address;
        }

        public Node(InetAddress ip, int port) {
            this(new InetSocketAddress(ip, port));
        }


        public Node(String ip, int port) {
            this(new InetSocketAddress(ip, port));
        }


        public String getIp() {
            return address.getAddress().getHostAddress();
        }

        public int getPort() {
            return address.getPort();
        }

        public InetSocketAddress toAddress() {
            return address;
        }

        @Override
        public int hashCode() {
            return address.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof NodeManager.Node && address.equals(((NodeManager.Node) o).toAddress());
        }

        @Override
        public String toString() {
            return getIp() + ":" + getPort();
        }
    }

}
