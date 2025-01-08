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

package io.xdag.net;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.xdag.Kernel;
import io.xdag.Network;
import io.xdag.config.Config;
import io.xdag.core.AbstractXdagLifecycle;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

import io.xdag.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

@Slf4j
public class NodeManager extends AbstractXdagLifecycle {

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

    private final Kernel kernel;
    private final Config config;

    private final ChannelManager channelMgr;
    private final PeerClient client;

    // Queue for nodes pending connection
    private final Deque<Node> connectionQueue = new ConcurrentLinkedDeque<>();

    private final Cache<Node, Long> lastConnect = Caffeine.newBuilder().maximumSize(LRU_CACHE_SIZE).build();

    private final ScheduledExecutorService exec;
    private ScheduledFuture<?> connectFuture;
    private ScheduledFuture<?> fetchFuture;

    public NodeManager(Kernel kernel) {
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

    public void addNode(Node node) {
        connectionQueue.addFirst(node);
        while (queueSize() > MAX_QUEUE_SIZE) {
            connectionQueue.removeLast();
        }
    }

    public void addNodes(Collection<Node> nodes) {
        for (Node node : nodes) {
            addNode(node);
        }
    }


    public Set<Node> getSeedNodes(Network network) {
        Set<Node> nodes = new HashSet<>();

        List<String> seedDnsNames = config.getNodeSpec().getSeedNodesDns(network);
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
                .forEach(address -> nodes.add(new Node(address.getHostAddress(), 8001)));

        return nodes;
    }

    /**
     * Connect to a node in the queue.
     */
    protected void doConnect() {
        Set<InetSocketAddress> activeAddresses = channelMgr.getActiveAddresses();
        Node node;

        while ((node = connectionQueue.pollFirst()) != null && channelMgr.size() < config.getNodeSpec().getNetMaxOutboundConnections()) {
            Long lastTouch = lastConnect.getIfPresent(node);
            long now = TimeUtils.currentTimeMillis();

            if (!client.getNode().equals(node) // self
                    && !(Objects.equals(node.getIp(), client.getIp()) && node.getPort() == client.getPort()) // self
                    && !activeAddresses.contains(node.toAddress()) // connected
                    && (lastTouch == null || lastTouch + RECONNECT_WAIT < now)) {

                XdagChannelInitializer ci = new XdagChannelInitializer(kernel, node);
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
        addNodes(getSeedNodes(config.getNetwork()));
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
            return o instanceof Node && address.equals(((Node) o).toAddress());
        }

        @Override
        public String toString() {
            return getIp() + ":" + getPort();
        }
    }

}
