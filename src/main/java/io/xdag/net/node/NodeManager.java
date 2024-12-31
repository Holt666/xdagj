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

package io.xdag.net.node;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.netty.channel.socket.SocketChannel;
import io.xdag.Kernel;
import io.xdag.Network;
import io.xdag.config.Config;
import io.xdag.core.AbstractXdagLifecycle;
import io.xdag.net.PeerClient;
import io.xdag.net.XdagChannelInitializer;
import io.xdag.net.ChannelManager;
import io.xdag.net.message.p2p.NodesMessage;
import io.xdag.utils.WalletUtils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

@Slf4j
public class NodeManager extends AbstractXdagLifecycle {

    private static final ThreadFactory factory = new BasicThreadFactory.Builder()
            .namingPattern("NodeManager-thread-%d")
            .daemon(true)
            .build();

    private static final long MAX_QUEUE_SIZE = 1024;
    private static final int LRU_CACHE_SIZE = 1024;
    private static final long RECONNECT_WAIT = 60L * 1000L;
    private static final long MIN_IP_UPDATE_INTERVAL = 60 * 1000; // Minimum 60 seconds between IP updates
    private final Deque<Node> deque = new ConcurrentLinkedDeque<>();

    // List of authorized addresses
    @Getter
    protected final Set<String> authorizedAddresses = Collections.synchronizedSet(new HashSet<>());

    private final Cache<Node, Long> lastConnect = Caffeine.newBuilder().maximumSize(LRU_CACHE_SIZE).build();
    private final ScheduledExecutorService exec;
    private final Kernel kernel;
    private final PeerClient client;
    private final ChannelManager channelMgr;
    private final Config config;
    private ScheduledFuture<?> connectFuture;
    private ScheduledFuture<?> fetchFuture;

    // Map to track node information
    private final Map<String, NodeInfo> publicKeyToNode = new ConcurrentHashMap<>();
    @Getter
    private final Map<String, Long> lastUpdateTime = new ConcurrentHashMap<>();
    // Map to track last IP update time for each address
    private final Map<String, Long> lastIpUpdateTime = new ConcurrentHashMap<>();

    // Map to track address-to-IP mapping
    private final Map<String, String> addressToIp = new ConcurrentHashMap<>();

    // Map to track address-channel mappings
    private final Map<String, SocketChannel> addressChannels = new ConcurrentHashMap<>();

    private static final int CLEANUP_INTERVAL = 60; // 60 seconds

    public NodeManager(Kernel kernel) {
        this.kernel = kernel;
        this.client = kernel.getClient();
        this.channelMgr = kernel.getChannelMgr();
        this.exec = new ScheduledThreadPoolExecutor(1, factory);
        this.config = kernel.getConfig();
    }

    @Override
    protected void doStart() {
        try {
            // 1. Start scheduled tasks first
            // Delay connect task to allow system initialization
            connectFuture = exec.scheduleAtFixedRate(this::doConnect, 3000, 1000, TimeUnit.MILLISECONDS);
            
            // Delay fetch task - this will handle fetching nodes periodically
            fetchFuture = exec.scheduleAtFixedRate(this::doFetch, 10, 100, TimeUnit.SECONDS);
            
            // Delay cleanup task
            exec.scheduleAtFixedRate(this::cleanupExpiredNodes, 15, CLEANUP_INTERVAL, TimeUnit.SECONDS);

            // 2. Schedule initial authorized addresses fetch
            exec.schedule(this::initializeAuthorizedAddresses, 5, TimeUnit.SECONDS);

            log.info("Node manager started successfully");
            
        } catch (Exception e) {
            log.error("Failed to start node manager", e);
            throw new RuntimeException("Node manager start failed", e);
        }
    }

    @Override
    protected void doStop() {
        if (connectFuture != null) {
            connectFuture.cancel(true);
        }
        if (fetchFuture != null) {
            fetchFuture.cancel(true);
        }
        exec.shutdown();
        log.debug("Node manager stopped");
    }

    /**
     * Initialize authorized addresses from seed nodes
     */
    protected void initializeAuthorizedAddresses() {
        try {
            Set<Node> seedNodes = getSeedNodes(config.getNetwork());
            if (!seedNodes.isEmpty()) {
                log.info("Found {} seed nodes from DNS, fetching authorized addresses", seedNodes.size());
                fetchAuthorizedAddressesFromSeedNodes(seedNodes);
            } else {
                log.warn("No seed nodes found from DNS during initialization");
            }
        } catch (Exception e) {
            log.error("Failed to initialize authorized addresses", e);
        }
    }

    /**
     * Fetches seed nodes from DNS records or configuration.
     */
    protected void doFetch() {
        try {
            // Get seed nodes from DNS
            Set<Node> seedNodes = getSeedNodes(config.getNetwork());
            if (!seedNodes.isEmpty()) {
                log.info("Found {} seed nodes from DNS", seedNodes.size());
                addNodes(seedNodes);
                
                // Only try to fetch authorized addresses if we lost all authorizations and connections
                if (authorizedAddresses.isEmpty() && channelMgr.size() == 0) {
                    log.warn("No authorized addresses and no active connections, attempting to re-fetch from seed nodes");
                    fetchAuthorizedAddressesFromSeedNodes(seedNodes);
                }
            } else {
                log.warn("No seed nodes found from DNS");
            }
        } catch (Exception e) {
            log.error("Failed to fetch nodes", e);
        }
    }

    /**
     * Fetch authorized addresses from seed nodes
     * Since we have multiple seed nodes for redundancy, we don't need complex retry logic
     */
    protected void fetchAuthorizedAddressesFromSeedNodes(Set<Node> seedNodes) {
        if (seedNodes == null || seedNodes.isEmpty()) {
            return;
        }

        // Connect to each seed node to get authorized addresses
        // No need to verify seed nodes as they are trusted and obtained from DNS
        for (Node node : seedNodes) {
            try {
                XdagChannelInitializer initializer = new XdagChannelInitializer(kernel, false, node);
                client.connect(node, initializer);
            } catch (Exception e) {
                log.warn("Failed to connect to seed node {}", node, e);
            }
        }
    }

    public int queueSize() {
        return deque.size();
    }

    public void addNodes(Collection<Node> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return;
        }
        for (Node node : nodes) {
            addNode(node);
        }
    }

    public void addNode(Node node) {
        if (deque.contains(node)) {
            return;
        }
        deque.addFirst(node);
        while (queueSize() > MAX_QUEUE_SIZE) {
            deque.removeLast();
        }
    }

    /**
     * Get seed nodes from DNS records.
     */
    public Set<Node> getSeedNodes(Network network) {
        Set<Node> nodes = new HashSet<>();

        List<String> names;
        switch (network) {
            case MAINNET:
                names = kernel.getConfig().getNodeSpec().getDnsSeedsMainNet();
                break;
            case TESTNET:
                names = kernel.getConfig().getNodeSpec().getDnsSeedsTestNet();
                break;
            default:
                return nodes;
        }

        names.parallelStream()
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
                .forEach(address -> {
                    Node node = new Node(address.getHostAddress(), 8001);
                    nodes.add(node);
                    log.debug("Added seed node: {}", node);
                });

        return nodes;
    }

    /**
     * Attempt to connect to a node
     */
    public void doConnect() {
        Set<InetSocketAddress> activeAddress = channelMgr.getActiveAddresses();
        Node node;
        while ((node = deque.pollFirst()) != null && channelMgr.size() < config.getNodeSpec().getMaxConnections()) {
            Long lastCon = lastConnect.getIfPresent(node);
            long now = System.currentTimeMillis();

            // Only connect if:
            // 1. Not connecting to self
            // 2. Not already connected
            // 3. Not too soon since last connection attempt
            // 4. Node is authorized (except for seed nodes)
            if (!client.getNode().equals(node)
                    && !node.equals(client.getNode())
                    && !activeAddress.contains(node.getAddress())
                    && (lastCon == null || lastCon + RECONNECT_WAIT < now)
                    && (isSeedNode(node) || isAddressAuthorized(node.getIp()))) {
                XdagChannelInitializer initializer = new XdagChannelInitializer(kernel, false, node);
                client.connect(node, initializer);
                lastConnect.put(node, now);
                break;
            }
        }
    }

    /**
     * Check if a node is a seed node
     */
    private boolean isSeedNode(Node node) {
        List<String> dnsSeeds = config.getNodeSpec().getNetwork() == Network.MAINNET ? 
                config.getNodeSpec().getDnsSeedsMainNet() : 
                config.getNodeSpec().getDnsSeedsTestNet();

        try {
            for (String seed : dnsSeeds) {
                InetAddress[] addresses = InetAddress.getAllByName(seed);
                for (InetAddress address : addresses) {
                    if (address.getHostAddress().equals(node.getIp())) {
                        return true;
                    }
                }
            }
        } catch (UnknownHostException e) {
            log.warn("Failed to resolve DNS seed", e);
        }
        return false;
    }

    /**
     * Connect to a specific node by IP and port
     */
    public void doConnect(String ip, int port) {
        Set<InetSocketAddress> activeAddresses = channelMgr.getActiveAddresses();
        Node remotenode = new Node(ip, port);
        if (!client.getNode().equals(remotenode) 
                && !activeAddresses.contains(remotenode.toAddress())
                && (isSeedNode(remotenode) || isAddressAuthorized(ip))) {
            XdagChannelInitializer initializer = new XdagChannelInitializer(kernel, false, remotenode);
            client.connect(remotenode, initializer);
        }
    }

    /**
     * Process node info from handshake
     * @param nodeInfo The node info from handshake
     * @return true if the node info is valid and accepted, false otherwise
     */
    public boolean processNodeInfo(NodeInfo nodeInfo) {
        try {
            // 1. Verify node info signature and expiration
            if (!nodeInfo.verify() || nodeInfo.isExpired()) {
                log.warn("Invalid or expired node info from {}", nodeInfo.getIpString());
                return false;
            }

            // 2. Verify node is in authorized addresses list
            String address = nodeInfo.getAddress();
            if (!isAddressAuthorized(address)) {
                log.warn("Node {} not in authorized addresses list", address);
                return false;
            }

            // 3. Check IP update time restriction
            String existingIp = addressToIp.get(address);
            String newIp = nodeInfo.getIpString();
            long now = System.currentTimeMillis();

            if (existingIp == null) {
                // New authorized address, allow IP registration
                log.info("Registering new authorized address {} with IP {}", address, newIp);
            } else if (!existingIp.equals(newIp)) {
                // Existing address trying to update IP
                Long lastIpUpdate = lastIpUpdateTime.get(address);
                if (lastIpUpdate != null && (now - lastIpUpdate) < MIN_IP_UPDATE_INTERVAL) {
                    log.warn("Reject IP update for {}: too soon since last update ({} seconds ago)", 
                            address, (now - lastIpUpdate) / 1000);
                    return false;
                }
                log.info("Allowing IP update for {}: {} -> {}", address, existingIp, newIp);
            }

            // 4. Check for existing node with same public key
            NodeInfo existingNode = publicKeyToNode.get(address);
            if (existingNode != null) {
                // Compare timestamps
                if (nodeInfo.getTimestamp() <= existingNode.getTimestamp()) {
                    log.warn("Reject outdated node info update from {}", address);
                    return false;
                }

                // Remove old node info
                InetSocketAddress oldAddress = new InetSocketAddress(
                        existingNode.getIpString(),
                        existingNode.getPort()
                );
                deque.remove(new Node(oldAddress));
                log.info("Removed old node info for {}: {}:{}",
                        address,
                        existingNode.getIpString(),
                        existingNode.getPort()
                );
            }

            // 5. Update node information
            publicKeyToNode.put(address, nodeInfo);
            lastUpdateTime.put(address, now);
            
            // Update IP mapping and last update time
            // For both new registrations and IP updates
            addressToIp.put(address, newIp);
            lastIpUpdateTime.put(address, now);
            
            addNode(new Node(nodeInfo.getIpString(), nodeInfo.getPort()));

            log.info("{} node info for {}: {}:{}",
                    existingIp == null ? "Registered" : "Updated",
                    address,
                    nodeInfo.getIpString(),
                    nodeInfo.getPort()
            );

            return true;
        } catch (Exception e) {
            log.error("Failed to process node info", e);
            return false;
        }
    }

    /**
     * Clean up expired nodes
     */
    protected void cleanupExpiredNodes() {
        long now = System.currentTimeMillis();
        Set<String> expiredAddresses = new HashSet<>();

        // 1. Find expired nodes
        for (Map.Entry<String, NodeInfo> entry : publicKeyToNode.entrySet()) {
            String address = entry.getKey();
            NodeInfo nodeInfo = entry.getValue();
            Long lastUpdate = lastUpdateTime.get(address);

            // Check if node has expired
            if (nodeInfo.isExpired() || 
                lastUpdate == null || 
                (now - lastUpdate) > NodeInfo.MAX_TIMESTAMP_DRIFT) {
                expiredAddresses.add(address);
                log.debug("Node expired: {} (last update: {})", address, 
                    lastUpdate != null ? now - lastUpdate + "ms ago" : "never");
            }
        }

        // 2. Remove expired nodes
        for (String address : expiredAddresses) {
            NodeInfo nodeInfo = publicKeyToNode.remove(address);
            lastUpdateTime.remove(address);
            addressToIp.remove(address);
            lastIpUpdateTime.remove(address);
            if (nodeInfo != null) {
                Node node = new Node(nodeInfo.getIpString(), nodeInfo.getPort());
                deque.remove(node);
                log.info("Removed expired node: {}", address);
            }
        }
    }

    /**
     * Update local node information and notify connected peers
     * @param nodeInfo The new node info
     * @return true if the update was successful, false otherwise
     */
    public boolean updateLocalNodeInfo(NodeInfo nodeInfo) {
        try {
            // 1. Verify node info
            if (!nodeInfo.verify() || nodeInfo.isExpired()) {
                log.warn("Invalid or expired node info");
                return false;
            }

            // 2. Process the update
            if (!processNodeInfo(nodeInfo)) {
                log.warn("Failed to process node info update");
                return false;
            }

            log.info("Successfully updated node info: {}", nodeInfo);
            return true;

        } catch (Exception e) {
            log.error("Failed to update local node info", e);
            return false;
        }
    }

    /**
     * Check if an address is authorized
     */
    public boolean isAddressAuthorized(String address) {
        if (address == null || address.isEmpty()) {
            return false;
        }
        try {
            // If the address is already in base58 format, use it directly
            if (WalletUtils.checkAddress(address)) {
                return authorizedAddresses.contains(address);
            }
            return false;
        } catch (Exception e) {
            log.warn("Invalid address format: {}", address);
            return false;
        }
    }

    /**
     * Check if the IP matches the authorized address's registered IP
     */
    public boolean checkIpForAddress(byte[] authorizedAddress, String ip) {
        if (authorizedAddress == null || ip == null) {
            return false;
        }

        try {
            String addressBase58 = WalletUtils.toBase58(authorizedAddress);
            String existingIp = addressToIp.get(addressBase58);

            // If address not registered yet, allow registration
            if (existingIp == null) {
                addressToIp.put(addressBase58, ip);
                log.info("New address-IP binding: {} -> {}", addressBase58, ip);
                return true;
            }

            // Check if IP matches existing registration
            return ip.equals(existingIp);
        } catch (Exception e) {
            log.warn("Invalid address format", e);
            return false;
        }
    }

    /**
     * Record channel for an authorized address
     */
    public void recordAddressChannel(String address, SocketChannel channel) {
        if (address != null && channel != null) {
            try {
                // If the address is already in base58 format, use it directly
                if (WalletUtils.checkAddress(address)) {
                    addressChannels.put(address, channel);
                    log.debug("Recorded channel for address: {}", address);
                } else {
                    log.warn("Invalid base58 address format: {}", address);
                }
            } catch (Exception e) {
                log.warn("Invalid address format: {}", address, e);
            }
        }
    }

    /**
     * Remove authorization for an address and disconnect its channel
     */
    public void removeAuthorization(String address) {
        if (address == null) {
            return;
        }

        try {
            // If the address is already in base58 format, use it directly
            if (!WalletUtils.checkAddress(address)) {
                log.warn("Invalid base58 address format: {}", address);
                return;
            }

            // Remove from authorized addresses
            synchronized (authorizedAddresses) {
                authorizedAddresses.remove(address);
            }

            // Get and remove channel
            SocketChannel channel = addressChannels.remove(address);
            if (channel != null) {
                // Disconnect the channel
                channel.close();
                log.info("Disconnected peer {} due to authorization removed", address);
            }

            // Clean up other mappings
            addressToIp.remove(address);
            lastIpUpdateTime.remove(address);
            NodeInfo nodeInfo = publicKeyToNode.remove(address);
            if (nodeInfo != null) {
                Node node = new Node(nodeInfo.getIpString(), nodeInfo.getPort());
                deque.remove(node);
            }
        } catch (Exception e) {
            log.warn("Invalid address format: {}", address, e);
        }
    }

    /**
     * Process nodes message for node discovery
     * Only seed nodes can update the authorized addresses list
     */
    public void processNodes(NodesMessage msg, String senderIp) {
        boolean isSeedNode = isSeedNodeByIp(senderIp);
        
        // Process nodes list for discovery
        if (msg.getNodes() != null) {
            for (NodeInfo nodeInfo : msg.getNodes()) {
                // Add to node queue if:
                // 1. Message is from a seed node, or
                // 2. The node's address is already authorized
                if (isSeedNode || authorizedAddresses.contains(nodeInfo.getAddress())) {
                    addNode(new Node(nodeInfo.getIpString(), nodeInfo.getPort()));
                }
            }
        }

        // Only seed nodes can update authorized addresses list
        if (isSeedNode && msg.getAuthorizedAddresses() != null) {
            Set<String> newAuthorizedAddresses = new HashSet<>(msg.getAuthorizedAddresses());
            
            // Find addresses that are no longer authorized
            Set<String> removedAddresses = new HashSet<>(authorizedAddresses);
            removedAddresses.removeAll(newAuthorizedAddresses);
            
            // Remove authorization and disconnect channels for removed addresses
            for (String address : removedAddresses) {
                removeAuthorization(address);
            }
            
            // Update authorized addresses list
            synchronized (authorizedAddresses) {
                authorizedAddresses.clear();
                authorizedAddresses.addAll(newAuthorizedAddresses);
                log.info("Updated authorized addresses from seed node {}, total count: {}", 
                        senderIp, authorizedAddresses.size());
            }
        }
    }

    /**
     * Check if an IP belongs to a seed node
     */
    private boolean isSeedNodeByIp(String ip) {
        List<String> dnsSeeds = config.getNodeSpec().getNetwork() == Network.MAINNET ? 
                config.getNodeSpec().getDnsSeedsMainNet() : 
                config.getNodeSpec().getDnsSeedsTestNet();

        try {
            for (String seed : dnsSeeds) {
                InetAddress[] addresses = InetAddress.getAllByName(seed);
                for (InetAddress address : addresses) {
                    if (address.getHostAddress().equals(ip)) {
                        return true;
                    }
                }
            }
        } catch (UnknownHostException e) {
            log.warn("Failed to resolve DNS seed", e);
        }
        return false;
    }

    /**
     * Get a copy of the current node information map for testing purposes
     * @return A copy of the public key to node info mapping
     */
    protected Map<String, NodeInfo> getNodeInfoMap() {
        return new ConcurrentHashMap<>(publicKeyToNode);
    }

    /**
     * Get the last update time for a node
     * @param address The node's address
     * @return The last update time in milliseconds, or null if not found
     */
    protected Long getLastUpdateTime(String address) {
        return lastUpdateTime.get(address);
    }
}
