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

    // Thread naming pattern
    private static final String THREAD_NAME_PATTERN = "NodeManager-thread-%d";

    private static final ThreadFactory factory = new BasicThreadFactory.Builder()
            .namingPattern(THREAD_NAME_PATTERN)
            .daemon(true)
            .build();

    // Node queue and cache related constants
    private static final long MAX_QUEUE_SIZE = 1024;
    private static final int LRU_CACHE_SIZE = 1024;
    
    // Time related constants (in milliseconds)
    private static final long RECONNECT_WAIT_MS = 60_000L;  // 60 seconds
    private static final long CLEANUP_INTERVAL_MS = 60_000L; // 60 seconds
    private static final long CONNECT_INITIAL_DELAY_MS = 3_000L;
    private static final long CONNECT_INTERVAL_MS = 1_000L;
    private static final long FETCH_INITIAL_DELAY_SEC = 10L;
    private static final long FETCH_INTERVAL_SEC = 100L;
    private static final long CLEANUP_INITIAL_DELAY_SEC = 15L;
    private static final long INIT_ADDRESSES_DELAY_SEC = 5L;

    // Network related constants
    private static final int SEED_NODE_PORT = 8001;

    // Queue for nodes pending connection
    private final Deque<Node> connectionQueue = new ConcurrentLinkedDeque<>();

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

    // Map to track address-channel mappings
    private final Map<String, SocketChannel> addressChannels = new ConcurrentHashMap<>();

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
            connectFuture = exec.scheduleAtFixedRate(
                    this::doConnect,
                    CONNECT_INITIAL_DELAY_MS,
                    CONNECT_INTERVAL_MS,
                    TimeUnit.MILLISECONDS);
            
            // Delay fetch task - this will handle fetching nodes periodically
            fetchFuture = exec.scheduleAtFixedRate(
                    this::doFetch,
                    FETCH_INITIAL_DELAY_SEC,
                    FETCH_INTERVAL_SEC,
                    TimeUnit.SECONDS);
            
            // Delay cleanup task
            exec.scheduleAtFixedRate(
                    this::cleanupExpiredNodes,
                    CLEANUP_INITIAL_DELAY_SEC,
                    CLEANUP_INTERVAL_MS / 1000, // Convert to seconds
                    TimeUnit.SECONDS);

            // 2. Schedule initial authorized addresses fetch
            exec.schedule(
                    this::initializeAuthorizedAddresses,
                    INIT_ADDRESSES_DELAY_SEC,
                    TimeUnit.SECONDS);

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

    /**
     * Get seed nodes from both local configuration and DNS records.
     * This method combines local seed nodes with DNS-resolved seed nodes.
     */
    public Set<Node> getSeedNodes(Network network) {
        Set<Node> nodes = new HashSet<>();

        // 1. Add seed nodes from local configuration
        List<Node> localSeedNodes = config.getNodeSpec().getSeedNodesFromLocal(network);
        nodes.addAll(localSeedNodes);

        // 2. Add seed nodes from DNS records
        List<String> seedDnsNames = config.getNodeSpec().getSeedNodesDns(network);
        for (String dnsName : seedDnsNames) {
            if (dnsName == null) {
                continue;
            }
            
            try {
                // Resolve DNS name to IP addresses
                InetAddress[] addresses = resolveHost(dnsName.trim());
                for (InetAddress address : addresses) {
                    // Create node with default seed port
                    Node node = new Node(address.getHostAddress(), SEED_NODE_PORT);
                    nodes.add(node);
                    log.debug("Added DNS seed node: {}", node);
                }
            } catch (UnknownHostException e) {
                log.warn("Failed to resolve seed node from DNS: {}", dnsName);
            }
        }

        return nodes;
    }

    /**
     * Add a node to the connection queue.
     * If the queue is full, the oldest node will be removed.
     */
    public void addNode(Node node) {
        if (connectionQueue.contains(node)) {
            return;
        }
        connectionQueue.addFirst(node);
        while (connectionQueue.size() > MAX_QUEUE_SIZE) {
            connectionQueue.removeLast();
        }
    }

    /**
     * Get the current size of the connection queue
     */
    public int queueSize() {
        return connectionQueue.size();
    }

    /**
     * Try to connect to a node if it meets all requirements
     * @param node The node to connect to
     * @param activeAddresses Current active connections
     * @return true if connection was attempted, false otherwise
     */
    private boolean tryConnect(Node node, Set<InetSocketAddress> activeAddresses) {
        // 1. Self-connection check
        if (isSelfNode(node)) {
            return false;
        }

        // 2. Already connected check
        if (isAlreadyConnected(node, activeAddresses)) {
            return false;
        }

        // 3. Connection rate limit check
        if (isConnectionRateLimited(node)) {
            return false;
        }

        // 4. Authorization check
        if (!isNodeAuthorized(node)) {
            return false;
        }

        // All checks passed, attempt connection
        return initiateConnection(node);
    }

    /**
     * Check if the node represents this node (self-connection)
     */
    private boolean isSelfNode(Node node) {
        return client.getNode().equals(node) || node.equals(client.getNode());
    }

    /**
     * Check if we already have an active connection to this node
     */
    private boolean isAlreadyConnected(Node node, Set<InetSocketAddress> activeAddresses) {
        return activeAddresses.contains(node.getAddress());
    }

    /**
     * Check if we need to wait before attempting to connect again
     */
    private boolean isConnectionRateLimited(Node node) {
        Long lastConnectionTime = lastConnect.getIfPresent(node);
        return lastConnectionTime != null && 
               System.currentTimeMillis() - lastConnectionTime < RECONNECT_WAIT_MS;
    }

    /**
     * Check if the node is authorized (either seed node or has authorized address)
     */
    private boolean isNodeAuthorized(Node node) {
        return isSeedNode(node) || isAddressAuthorized(node.getIp());
    }

    /**
     * Initiate connection to the node
     * @return true if connection was initiated successfully
     */
    private boolean initiateConnection(Node node) {
        try {
            XdagChannelInitializer initializer = new XdagChannelInitializer(kernel, false, node);
            client.connect(node, initializer);
            lastConnect.put(node, System.currentTimeMillis());
            return true;
        } catch (Exception e) {
            log.warn("Failed to initiate connection to {}: {}", node, e.getMessage());
            return false;
        }
    }

    /**
     * Attempt to connect to nodes in the connection queue
     */
    public void doConnect() {
        Set<InetSocketAddress> activeAddress = channelMgr.getActiveAddresses();
        Node node;
        
        // Try to connect to nodes until we reach max connections or queue is empty
        while ((node = connectionQueue.pollFirst()) != null 
                && channelMgr.size() < config.getNodeSpec().getMaxConnections()) {
            if (tryConnect(node, activeAddress)) {
                break;
            }
        }
    }

    /**
     * Connect to a specific node by IP and port
     */
    public void doConnect(String ip, int port) {
        Set<InetSocketAddress> activeAddresses = channelMgr.getActiveAddresses();
        Node node = new Node(ip, port);
        tryConnect(node, activeAddresses);
    }

    /**
     * Check if a node is a seed node by checking against both DNS and local configuration
     * @param node The node to check, can be Node object or IP string
     * @return true if the node matches a seed node
     */
    public boolean isSeedNode(Object node) {
        if (node == null) {
            return false;
        }

        String ipToCheck;
        if (node instanceof Node) {
            ipToCheck = ((Node) node).getIp();
        } else if (node instanceof String) {
            ipToCheck = (String) node;
        } else {
            return false;
        }

        // First check local seed nodes
        List<Node> localSeedNodes = config.getNodeSpec().getSeedNodesFromLocal(config.getNetwork());
        for (Node seedNode : localSeedNodes) {
            if (seedNode.getIp().equals(ipToCheck)) {
                return true;
            }
        }

        // Then check DNS seed nodes
        List<String> seedDnsNames = config.getNodeSpec().getSeedNodesDns(config.getNetwork());
        try {
            for (String seed : seedDnsNames) {
                InetAddress[] addresses = resolveHost(seed);
                for (InetAddress address : addresses) {
                    if (address.getHostAddress().equals(ipToCheck)) {
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
     * Process node info from handshake
     * @param nodeInfo The node info from handshake
     * @return true if the node info is valid and accepted, false otherwise
     */
    public boolean processNodeInfo(NodeInfo nodeInfo) {
        if (nodeInfo == null) {
            log.warn("Received null node info");
            return false;
        }

        try {
            // 1. Basic validation
            if (!nodeInfo.verify()) {
                log.warn("Node info verification failed for {}", nodeInfo.getIpString());
                return false;
            }
            
            if (nodeInfo.isExpired()) {
                log.warn("Node info expired for {}", nodeInfo.getIpString());
                return false;
            }

            // Verify port number
            if (nodeInfo.getPort() != SEED_NODE_PORT) {
                log.warn("Invalid port number {} for node {}, expected {}", 
                        nodeInfo.getPort(), nodeInfo.getIpString(), SEED_NODE_PORT);
                return false;
            }

            String address = nodeInfo.getAddress();
            String newIp = nodeInfo.getIpString();

            // 2. Authorization check
            if (!isAddressAuthorized(address)) {
                log.warn("Node {} with IP {} not in authorized addresses list", address, newIp);
                return false;
            }

            // 3. Check if IP has changed
            NodeInfo existingNodeInfo = publicKeyToNode.get(address);
            String existingIp = existingNodeInfo != null ? existingNodeInfo.getIpString() : null;
            
            // 4. Update node info
            publicKeyToNode.put(address, nodeInfo);
            
            // 5. Update connection queue if IP changed
            if (existingIp == null || !existingIp.equals(newIp)) {
                Node newNode = new Node(newIp, SEED_NODE_PORT);
                if (existingIp != null) {
                    // Remove old node if exists
                    connectionQueue.remove(new Node(existingIp, SEED_NODE_PORT));
                }
                addNode(newNode);
                
                log.info("Node {} {} - IP: {}:{}",
                        address,
                        existingIp == null ? "registered" : "updated IP from " + existingIp,
                        newIp,
                        SEED_NODE_PORT);
            } else {
                log.debug("Updated node info for {} at {}:{}", 
                        address, newIp, SEED_NODE_PORT);
            }

            return true;
        } catch (Exception e) {
            log.error("Failed to process node info for {}: {}", 
                    nodeInfo.getIpString(), e.getMessage());
            return false;
        }
    }

    /**
     * Clean up expired nodes
     */
    protected void cleanupExpiredNodes() {
        Set<String> expiredAddresses = new HashSet<>();

        // 1. Find expired nodes
        for (Map.Entry<String, NodeInfo> entry : publicKeyToNode.entrySet()) {
            String address = entry.getKey();
            NodeInfo nodeInfo = entry.getValue();

            // Check if node has expired
            if (nodeInfo.isExpired()) {
                expiredAddresses.add(address);
                log.debug("Node expired: {}", address);
            }
        }

        // 2. Remove expired nodes
        for (String address : expiredAddresses) {
            NodeInfo nodeInfo = publicKeyToNode.remove(address);
            if (nodeInfo != null) {
                Node node = new Node(nodeInfo.getIpString(), nodeInfo.getPort());
                connectionQueue.remove(node);
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
        if (nodeInfo == null) {
            return false;
        }
        return processNodeInfo(nodeInfo);
    }

    /**
     * Check if an address is authorized
     */
    public boolean isAddressAuthorized(String address) {
        return isValidBase58Address(address) && authorizedAddresses.contains(address);
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
            NodeInfo nodeInfo = publicKeyToNode.get(addressBase58);

            // If address not registered yet, allow registration
            if (nodeInfo == null) {
                return true;
            }

            // Check if IP matches existing registration
            return ip.equals(nodeInfo.getIpString());
        } catch (Exception e) {
            log.warn("Invalid address format", e);
            return false;
        }
    }

    /**
     * Record channel for an authorized address
     */
    public void recordAddressChannel(String address, SocketChannel channel) {
        if (channel != null && isValidBase58Address(address)) {
            addressChannels.put(address, channel);
            log.debug("Recorded channel for address: {}", address);
        }
    }

    /**
     * Remove authorization for an address and disconnect its channel
     */
    public void removeAuthorization(String address) {
        if (!isValidBase58Address(address)) {
            return;
        }

        // Remove from authorized addresses
        synchronized (authorizedAddresses) {
            authorizedAddresses.remove(address);
        }

        // Get and remove channel
        SocketChannel channel = addressChannels.remove(address);
        if (channel != null) {
            try {
                // Disconnect the channel
                channel.close();
                log.info("Disconnected peer {} due to authorization removed", address);
            } catch (Exception e) {
                log.warn("Failed to close channel for {}", address, e);
            }
        }

        // Clean up node info
        NodeInfo nodeInfo = publicKeyToNode.remove(address);
        if (nodeInfo != null) {
            Node node = new Node(nodeInfo.getIpString(), nodeInfo.getPort());
            connectionQueue.remove(node);
        }
    }

    /**
     * Process nodes message for node discovery
     * Only seed nodes can update the authorized addresses list
     *
     * @param msg The nodes message containing node list and optionally authorized addresses
     * @param senderIp The IP address of the sender
     */
    public void processNodes(NodesMessage msg, String senderIp) {
        // 1. Process node list
        processNodeList(msg.getNodes(), senderIp);

        // 2. Process authorized addresses (only from seed nodes)
        if (isSeedNode(senderIp)) {
            processAuthorizedAddresses(msg.getAuthorizedAddresses());
        }
    }

    /**
     * Process list of nodes for discovery
     */
    private void processNodeList(List<NodeInfo> nodes, String senderIp) {
        if (nodes == null || nodes.isEmpty()) {
            return;
        }

        boolean isSeedNode = isSeedNode(senderIp);
        for (NodeInfo nodeInfo : nodes) {
            // Only add nodes from seed nodes or already authorized nodes
            if (isSeedNode || authorizedAddresses.contains(nodeInfo.getAddress())) {
                Node node = new Node(nodeInfo.getIpString(), nodeInfo.getPort());
                if (!connectionQueue.contains(node)) {
                    addNode(node);
                }
            }
        }
    }

    /**
     * Process authorized addresses from seed nodes
     */
    private void processAuthorizedAddresses(Set<String> newAddresses) {
        if (newAddresses == null || newAddresses.isEmpty()) {
            return;
        }

        // Create a copy of current authorized addresses to avoid long lock
        Set<String> currentAddresses;
        synchronized (authorizedAddresses) {
            currentAddresses = new HashSet<>(authorizedAddresses);
        }

        // Skip if no changes - check size and content
        if (currentAddresses.size() == newAddresses.size() 
                && currentAddresses.containsAll(newAddresses)) {
            return;
        }

        // Find addresses to remove (outside of sync block)
        Set<String> removedAddresses = new HashSet<>(currentAddresses);
        removedAddresses.removeAll(newAddresses);

        // Remove unauthorized addresses first (each removal is independent)
        for (String address : removedAddresses) {
            removeAuthorization(address);
        }

        // Update authorized addresses with minimal lock time
        synchronized (authorizedAddresses) {
            authorizedAddresses.clear();
            authorizedAddresses.addAll(newAddresses);
        }

        log.info("Updated authorized addresses list, total count: {}", newAddresses.size());
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
        NodeInfo nodeInfo = publicKeyToNode.get(address);
        return nodeInfo != null ? nodeInfo.getTimestamp() : null;
    }

    /**
     * Resolve hostname to IP addresses. Protected for testing.
     */
    protected InetAddress[] resolveHost(String hostname) throws UnknownHostException {
        return InetAddress.getAllByName(hostname);
    }

    /**
     * Add multiple nodes to the connection queue
     */
    public void addNodes(Collection<Node> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return;
        }
        for (Node node : nodes) {
            addNode(node);
        }
    }

    /**
     * Validate if the address is in correct base58 format
     * @param address The address to validate
     * @return true if the address is valid base58 format, false otherwise
     */
    private boolean isValidBase58Address(String address) {
        if (address == null || address.isEmpty()) {
            return false;
        }
        try {
            return WalletUtils.checkAddress(address);
        } catch (Exception e) {
            log.warn("Invalid address format: {}", address);
            return false;
        }
    }
}
