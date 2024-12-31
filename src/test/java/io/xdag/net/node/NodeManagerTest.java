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

import io.xdag.Kernel;
import io.xdag.config.Config;
import io.xdag.config.spec.NodeSpec;
import io.xdag.crypto.Keys;
import io.xdag.net.ChannelManager;
import io.xdag.net.PeerClient;
import io.xdag.net.message.p2p.NodesMessage;
import org.hyperledger.besu.crypto.KeyPair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.InetSocketAddress;
import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class NodeManagerTest {

    @Mock
    private Kernel kernel;
    @Mock
    private Config config;
    @Mock
    private PeerClient client;
    @Mock
    private ChannelManager channelMgr;
    @Mock
    private NodeSpec nodeSpec;

    private NodeManager nodeManager;
    private KeyPair keyPair1;
    private KeyPair keyPair2;
    private NodeInfo node1;
    private NodeInfo node2;
    private String ip1 = "192.168.1.1";
    private String ip2 = "192.168.1.2";

    @Before
    public void setup() throws InvalidAlgorithmParameterException, NoSuchAlgorithmException, NoSuchProviderException {
        MockitoAnnotations.initMocks(this);
        
        // Setup mocks
        when(kernel.getConfig()).thenReturn(config);
        when(kernel.getClient()).thenReturn(client);
        when(kernel.getChannelMgr()).thenReturn(channelMgr);
        when(config.getNodeSpec()).thenReturn(nodeSpec);
        when(nodeSpec.getMaxConnections()).thenReturn(100);
        
        nodeManager = new NodeManager(kernel);
        keyPair1 = Keys.createEcKeyPair();
        keyPair2 = Keys.createEcKeyPair();

        // Create test nodes
        byte[] ip1Bytes = new byte[]{(byte)192, (byte)168, 1, 1};
        byte[] ip2Bytes = new byte[]{(byte)192, (byte)168, 1, 2};
        node1 = NodeInfo.create(ip1Bytes, keyPair1);
        node2 = NodeInfo.create(ip2Bytes, keyPair2);
    }

    @Test
    public void testAuthorizedAddressManagement() {
        String address1 = node1.getAddress();
        String address2 = node2.getAddress();

        // Initially empty
        assertTrue(nodeManager.getAuthorizedAddresses().isEmpty());

        // Add addresses
        nodeManager.getAuthorizedAddresses().add(address1);
        nodeManager.getAuthorizedAddresses().add(address2);

        // Verify addresses are added
        assertEquals(2, nodeManager.getAuthorizedAddresses().size());
        assertTrue(nodeManager.isAddressAuthorized(address1));
        assertTrue(nodeManager.isAddressAuthorized(address2));

        // Test null and empty address
        assertFalse(nodeManager.isAddressAuthorized(null));
        assertFalse(nodeManager.isAddressAuthorized(""));
    }

    @Test
    public void testIpBindingValidation() {
        String address1 = node1.getAddress();
        byte[] addressBytes = org.bouncycastle.util.encoders.Hex.decode(address1);

        // Add to authorized list
        nodeManager.getAuthorizedAddresses().add(address1);

        // First IP binding should succeed
        assertTrue(nodeManager.checkIpForAddress(addressBytes, ip1));

        // Same IP should succeed
        assertTrue(nodeManager.checkIpForAddress(addressBytes, ip1));

        // Different IP should fail
        assertFalse(nodeManager.checkIpForAddress(addressBytes, ip2));

        // Test null values
        assertFalse(nodeManager.checkIpForAddress(null, ip1));
        assertFalse(nodeManager.checkIpForAddress(addressBytes, null));
    }

    @Test
    public void testNodeInfoProcessing() {
        String address1 = node1.getAddress();

        // Add to authorized list
        nodeManager.getAuthorizedAddresses().add(address1);

        // Process node info should succeed
        assertTrue(nodeManager.processNodeInfo(node1));

        // Process same node info again should succeed
        assertTrue(nodeManager.processNodeInfo(node1));

        // Create node info with different IP
        byte[] differentIp = new byte[]{(byte)192, (byte)168, 1, 3};
        NodeInfo node1WithDifferentIp = NodeInfo.create(differentIp, keyPair1);

        // Process node info with different IP should fail
        assertFalse(nodeManager.processNodeInfo(node1WithDifferentIp));

        // Test null node info
        assertFalse(nodeManager.processNodeInfo(null));
    }

    @Test
    public void testUnauthorizedNodeInfo() {
        // Process unauthorized node info should fail
        assertFalse(nodeManager.processNodeInfo(node1));

        // Authorize the address
        nodeManager.getAuthorizedAddresses().add(node1.getAddress());

        // Now processing should succeed
        assertTrue(nodeManager.processNodeInfo(node1));
    }

    @Test
    public void testNodesMessageProcessing() {
        // Create nodes list
        List<NodeInfo> nodes = new ArrayList<>();
        nodes.add(node1);
        nodes.add(node2);

        // Create nodes message
        NodesMessage nodesMsg = new NodesMessage(nodes);

        // Authorize addresses
        nodeManager.getAuthorizedAddresses().add(node1.getAddress());
        nodeManager.getAuthorizedAddresses().add(node2.getAddress());

        // Process nodes message
        nodeManager.processNodes(nodesMsg, ip1);

        // Verify nodes were processed
        assertTrue(nodeManager.processNodeInfo(node1));
        assertTrue(nodeManager.processNodeInfo(node2));

        // Test empty nodes list
        NodesMessage emptyMsg = new NodesMessage(new ArrayList<>());
        nodeManager.processNodes(emptyMsg, ip1);
    }

    @Test
    public void testNodeConnection() {
        // Setup mock for active addresses
        Set<InetSocketAddress> activeAddresses = new HashSet<>();
        when(channelMgr.getActiveAddresses()).thenReturn(activeAddresses);
        when(channelMgr.size()).thenReturn(0);

        // Create a node
        Node testNode = new Node(ip1, 8001);

        // Add node to manager
        nodeManager.addNode(testNode);

        // Test connection
        nodeManager.doConnect();

        // Verify connection attempt was made
        verify(client).connect(eq(testNode), any());
    }

    @Test
    public void testNodeQueueManagement() {
        // Create test nodes
        Node node1 = new Node(ip1, 8001);
        Node node2 = new Node(ip2, 8001);

        // Initially empty
        assertEquals(0, nodeManager.queueSize());

        // Add nodes
        nodeManager.addNode(node1);
        assertEquals(1, nodeManager.queueSize());

        // Add same node again should not increase size
        nodeManager.addNode(node1);
        assertEquals(1, nodeManager.queueSize());

        // Add different node
        nodeManager.addNode(node2);
        assertEquals(2, nodeManager.queueSize());
    }

    @Test
    public void testNodeExpiration() throws Exception {
        // Add node1 to authorized list
        String address1 = node1.getAddress();
        nodeManager.getAuthorizedAddresses().add(address1);

        // Process node info
        assertTrue(nodeManager.processNodeInfo(node1));

        // Force node expiration by setting last update time to a very old value
        nodeManager.getLastUpdateTime().put(address1, System.currentTimeMillis() - (NodeInfo.MAX_TIMESTAMP_DRIFT * 2));

        // Trigger cleanup
        nodeManager.cleanupExpiredNodes();

        // Verify node was removed
        assertFalse(nodeManager.processNodeInfo(node1));
    }

    @Test
    public void testNodeUpdateInterval() throws Exception {
        // Add node1 to authorized list
        String address1 = node1.getAddress();
        nodeManager.getAuthorizedAddresses().add(address1);

        // Process initial node info
        assertTrue(nodeManager.processNodeInfo(node1));

        // Create node with different IP immediately
        byte[] differentIp = new byte[]{(byte)192, (byte)168, 1, 3};
        NodeInfo node1WithDifferentIp = NodeInfo.create(differentIp, keyPair1);

        // Update should fail due to minimum interval requirement
        assertFalse(nodeManager.processNodeInfo(node1WithDifferentIp));

        // Wait for minimum interval
        Thread.sleep(60_000); // 60 seconds

        // Now update should succeed
        assertTrue(nodeManager.processNodeInfo(node1WithDifferentIp));
    }

    @Test
    public void testSeedNodeHandling() {
        // Setup seed nodes
        List<String> seedNodes = new ArrayList<>();
        seedNodes.add(ip1);
        when(nodeSpec.getDnsSeedsMainNet()).thenReturn(seedNodes);

        // Create node with seed node IP
        byte[] seedIp = new byte[]{(byte)192, (byte)168, 1, 1};
        NodeInfo seedNodeInfo = NodeInfo.create(seedIp, keyPair1);

        // Seed node should be processed even without authorization
        assertTrue(nodeManager.processNodeInfo(seedNodeInfo));

        // Non-seed node should still require authorization
        assertFalse(nodeManager.processNodeInfo(node2));
    }

    @Test
    public void testNodeInfoValidation() throws Exception {
        // Add node1 to authorized list
        String address1 = node1.getAddress();
        nodeManager.getAuthorizedAddresses().add(address1);

        // Process initial node info
        assertTrue(nodeManager.processNodeInfo(node1));

        // Wait for node to expire
        Thread.sleep(NodeInfo.MAX_TIMESTAMP_DRIFT + 1000);

        // Create new node info with same key
        NodeInfo newNode = NodeInfo.create(node1.getIp(), keyPair1);

        // Process should fail due to timestamp drift
        assertFalse(nodeManager.processNodeInfo(newNode));

        // Create node info with invalid IP
        byte[] invalidIp = new byte[]{(byte)192, (byte)168, 1, 3};
        NodeInfo invalidNode = NodeInfo.create(invalidIp, keyPair1);

        // Process should fail due to IP mismatch
        assertFalse(nodeManager.processNodeInfo(invalidNode));
    }
} 