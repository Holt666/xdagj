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
import io.xdag.Network;
import io.xdag.config.Config;
import io.xdag.config.spec.NodeSpec;
import io.xdag.crypto.Keys;
import io.xdag.net.message.p2p.NodesMessage;
import org.hyperledger.besu.crypto.KeyPair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class NodeManagerTest {

    @Mock
    private Kernel kernel;
    @Mock
    private Config config;
    @Mock
    private NodeSpec nodeSpec;

    private NodeManager nodeManager;
    private KeyPair keyPair1;
    private KeyPair keyPair2;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Setup kernel and config mocks
        when(kernel.getConfig()).thenReturn(config);
        when(config.getNodeSpec()).thenReturn(nodeSpec);
        when(config.getNetwork()).thenReturn(Network.MAINNET);

        // Create test key pairs
        keyPair1 = Keys.createEcKeyPair();
        keyPair2 = Keys.createEcKeyPair();

        // Initialize NodeManager
        nodeManager = spy(new NodeManager(kernel));

        // Mock DNS resolution
        InetAddress mockAddress = mock(InetAddress.class);
        when(mockAddress.getHostAddress()).thenReturn("1.2.3.4");
        doReturn(new InetAddress[]{mockAddress})
                .when(nodeManager)
                .resolveHost(anyString());

        // Start NodeManager
        nodeManager.start();
    }

    @Test
    public void testIsSeedNode() {
        // Setup DNS seed nodes
        when(config.getNetwork()).thenReturn(Network.MAINNET);
        when(nodeSpec.getSeedNodesDns(Network.MAINNET))
                .thenReturn(Arrays.asList("seed1.xdag.org"));

        // Test with DNS seed node IP
        assertTrue(nodeManager.isSeedNode("1.2.3.4"));

        // Test with non-seed node IP
        assertFalse(nodeManager.isSeedNode("5.6.7.8"));

        // Test with null IP
        assertFalse(nodeManager.isSeedNode((String)null));

        // Test with Node object
        Node seedNode = new Node("1.2.3.4", 8001);
        assertTrue(nodeManager.isSeedNode(seedNode));

        Node nonSeedNode = new Node("5.6.7.8", 8001);
        assertFalse(nodeManager.isSeedNode(nonSeedNode));
    }

    @Test
    public void testProcessNodes() throws UnknownHostException {
        // Setup DNS seed nodes
        when(config.getNetwork()).thenReturn(Network.MAINNET);
        when(nodeSpec.getSeedNodesDns(Network.MAINNET))
                .thenReturn(Arrays.asList("seed1.xdag.org"));

        // Create test nodes
        List<NodeInfo> nodes = Arrays.asList(
                NodeInfo.create("192.168.1.1", keyPair1),
                NodeInfo.create("192.168.1.2", keyPair2)
        );

        // Create test authorized addresses
        Set<String> authorizedAddresses = new HashSet<>(Arrays.asList(
                "addr1", "addr2"
        ));

        // Test processing nodes from seed node
        NodesMessage msgFromSeed = new NodesMessage(nodes, authorizedAddresses);
        nodeManager.processNodes(msgFromSeed, "1.2.3.4");

        // Verify authorized addresses are updated
        assertEquals(authorizedAddresses, nodeManager.getAuthorizedAddresses());

        // Test processing nodes from non-seed node
        NodesMessage msgFromNonSeed = new NodesMessage(nodes, authorizedAddresses);
        nodeManager.processNodes(msgFromNonSeed, "5.6.7.8");

        // Verify authorized addresses are not updated from non-seed node
        assertEquals(authorizedAddresses, nodeManager.getAuthorizedAddresses());
    }

    @Test
    public void testDNSResolutionFailure() throws UnknownHostException {
        // Setup DNS resolution to fail
        doThrow(new UnknownHostException("Test DNS failure"))
                .when(nodeManager)
                .resolveHost(anyString());

        // Setup DNS seed nodes
        when(config.getNetwork()).thenReturn(Network.MAINNET);
        when(nodeSpec.getSeedNodesDns(Network.MAINNET))
                .thenReturn(Arrays.asList("seed1.xdag.org"));

        // Test with DNS resolution failure
        assertFalse(nodeManager.isSeedNode("1.2.3.4"));
    }

    @Test
    public void testNodeConnectionManagement() {
        // Create test nodes
        Node node1 = new Node("192.168.1.1", 8001);
        Node node2 = new Node("192.168.1.2", 8001);

        // Test adding nodes
        nodeManager.addNode(node1);
        assertEquals(1, nodeManager.queueSize());

        nodeManager.addNode(node2);
        assertEquals(2, nodeManager.queueSize());

        // Test adding duplicate node
        nodeManager.addNode(node1);
        assertEquals(2, nodeManager.queueSize());

        // Test queue size limit
        for (int i = 0; i < 1024; i++) {
            nodeManager.addNode(new Node("10.0.0." + i, 8001));
        }
        assertTrue(nodeManager.queueSize() <= 1024);
    }

    @Test
    public void testNodeInfoProcessing() {
        // Create valid node info with correct port number (8001)
        NodeInfo nodeInfo = NodeInfo.create("192.168.1.1", keyPair1);
        String address = nodeInfo.getAddress();

        // Setup authorized address
        nodeManager.getAuthorizedAddresses().add(address);

        assertTrue("Valid node info should be processed successfully", nodeManager.processNodeInfo(nodeInfo));

        // Verify node info was stored correctly
        Map<String, NodeInfo> nodeMap = nodeManager.getNodeInfoMap();
        assertEquals("Node map should contain one entry", 1, nodeMap.size());
        assertEquals("Stored node info should match original", nodeInfo, nodeMap.get(nodeInfo.getAddress()));

        // Test processing null node info
        assertFalse("Null node info should be rejected", nodeManager.processNodeInfo(null));

        // Test processing node info with wrong port
        NodeInfo wrongPortInfo = mock(NodeInfo.class);
        when(wrongPortInfo.verify()).thenReturn(true);
        when(wrongPortInfo.isExpired()).thenReturn(false);
        when(wrongPortInfo.getPort()).thenReturn(9999);
        when(wrongPortInfo.getIpString()).thenReturn("192.168.1.1");
        when(wrongPortInfo.getAddress()).thenReturn(address);
        assertFalse("Node info with wrong port should be rejected", nodeManager.processNodeInfo(wrongPortInfo));

        // Test processing expired node info
        NodeInfo expiredInfo = mock(NodeInfo.class);
        when(expiredInfo.verify()).thenReturn(true);
        when(expiredInfo.isExpired()).thenReturn(true);
        when(expiredInfo.getIpString()).thenReturn("192.168.1.1");
        when(expiredInfo.getPort()).thenReturn(8001);
        when(expiredInfo.getAddress()).thenReturn(address);
        assertFalse("Expired node info should be rejected", nodeManager.processNodeInfo(expiredInfo));

        // Test processing node info with verification failure
        NodeInfo invalidInfo = mock(NodeInfo.class);
        when(invalidInfo.verify()).thenReturn(false);
        when(invalidInfo.getIpString()).thenReturn("192.168.1.1");
        assertFalse("Node info with failed verification should be rejected", nodeManager.processNodeInfo(invalidInfo));
    }
} 