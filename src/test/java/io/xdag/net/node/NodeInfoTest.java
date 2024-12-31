package io.xdag.net.node;

import io.xdag.crypto.Keys;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.crypto.KeyPair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.Arrays;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class NodeInfoTest {

    private KeyPair keyPair;
    private byte[] ip;
    private NodeInfo nodeInfo;

    @Before
    public void setUp() throws Exception {
        // Generate a random key pair for testing
        keyPair = Keys.createSecp256k1KeyPair();
        
        // Create a sample IP address (127.0.0.1)
        ip = new byte[]{(byte)127, (byte)0, (byte)0, (byte)1};
        
        // Create a NodeInfo instance for testing
        nodeInfo = NodeInfo.create(ip, keyPair);
    }

    @Test
    public void testCreate() {
        assertNotNull("NodeInfo should not be null", nodeInfo);
        assertTrue("NodeInfo should be valid", nodeInfo.verify());
        assertArrayEquals("IP should match", ip, nodeInfo.getIp());
        assertEquals("Full address should match", "127.0.0.1:8001", nodeInfo.getFullAddress());
    }

    @Test(expected = RuntimeException.class)
    public void testCreateWithNullIp() {
        NodeInfo.create(null, keyPair);
    }

    @Test(expected = RuntimeException.class)
    public void testCreateWithInvalidIpLength() {
        byte[] invalidIp = new byte[]{(byte)127, (byte)0, (byte)0};
        NodeInfo.create(invalidIp, keyPair);
    }

    @Test(expected = RuntimeException.class)
    public void testCreateWithNullKeyPair() {
        NodeInfo.create(ip, null);
    }

    @Test
    public void testSerialization() {
        // Serialize
        byte[] bytes = nodeInfo.toBytes();
        assertNotNull("Serialized bytes should not be null", bytes);
        assertTrue("Serialized bytes should not be empty", bytes.length > 0);

        // Deserialize
        NodeInfo deserializedNode = NodeInfo.fromBytes(bytes);
        assertNotNull("Deserialized node should not be null", deserializedNode);
        
        // Verify equality
        assertEquals("Nodes should be equal", nodeInfo, deserializedNode);
        assertEquals("Node addresses should match", nodeInfo.getAddress(), deserializedNode.getAddress());
        assertEquals("Full addresses should match", nodeInfo.getFullAddress(), deserializedNode.getFullAddress());
        assertEquals("Timestamps should match", nodeInfo.getTimestamp(), deserializedNode.getTimestamp());
    }

    @Test(expected = RuntimeException.class)
    public void testDeserializeEmptyBytes() {
        NodeInfo.fromBytes(new byte[0]);
    }

    @Test(expected = RuntimeException.class)
    public void testDeserializeInvalidBytes() {
        byte[] invalidBytes = new byte[100];
        new Random().nextBytes(invalidBytes);
        NodeInfo.fromBytes(invalidBytes);
    }

    @Test
    public void testIpAddressFormats() {
        assertEquals("IP string should match", "127.0.0.1", nodeInfo.getIpString());
        assertEquals("Full address should match", "127.0.0.1:8001", nodeInfo.getFullAddress());
        
        InetSocketAddress socketAddress = nodeInfo.getSocketAddress();
        assertNotNull("Socket address should not be null", socketAddress);
        assertEquals("Host string should match", "127.0.0.1", socketAddress.getHostString());
        assertEquals("Port should match", 8001, socketAddress.getPort());
        assertEquals("Port getter should return default port", 8001, nodeInfo.getPort());
    }

    @Test
    public void testTimestampExpiration() throws Exception {
        // Current timestamp should not be expired
        assertFalse("New node should not be expired", nodeInfo.isExpired());

        // Wait for a very short time - should still be valid
        Thread.sleep(100);
        assertFalse("Node should not expire after short time", nodeInfo.isExpired());

        // Create a node with timestamp from the past
        long oldTimestamp = System.currentTimeMillis() - NodeInfo.MAX_TIMESTAMP_DRIFT - 1000;
        try {
            NodeInfo.fromBytes(createTestBytes(oldTimestamp, ip));
            fail("Should throw exception for expired timestamp");
        } catch (RuntimeException e) {
            // Expected exception
        }
    }

    @Test
    public void testEqualsAndHashCode() throws Exception {
        // Test equality with self
        assertEquals("Node should equal itself", nodeInfo, nodeInfo);
        assertNotEquals("Node should not equal null", null, nodeInfo);
        assertNotEquals("Node should not equal other type", nodeInfo, new Object());
        
        // Different IP should not be equal
        byte[] differentIp = new byte[]{(byte)192, (byte)168, (byte)0, (byte)1};
        NodeInfo differentNodeInfo = NodeInfo.create(differentIp, keyPair);
        assertNotEquals("Nodes with different IPs should not be equal", nodeInfo, differentNodeInfo);
        
        // Different timestamps should not be equal
        Thread.sleep(1); // Ensure different timestamp
        NodeInfo differentTimestampNodeInfo = NodeInfo.create(ip, keyPair);
        assertNotEquals("Nodes with different timestamps should not be equal", nodeInfo, differentTimestampNodeInfo);
        
        // HashCode should be consistent
        assertEquals("HashCode should be consistent", nodeInfo.hashCode(), nodeInfo.hashCode());
        assertNotEquals("HashCode should differ for different nodes", nodeInfo.hashCode(), differentNodeInfo.hashCode());
    }

    @Test
    public void testGetAddress() {
        // Get address twice to verify consistency
        String address1 = nodeInfo.getAddress();
        String address2 = nodeInfo.getAddress();
        
        assertNotNull("Address should not be null", address1);
        assertFalse("Address should not be empty", address1.isEmpty());
        assertEquals("Address should be consistent", address1, address2);
    }

    @Test
    public void testVerifyWithInvalidSignature() {
        // Create invalid signature bytes
        byte[] invalidSigBytes = new byte[64];
        new Random().nextBytes(invalidSigBytes);
        
        try {
            NodeInfo.fromBytes(Bytes.concatenate(
                    Bytes.ofUnsignedLong(System.currentTimeMillis()),
                    Bytes.wrap(ip),
                    Bytes.wrap(invalidSigBytes)
            ).toArray());
            fail("Should throw exception for invalid signature");
        } catch (RuntimeException e) {
            // Expected exception
        }
    }

    @Test
    public void testVerifyWithModifiedMessage() {
        // Get original serialized bytes
        byte[] originalBytes = nodeInfo.toBytes();
        
        // Modify the IP in the message
        byte[] modifiedIp = new byte[]{(byte)192, (byte)168, (byte)1, (byte)1};
        byte[] modifiedBytes = Bytes.concatenate(
                Bytes.wrap(Arrays.copyOfRange(originalBytes, 0, 8)), // Keep timestamp
                Bytes.wrap(modifiedIp),
                Bytes.wrap(Arrays.copyOfRange(originalBytes, 12, originalBytes.length)) // Keep signature
        ).toArray();

        try {
            NodeInfo.fromBytes(modifiedBytes);
            fail("Should throw exception for modified message");
        } catch (RuntimeException e) {
            // Expected exception
        }
    }

    @Test
    public void testToString() {
        String str = nodeInfo.toString();
        
        assertTrue("ToString should contain IP", str.contains("127.0.0.1:8001"));
        assertTrue("ToString should contain address", str.contains(nodeInfo.getAddress()));
        assertTrue("ToString should contain timestamp", str.contains(String.valueOf(nodeInfo.getTimestamp())));
    }

    @Test
    public void testTimestampFromFuture() {
        long futureTimestamp = System.currentTimeMillis() + NodeInfo.MAX_TIMESTAMP_DRIFT + 1000;
        try {
            NodeInfo.fromBytes(createTestBytes(futureTimestamp, ip));
            fail("Should throw exception for future timestamp");
        } catch (RuntimeException e) {
            // Expected exception
        }
    }

    @Test
    public void testEqualsWithDifferentSignature() throws Exception {
        // Create two NodeInfos with same IP but different timestamps (thus different signatures)
        NodeInfo node1 = NodeInfo.create(ip, keyPair);
        Thread.sleep(1);
        NodeInfo node2 = NodeInfo.create(ip, keyPair);
        
        assertNotEquals("Different signatures should not be equal", node1, node2);
        assertNotEquals("Different signatures should have different hashcodes", 
                node1.hashCode(), node2.hashCode());
    }

    @Test
    public void testGetSocketAddressWithDifferentIPs() {
        // Test various IP addresses
        byte[][] testIPs = {
            new byte[]{(byte)127, (byte)0, (byte)0, (byte)1},    // localhost
            new byte[]{(byte)192, (byte)168, (byte)0, (byte)1},  // private network
            new byte[]{(byte)10, (byte)0, (byte)0, (byte)1},     // private network
            new byte[]{(byte)172, (byte)16, (byte)0, (byte)1}    // private network
        };
        
        for (byte[] testIP : testIPs) {
            NodeInfo testNode = NodeInfo.create(testIP, keyPair);
            InetSocketAddress addr = testNode.getSocketAddress();
            
            assertEquals("Port should be default", 8001, addr.getPort());
            assertEquals("IP should match", testNode.getIpString(), addr.getHostString());
        }
    }

    // Helper method to create test bytes with specific timestamp
    private byte[] createTestBytes(long timestamp, byte[] ip) {
        return Bytes.concatenate(
                Bytes.ofUnsignedLong(timestamp),
                Bytes.wrap(ip),
                Bytes.wrap(new byte[64]) // dummy signature
        ).toArray();
    }
} 