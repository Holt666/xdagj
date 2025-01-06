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

import io.xdag.crypto.Hash;
import io.xdag.crypto.Keys;
import io.xdag.crypto.Sign;
import io.xdag.utils.BytesUtils;
import io.xdag.utils.SimpleDecoder;
import io.xdag.utils.SimpleEncoder;
import io.xdag.utils.WalletUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SECPSignature;

import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * Node information with signature
 * 
 * Structure:
 * - Timestamp (8 bytes)
 * - IP (4 bytes)
 * - Signature
 */
@Slf4j
@Getter
public class NodeInfo {

    public static final long MAX_TIMESTAMP_DRIFT = 300000; // 5 minutes in milliseconds
    private static final int DEFAULT_PORT = 8001;
    private static final int IPV4_BYTES = 4;
    private static final int MIN_SIGNATURE_LENGTH = 64;
    private static final int MAX_SIGNATURE_LENGTH = 72; // DER encoded ECDSA signature

    private final byte[] ip;
    private final SECPPublicKey publicKey;
    private final SECPSignature signature;
    private final long timestamp;

    public static NodeInfo create(String ip, KeyPair keyPair) {
        if (ip == null || ip.isEmpty()) {
            throw new RuntimeException("IP address cannot be null or empty");
        }
        
        byte[] ipBytes = BytesUtils.parseIpString(ip);
        if (ipBytes == null) {
            throw new RuntimeException("Invalid IP address format");
        }
        
        return create(ipBytes, keyPair);
    }

    /**
     * Create a new NodeInfo with signature
     * @throws RuntimeException if creation fails
     */
    public static NodeInfo create(byte[] ip, KeyPair keyPair) {
        validateIp(ip);
        if (keyPair == null) {
            throw new RuntimeException("KeyPair cannot be null");
        }

        try {
            long timestamp = System.currentTimeMillis();
            validateTimestamp(timestamp);
            
            SECPPublicKey publicKey = keyPair.getPublicKey();

            // Create message to sign
            byte[] message = encodeForSigning(timestamp, ip);
            
            // Sign the message
            Bytes32 messageHash = Hash.hashTwice(Bytes.wrap(message));
            SECPSignature signature = Sign.SECP256K1.sign(messageHash, keyPair);

            // Create and verify the node info
            NodeInfo nodeInfo = new NodeInfo(ip, publicKey, signature, timestamp);
            if (!nodeInfo.verify()) {
                throw new RuntimeException("Failed to verify created node info");
            }

            return nodeInfo;

        } catch (Exception e) {
            throw new RuntimeException("Failed to create node info", e);
        }
    }

    private static void validateIp(byte[] ip) {
        if (ip == null || ip.length != IPV4_BYTES) {
            throw new RuntimeException("IP address must be 4 bytes for IPv4");
        }
    }

    private static void validateTimestamp(long timestamp) {
        long now = System.currentTimeMillis();
        if (Math.abs(now - timestamp) > MAX_TIMESTAMP_DRIFT) {
            throw new RuntimeException("Timestamp is too far from current time");
        }
    }

    private NodeInfo(byte[] ip, SECPPublicKey publicKey, SECPSignature signature, long timestamp) {
        this.ip = ip.clone();
        this.publicKey = publicKey;
        this.signature = signature;
        this.timestamp = timestamp;
    }

    /**
     * Encode data for signing
     */
    private static byte[] encodeForSigning(long timestamp, byte[] ip) {
        SimpleEncoder enc = new SimpleEncoder();
        enc.writeLong(timestamp);
        enc.writeBytes(ip);
        return enc.toBytes();
    }

    /**
     * Encode full node info including signature
     */
    public byte[] toBytes() {
        SimpleEncoder enc = new SimpleEncoder();
        enc.writeLong(timestamp);
        enc.writeBytes(ip);
        enc.writeBytes(signature.encodedBytes().toArray());
        return enc.toBytes();
    }

    /**
     * Decode node info from bytes
     * @throws RuntimeException if decoding fails
     */
    public static NodeInfo fromBytes(byte[] bytes) {
        try {
            SimpleDecoder dec = new SimpleDecoder(bytes);

            // Read fields
            long timestamp = dec.readLong();
            validateTimestamp(timestamp);
            
            byte[] ip = dec.readBytes();
            validateIp(ip);
            
            byte[] sigBytes = dec.readBytes();
            if (sigBytes.length < MIN_SIGNATURE_LENGTH || sigBytes.length > MAX_SIGNATURE_LENGTH) {
                throw new RuntimeException("Invalid signature length");
            }
            
            SECPSignature signature = SECPSignature.decode(Bytes.wrap(sigBytes), Sign.CURVE.getN());
            
            // Recover public key from signature
            byte[] message = encodeForSigning(timestamp, ip);
            Bytes32 messageHash = Hash.hashTwice(Bytes.wrap(message));
            SECPPublicKey publicKey = Sign.SECP256K1.recoverPublicKeyFromSignature(messageHash, signature)
                    .orElseThrow(() -> new RuntimeException("Failed to recover public key from signature"));

            // Create and verify
            NodeInfo nodeInfo = new NodeInfo(ip, publicKey, signature, timestamp);
            if (!nodeInfo.verify()) {
                throw new RuntimeException("Invalid signature");
            }
            
            return nodeInfo;

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to decode node info", e);
        }
    }

    /**
     * Verify signature
     */
    public boolean verify() {
        byte[] message = encodeForSigning(timestamp, ip);
        Bytes32 hash = Hash.hashTwice(Bytes.wrap(message));
        
        try {
            return Sign.SECP256K1.verify(hash, signature, publicKey);
        } catch (Exception e) {
            log.warn("Failed to verify node info signature", e);
            return false;
        }
    }

    /**
     * Get IP address as string
     */
    public String getIpString() {
        return String.format("%d.%d.%d.%d", 
                ip[0] & 0xFF, ip[1] & 0xFF, ip[2] & 0xFF, ip[3] & 0xFF);
    }

    /**
     * Get full address (IP:PORT)
     */
    public String getFullAddress() {
        return getIpString() + ":" + DEFAULT_PORT;
    }

    /**
     * Get socket address
     */
    public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(getIpString(), DEFAULT_PORT);
    }

    /**
     * Get IP address bytes
     */
    public byte[] getIp() {
        return ip.clone();
    }

    /**
     * Get default port number
     */
    public int getPort() {
        return DEFAULT_PORT;
    }

    /**
     * Check if this node info has expired
     */
    public boolean isExpired() {
        return Math.abs(System.currentTimeMillis() - timestamp) > MAX_TIMESTAMP_DRIFT;
    }

    /**
     * Get node's address derived from public key
     */
    public String getAddress() {
        byte[] addressBytes = Keys.toBytesAddress(publicKey);
        return WalletUtils.toBase58(addressBytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        NodeInfo other = (NodeInfo) o;
        return timestamp == other.timestamp &&
                Arrays.equals(ip, other.ip) &&
                publicKey.equals(other.publicKey) &&
                signature.equals(other.signature);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(ip);
        result = 31 * result + publicKey.hashCode();
        result = 31 * result + signature.hashCode();
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return String.format("NodeInfo{ip='%s', address='%s', timestamp=%d}", 
                getFullAddress(), getAddress(), timestamp);
    }
} 