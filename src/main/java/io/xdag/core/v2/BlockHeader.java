package io.xdag.core.v2;

import io.xdag.crypto.Keys;
import io.xdag.utils.BytesUtils;
import io.xdag.utils.SimpleDecoder;
import io.xdag.utils.SimpleEncoder;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.binary.Hex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.math.BigInteger;

import io.xdag.crypto.Hash;

import static io.xdag.crypto.Hash.HASH_LEN;

@Getter
@Setter
public class BlockHeader {
    private final byte[] hash;

    private final long number;

    private final byte[] coinbase;

    private final byte[] parentHash;

    private final long timestamp;

    private final byte[] transactionsRoot;

    private final byte[] resultsRoot;

    private final byte[] stateRoot;

    private final byte[] data;

    private final byte[] nbits;
    private final byte[] nonce;
    private final List<byte[]> witnessBlockHashes;

    private final byte[] encoded;

    public BlockHeader(long number, byte[] coinbase, byte[] prevHash, long timestamp, byte[] transactionsRoot,
                       byte[] resultsRoot, byte[] stateRoot, byte[] data, byte[] nbits, byte[] nonce, List<byte[]> witnessBlockHashes) {
        this.number = number;
        this.coinbase = coinbase;
        this.parentHash = prevHash;
        this.timestamp = timestamp;
        this.transactionsRoot = transactionsRoot;
        this.resultsRoot = resultsRoot;
        this.stateRoot = stateRoot;
        this.data = data;

        this.nbits = nbits;
        this.nonce = nonce;
        this.witnessBlockHashes = witnessBlockHashes;

        SimpleEncoder enc = new SimpleEncoder();
        enc.writeLong(number);
        enc.writeBytes(coinbase);
        enc.writeBytes(prevHash);
        enc.writeLong(timestamp);
        enc.writeBytes(transactionsRoot);
        enc.writeBytes(resultsRoot);
        enc.writeBytes(stateRoot);
        enc.writeBytes(data);

        enc.writeBytes(nbits);
        enc.writeBytes(nonce);
        enc.writeInt(witnessBlockHashes.size());
        for(byte[] hash : witnessBlockHashes) {
            enc.write(hash);
        }

        this.encoded = enc.toBytes();
        this.hash = Hash.h256(encoded);
    }

    /**
     * Parses block header from byte arrays.
     *
     * @param hash
     * @param encoded
     */
    public BlockHeader(byte[] hash, byte[] encoded) {
        this.hash = hash;

        SimpleDecoder dec = new SimpleDecoder(encoded);
        this.number = dec.readLong();
        this.coinbase = dec.readBytes();
        this.parentHash = dec.readBytes();
        this.timestamp = dec.readLong();
        this.transactionsRoot = dec.readBytes();
        this.resultsRoot = dec.readBytes();
        this.stateRoot = dec.readBytes();
        this.data = dec.readBytes();

        this.nbits = dec.readBytes();
        this.nonce = dec.readBytes();
        int witnessBlockHashCount = dec.readInt();
        this.witnessBlockHashes = new ArrayList<>(witnessBlockHashCount);
        for(int i = 0; i < witnessBlockHashCount; i++) {
            byte[] witnessBlockHash = dec.readBytes();
            this.witnessBlockHashes.add(witnessBlockHash);
        }

        this.encoded = encoded;
    }

    /**
     * Validates block header format.
     *
     * @return true if success, otherwise false
     */
    public boolean validate() {
        return hash != null && hash.length == HASH_LEN
                && number >= 0
                && coinbase != null && coinbase.length == Keys.ADDRESS_LEN
                && parentHash != null && parentHash.length == HASH_LEN
                && timestamp >= 0
                && transactionsRoot != null && transactionsRoot.length == HASH_LEN
                && resultsRoot != null && resultsRoot.length == HASH_LEN
                && stateRoot != null && Arrays.equals(BytesUtils.EMPTY_HASH, stateRoot) // RESERVED FOR VM
                && data != null && data.length <= BlockHeaderData.MAX_SIZE
                && nbits != null && nbits.length == HASH_LEN
                && nonce != null
                && encoded != null
                && Arrays.equals(Hash.h256(encoded), hash);
    }

    public BlockHeaderData getDecodedData() {
        return new BlockHeaderData(data);
    }

    public byte[] toBytes() {
        SimpleEncoder enc = new SimpleEncoder();
        enc.writeBytes(hash);
        enc.writeBytes(encoded);
        return enc.toBytes();
    }

    public static BlockHeader fromBytes(byte[] bytes) {
        SimpleDecoder dec = new SimpleDecoder(bytes);
        byte[] hash = dec.readBytes();
        byte[] encoded = dec.readBytes();

        return new BlockHeader(hash, encoded);
    }

    /**
     * Check if block meets difficulty target
     * Similar to Bitcoin's difficulty check:
     * - Convert nbits to target hash
     * - Block hash must be less than target
     */
    public boolean meetsDifficulty() {
        // Convert nbits to target threshold
        // nbits format: first byte is number of bytes, next 3 bytes are significant digits
        if (nbits.length != 4) {
            return false;
        }

        int nSize = nbits[0] & 0xFF;
        long nWord = ((nbits[1] & 0xFFL) << 16) |
                ((nbits[2] & 0xFFL) << 8) |
                (nbits[3] & 0xFFL);

        // Convert to target
        BigInteger target;
        if (nSize <= 3) {
            target = BigInteger.valueOf(nWord).shiftRight(8 * (3 - nSize));
        } else {
            target = BigInteger.valueOf(nWord).shiftLeft(8 * (nSize - 3));
        }

        // Convert block hash to BigInteger (interpret as unsigned big-endian)
        BigInteger hashNum = new BigInteger(1, hash);

        // Check if hash is less than target
        return hashNum.compareTo(target) <= 0;
    }

    @Override
    public String toString() {
        StringBuilder witnessHashsStr = new StringBuilder();
        for (byte[] witnessBlockHash : witnessBlockHashes) {
            witnessHashsStr.append(Hex.encodeHexString(witnessBlockHash)).append(", ");
        }
        return "XdagHeader {" +
                "  number=" + number + "," +
                "  hash=" + Hex.encodeHexString(hash) + "," +
                "  coinbase=" + Hex.encodeHexString(coinbase) + "," +
                "  parentHash=" + Hex.encodeHexString(parentHash) + ", " +
                "  witnessBlockHash=[" + witnessHashsStr + "]" +
                "  timestamp=" + timestamp + ", " +
                "  transactionsRoot=" + Hex.encodeHexString(transactionsRoot) + ", " +
                "  resultsRoot=" + Hex.encodeHexString(resultsRoot) +
                "}";
    }

}
