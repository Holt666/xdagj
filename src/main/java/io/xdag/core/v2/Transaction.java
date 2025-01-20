package io.xdag.core.v2;

import io.xdag.Network;
import io.xdag.core.XAmount;
import io.xdag.crypto.Hash;
import io.xdag.crypto.Keys;
import io.xdag.crypto.Sign;
import io.xdag.utils.SimpleDecoder;
import io.xdag.utils.SimpleEncoder;
import lombok.Getter;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPSignature;

import java.util.Arrays;

import static io.xdag.utils.BytesUtils.EMPTY_ADDRESS;

@Getter
public class Transaction {

    private final byte networkId;
    private final TransactionType type;
    private final byte[] to;
    private final XAmount value;
    private final XAmount fee;
    private final long nonce;
    private final long timestamp;
    private final byte[] data;
    private final byte[] encoded;
    private final byte[] hash;
    private SECPSignature signature;


    public Transaction(Network network, TransactionType type, byte[] to, XAmount value, XAmount fee, long nonce, long timestamp, byte[] data) {
        this.networkId = network.id();
        this.type = type;
        this.to = to;
        this.value = value;
        this.fee = fee;
        this.nonce = nonce;
        this.timestamp = timestamp;
        this.data = data;

        SimpleEncoder enc = new SimpleEncoder();
        enc.writeByte(networkId);
        enc.writeByte(type.toByte());
        enc.writeBytes(to);
        enc.writeXAmount(value);
        enc.writeXAmount(fee);
        enc.writeLong(nonce);
        enc.writeLong(timestamp);
        enc.writeBytes(data);

        this.encoded = enc.toBytes();
        this.hash = Hash.h256(encoded);
    }

    private Transaction(byte[] hash, byte[] encoded, byte[] signature) {
        this.hash = hash;

        Transaction decodedTx = fromEncoded(encoded);
        this.networkId = decodedTx.networkId;
        this.type = decodedTx.type;
        this.to = decodedTx.to;
        this.value = decodedTx.value;
        this.fee = decodedTx.fee;
        this.nonce = decodedTx.nonce;
        this.timestamp = decodedTx.timestamp;
        this.data = decodedTx.data;

        this.encoded = encoded;
        this.signature = SECPSignature.decode(Bytes.wrap(signature), Sign.CURVE.getN());
    }

    public Transaction sign(KeyPair key) {
        this.signature = Sign.SECP256K1.sign(Bytes32.wrap(this.hash), key);
        return this;
    }

    public boolean validate(Network network, boolean verifySignature) {
        return hash != null && hash.length == Hash.HASH_LEN
                && networkId == network.id()
                && type != null
                && to != null && to.length == Keys.ADDRESS_LEN
                && Arrays.equals(to, EMPTY_ADDRESS)
                && value.isNotNegative()
                && fee.isNotNegative()
                && nonce >= 0
                && timestamp > 0
                && data != null
                && encoded != null
                && signature != null && !Arrays.equals(Keys.toBytesAddress(hash, signature), EMPTY_ADDRESS)
                && Arrays.equals(Hash.h256(encoded), hash)
                && (!verifySignature || Keys.verify(hash, signature));

                // The coinbase key is publicly available. People can use it for transactions.
                // It won't introduce any fundamental loss to the system but could potentially
                // cause confusion for block explorer, and thus are prohibited.
//                && (type == TransactionType.COINBASE
//                || (!Arrays.equals(Keys.toBytesAddress(hash, signature), Constants.COINBASE_ADDRESS)
//                && !Arrays.equals(to, Constants.COINBASE_ADDRESS)));
    }

    public boolean validate(Network network) {
        return validate(network, true);
    }

    public byte[] getFrom() {
        return (signature == null) ? null : Keys.toBytesAddress(hash, signature);
    }

    public static Transaction fromEncoded(byte[] encoded) {
        SimpleDecoder decoder = new SimpleDecoder(encoded);

        byte networkId = decoder.readByte();
        byte type = decoder.readByte();
        byte[] to = decoder.readBytes();
        XAmount value = decoder.readXAmount();
        XAmount fee = decoder.readXAmount();
        long nonce = decoder.readLong();
        long timestamp = decoder.readLong();
        byte[] data = decoder.readBytes();

        TransactionType transactionType = TransactionType.of(type);

        return new Transaction(Network.of(networkId), transactionType, to, value, fee, nonce, timestamp, data);
    }

    public byte[] toBytes() {
        SimpleEncoder enc = new SimpleEncoder();
        enc.writeBytes(hash);
        enc.writeBytes(encoded);
        enc.writeBytes(signature.encodedBytes().toArray());

        return enc.toBytes();
    }

    public static Transaction fromBytes(byte[] bytes) {
        SimpleDecoder dec = new SimpleDecoder(bytes);
        byte[] hash = dec.readBytes();
        byte[] encoded = dec.readBytes();
        byte[] signature = dec.readBytes();

        return new Transaction(hash, encoded, signature);
    }

    public int size() {
        return toBytes().length;
    }

    @Override
    public String toString() {
        return "Transaction [type=" + type + ", from=" + Hex.encodeHexString(getFrom()) + ", to=" + Hex.encodeHexString(to) + ", value="
                + value + ", fee=" + fee + ", nonce=" + nonce + ", timestamp=" + timestamp + ", data="
                + Hex.encodeHexString(data) + ", hash=" + Hex.encodeHexString(hash) + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        Transaction that = (Transaction) o;

        return new EqualsBuilder()
                .append(encoded, that.encoded)
                .append(hash, that.hash)
                .append(signature, that.signature)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(encoded)
                .append(hash)
                .append(signature)
                .toHashCode();
    }

}
