package io.xdag.core.v2.state;

import io.xdag.core.XAmount;
import io.xdag.utils.SimpleDecoder;
import io.xdag.utils.SimpleEncoder;
import lombok.Getter;
import org.apache.commons.codec.binary.Hex;

@Getter
public class Account {

    private final byte[] address;
    private XAmount available;
    private XAmount locked;
    private long nonce;

    public Account(byte[] address, XAmount available, XAmount locked, long nonce) {
        this.address = address;
        this.available = available;
        this.locked = locked;
        this.nonce = nonce;
    }

    public byte[] toBytes() {
        SimpleEncoder enc = new SimpleEncoder();
        enc.writeXAmount(available);
        enc.writeXAmount(locked);
        enc.writeLong(nonce);

        return enc.toBytes();
    }

    public static Account fromBytes(byte[] address, byte[] bytes) {
        SimpleDecoder dec = new SimpleDecoder(bytes);
        XAmount available = dec.readXAmount();
        XAmount locked = dec.readXAmount();
        long nonce = dec.readLong();

        return new Account(address, available, locked, nonce);
    }

    void setAvailable(XAmount available) {
        this.available = available;
    }

    void setLocked(XAmount locked) {
        this.locked = locked;
    }

    void setNonce(long nonce) {
        this.nonce = nonce;
    }

    @Override
    public String toString() {
        return "Account [address=" + Hex.encodeHexString(address) + ", available=" + available + ", locked=" + locked
                + ", nonce=" + nonce + "]";
    }

}
