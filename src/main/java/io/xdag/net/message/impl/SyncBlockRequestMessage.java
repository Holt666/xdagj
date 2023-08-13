package io.xdag.net.message.impl;

import io.xdag.core.XdagStats;
import io.xdag.net.message.AbstractMessage;
import io.xdag.net.message.NetDB;
import io.xdag.net.message.XdagMessageCodes;
import io.xdag.utils.BytesUtils;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;
import org.apache.tuweni.bytes.MutableBytes32;

import java.math.BigInteger;
import java.nio.ByteOrder;

import static io.xdag.config.Constants.DNET_PKT_XDAG;
import static io.xdag.core.XdagBlock.XDAG_BLOCK_SIZE;
import static io.xdag.core.XdagField.FieldType.XDAG_FIELD_NONCE;

public class SyncBlockRequestMessage extends AbstractMessage {
    public SyncBlockRequestMessage(MutableBytes hash, XdagStats xdagStats, NetDB currentDB) {
        super(XdagMessageCodes.SYNCBLOCK_REQUEST, 0, 0, Bytes32.wrap(hash), xdagStats,currentDB);
    }

    public SyncBlockRequestMessage(MutableBytes hash) {
        super(hash);
    }

    @Override
    public Class<?> getAnswerMessage() {
        return null;
    }

    @Override
    public Bytes getEncoded() {
        // TODO Auto-generated method stub
        return encoded;
    }

    @Override
    public String toString() {
        if (!parsed) {
            parse();
        }
        return "["
                + this.getCommand().name()
                + " starttime="
                + starttime
                + " endtime="
                + this.endtime
                + " hash="
                + hash.toHexString()
                + " netstatus="
                + xdagStats;
    }

    @Override
    public XdagMessageCodes getCommand() {
        return XdagMessageCodes.SYNCBLOCK_REQUEST;
    }

    @Override
    public void encode() {
        parsed = true;
        encoded = MutableBytes.create(512);
        int ttl = 1;
        long transportheader = (ttl << 8) | DNET_PKT_XDAG | (XDAG_BLOCK_SIZE << 16);
        long type = (codes.asByte() << 4) | XDAG_FIELD_NONCE.asByte();

        BigInteger diff = xdagStats.getDifficulty();
        BigInteger maxDiff = xdagStats.getMaxdifficulty();
        long nmain = xdagStats.getNmain();
        long totalMainNumber = Math.max(xdagStats.getTotalnmain(), nmain);
        long nblocks = xdagStats.getNblocks();
        long totalBlockNumber = xdagStats.getTotalnblocks();

        MutableBytes mutableBytes = MutableBytes.create(112);
        long nhosts = currentDB.getIpList().size();
        long totalHosts = currentDB.getIpList().size();

        mutableBytes.set(0, Bytes.wrap(BytesUtils.longToBytes(nhosts, true)));
        mutableBytes.set(8, Bytes.wrap(BytesUtils.longToBytes(totalHosts, true)));
        mutableBytes.set(16, Bytes.wrap(currentDB.getEncoded()));

//        // TODO：后续根据ip替换
//        String tmp = "04000000040000003ef4780100000000" + "7f000001611e7f000001b8227f0000015f767f000001d49d";
//        // net 相关
//        byte[] tmpbyte = Hex.decode(tmp);

        // field 0 and field1
        MutableBytes32 first = MutableBytes32.create();
        first.set(0, Bytes.wrap(BytesUtils.longToBytes(transportheader, true)));
        first.set(8, Bytes.wrap(BytesUtils.longToBytes(type, true)));
        first.set(16, Bytes.wrap(BytesUtils.longToBytes(starttime, true)));
        first.set(24, Bytes.wrap(BytesUtils.longToBytes(endtime, true)));

        encoded.set(0, first);
        encoded.set(32, hash.reverse());

        // field2 diff and maxdiff
        encoded.set(64, Bytes.wrap(BytesUtils.bigIntegerToBytes(diff, 16, true)));
        encoded.set(80, Bytes.wrap(BytesUtils.bigIntegerToBytes(maxDiff, 16, true)));

        // field3 nblock totalblock main totalmain
        encoded.set(96, Bytes.wrap(BytesUtils.longToBytes(nblocks, true)));
        encoded.set(104, Bytes.wrap(BytesUtils.longToBytes(totalBlockNumber, true)));
        encoded.set(112, Bytes.wrap(BytesUtils.longToBytes(nmain, true)));
        encoded.set(120, Bytes.wrap(BytesUtils.longToBytes(totalMainNumber, true)));
        encoded.set(128, Bytes.wrap(mutableBytes));
        updateCrc();
    }

    @Override
    public void parse() {
        if (parsed) {
            return;
        }

        this.starttime = encoded.getLong(16, ByteOrder.LITTLE_ENDIAN);
        this.endtime = encoded.getLong(24, ByteOrder.LITTLE_ENDIAN);
        BigInteger maxdifficulty = encoded.slice(80, 16).toUnsignedBigInteger(ByteOrder.LITTLE_ENDIAN);
        long totalnblocks = encoded.getLong(104, ByteOrder.LITTLE_ENDIAN);
        long totalnmains = encoded.getLong(120, ByteOrder.LITTLE_ENDIAN);
        int totalnhosts = encoded.getInt(132, ByteOrder.LITTLE_ENDIAN);
        long maintime = encoded.getLong(136, ByteOrder.LITTLE_ENDIAN);
        xdagStats = new XdagStats(maxdifficulty, totalnblocks, totalnmains, totalnhosts, maintime);
        MutableBytes32 hash = MutableBytes32.create();
        hash.set(0, encoded.slice(32, 24));
        this.hash = hash.copy();
        parsed = true;
    }
}
