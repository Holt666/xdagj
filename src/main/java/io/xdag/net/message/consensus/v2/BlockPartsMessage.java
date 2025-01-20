package io.xdag.net.message.consensus.v2;

import io.xdag.net.message.Message;
import io.xdag.net.message.MessageCode;
import io.xdag.utils.SimpleDecoder;
import io.xdag.utils.SimpleEncoder;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class BlockPartsMessage extends Message {
    private final long number;
    private final int parts;
    private final List<byte[]> data;

    public BlockPartsMessage(long number, int parts, List<byte[]> data) {
        super(MessageCode.MAIN_BLOCK_PARTS, null);

        this.number = number;
        this.parts = parts;
        this.data = data;

        SimpleEncoder enc = new SimpleEncoder();
        enc.writeLong(number);
        enc.writeInt(parts);
        enc.writeInt(data.size());
        for (byte[] b : data) {
            enc.writeBytes(b);
        }
        this.body = enc.toBytes();
    }

    public BlockPartsMessage(byte[] body) {
        super(MessageCode.MAIN_BLOCK_PARTS, null);

        SimpleDecoder dec = new SimpleDecoder(body);
        this.number = dec.readLong();
        this.parts = dec.readInt();
        this.data = new ArrayList<>();
        int n = dec.readInt();
        for (int i = 0; i < n; i++) {
            data.add(dec.readBytes());
        }

        this.body = body;
    }

    @Override
    public String toString() {
        return "BlockPartsMessage [number=" + number + ", parts=" + parts + "]";
    }
}
