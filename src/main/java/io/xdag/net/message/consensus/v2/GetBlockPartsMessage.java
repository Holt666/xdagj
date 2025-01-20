package io.xdag.net.message.consensus.v2;

import io.xdag.net.message.Message;
import io.xdag.net.message.MessageCode;
import io.xdag.utils.SimpleDecoder;
import io.xdag.utils.SimpleEncoder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GetBlockPartsMessage extends Message  {

    private final long number;
    private final int parts;

    public GetBlockPartsMessage(long number, int parts) {
        super(MessageCode.GET_MAIN_BLOCK_PARTS, BlockPartsMessage.class);

        this.number = number;
        this.parts = parts;

        SimpleEncoder enc = new SimpleEncoder();
        enc.writeLong(number);
        enc.writeInt(parts);
        this.body = enc.toBytes();
    }

    public GetBlockPartsMessage(byte[] body) {
        super(MessageCode.GET_MAIN_BLOCK_PARTS, BlockPartsMessage.class);

        SimpleDecoder dec = new SimpleDecoder(body);
        this.number = dec.readLong();
        this.parts = dec.readInt();

        this.body = body;
    }

    @Override
    public String toString() {
        return "GetBlockPartsMessage [number=" + number + ", parts = " + parts + "]";
    }
}
