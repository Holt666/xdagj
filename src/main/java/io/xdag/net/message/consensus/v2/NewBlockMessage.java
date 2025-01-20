package io.xdag.net.message.consensus.v2;

import io.xdag.core.v2.Block;
import io.xdag.net.message.Message;
import io.xdag.net.message.MessageCode;
import lombok.Getter;

@Getter
public class NewBlockMessage extends Message  {
    private final Block block;

    public NewBlockMessage(Block block) {
        super(MessageCode.MAIN_BLOCK, null);

        this.block = block;

        this.body = block.toBytes();
    }

    public NewBlockMessage(byte[] body) {
        super(MessageCode.MAIN_BLOCK, null);

        this.block = Block.fromBytes(body);

        this.body = body;
    }

    @Override
    public String toString() {
        return "NewBlockMessage [Block=" + block + "]";
    }
}
