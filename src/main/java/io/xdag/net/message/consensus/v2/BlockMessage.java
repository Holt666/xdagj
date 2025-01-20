package io.xdag.net.message.consensus.v2;

import io.xdag.core.v2.Block;
import io.xdag.net.message.Message;
import io.xdag.net.message.MessageCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BlockMessage extends Message  {

    private final Block block;

    public BlockMessage(Block block) {
        super(MessageCode.MAIN_BLOCK, null);

        this.block = block;

        this.body = block.toBytes();
    }

    public BlockMessage(byte[] body) {
        super(MessageCode.MAIN_BLOCK, null);

        this.block = Block.fromBytes(body);

        this.body = body;
    }

    @Override
    public String toString() {
        return "BlockMessage [block=" + block + "]";
    }

}
