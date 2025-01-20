package io.xdag.config.spec;

import io.xdag.core.XAmount;
import io.xdag.core.v2.TransactionType;

import java.io.File;

public interface ChainSpec {

    File getChainDir();
    XAmount getMinTransactionFee();
    int getMaxTransactionDataSize(TransactionType type);
    XAmount getBlockReward(long blockNumber);

}
