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

package io.xdag.core;

import com.google.common.collect.Lists;
import io.xdag.Kernel;
import io.xdag.Wallet;
import io.xdag.config.Config;
import io.xdag.config.DevnetConfig;
import io.xdag.config.RandomXConstants;
import io.xdag.crypto.SampleKeys;
import io.xdag.crypto.Sign;
import io.xdag.db.BlockStore;
import io.xdag.db.OrphanBlockStore;
import io.xdag.db.rocksdb.*;
import io.xdag.crypto.RandomX;
import io.xdag.utils.XdagTime;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

import static io.xdag.BlockBuilder.*;
import static io.xdag.core.ImportResult.IMPORTED_BEST;
import static io.xdag.core.XdagField.FieldType.XDAG_FIELD_OUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@Slf4j
public class RewardTest {

    @Rule
    public TemporaryFolder root = new TemporaryFolder();

    Config config = new DevnetConfig();
    Wallet wallet;
    String pwd;
    Kernel kernel;
    DatabaseFactory dbFactory;

    String privString = "c85ef7d79691fe79573b1a7064c19c1a9819ebdbd1faaab1a8ec92344438aaf4";
    BigInteger privateKey = new BigInteger(privString, 16);
    SECPPrivateKey secretKey = SECPPrivateKey.create(privateKey, Sign.CURVE_NAME);

    @Before
    public void setUp() throws Exception {
        config.getNodeSpec().setStoreDir(root.newFolder().getAbsolutePath());
        config.getNodeSpec().setStoreBackupDir(root.newFolder().getAbsolutePath());

        pwd = "password";
        wallet = new Wallet(config);
        wallet.unlock(pwd);
        KeyPair key = KeyPair.create(SampleKeys.SRIVATE_KEY, Sign.CURVE, Sign.CURVE_NAME);
        wallet.setAccounts(Collections.singletonList(key));
        wallet.flush();

        kernel = new Kernel(config, key);
        dbFactory = new RocksdbFactory(config);

        BlockStore blockStore = new BlockStoreImpl(
                dbFactory.getDB(DatabaseName.INDEX),
                dbFactory.getDB(DatabaseName.TIME),
                dbFactory.getDB(DatabaseName.BLOCK),
                dbFactory.getDB(DatabaseName.TXHISTORY));

        blockStore.reset();
        OrphanBlockStore orphanBlockStore = new OrphanBlockStoreImpl(dbFactory.getDB(DatabaseName.ORPHANIND));
        orphanBlockStore.reset();

        kernel.setBlockStore(blockStore);
        kernel.setOrphanBlockStore(orphanBlockStore);
        kernel.setWallet(wallet);
    }

    @After
    public void tearDown() throws IOException {
        wallet.delete();
    }

    @Test
    public void testReward() {
        RandomXConstants.RANDOMX_TESTNET_FORK_HEIGHT = 4096;
        RandomXConstants.SEEDHASH_EPOCH_TESTNET_BLOCKS = 8;
        RandomXConstants.SEEDHASH_EPOCH_TESTNET_LAG = 2;

        RandomX randomXUtils = new RandomX(config);
        try {
            randomXUtils.start();
            kernel.setRandomx(randomXUtils);

            Bytes32 targetBlock = Bytes32.ZERO;

            KeyPair addrKey = KeyPair.create(secretKey, Sign.CURVE, Sign.CURVE_NAME);
            KeyPair poolKey = KeyPair.create(secretKey, Sign.CURVE, Sign.CURVE_NAME);
            long generateTime = 1600616700000L;
            
            // 1. add one address block
            Block addressBlock = generateAddressBlock(config, addrKey, generateTime);
            MockBlockchain blockchain = new MockBlockchain(kernel);
            ImportResult result = blockchain.tryToConnect(addressBlock);
            assertSame(result, IMPORTED_BEST);
            
            List<Address> pending = Lists.newArrayList();
            List<Block> extraBlockList = Lists.newLinkedList();
            Bytes32 ref = addressBlock.getHashLow();

            // 2. create 10 mainblocks instead of 20
            for (int i = 1; i <= 10; i++) {
                generateTime += 64000L;
                pending.clear();
                pending.add(new Address(ref, XDAG_FIELD_OUT,false));
                long time = XdagTime.msToXdagtimestamp(generateTime);
                long xdagTime = XdagTime.getEndOfEpoch(time);
                Block extraBlock = generateExtraBlock(config, poolKey, xdagTime, pending);
                result = blockchain.tryToConnect(extraBlock);
                assertSame(result, IMPORTED_BEST);
                ref = extraBlock.getHashLow();
                extraBlockList.add(extraBlock);
            }

            // 3. create 10 fork blocks instead of 30
            generateTime += 64000L;
            for (int i = 0; i < 10; i++) {
                pending.clear();
                pending.add(new Address(ref, XDAG_FIELD_OUT,false));
                long time = XdagTime.msToXdagtimestamp(generateTime);
                long xdagTime = XdagTime.getEndOfEpoch(time);
                Block extraBlock = generateExtraBlockGivenRandom(config, poolKey, xdagTime, pending, "3456");
                blockchain.tryToConnect(extraBlock);
                ref = extraBlock.getHashLow();
                extraBlockList.add(extraBlock);
            }

        } finally {
            try {
                randomXUtils.stop();
            } catch (Exception e) {
                // log error but don't throw
                log.error("Error stopping RandomX", e);
            }
        }
    }

    static class MockBlockchain extends BlockchainImpl {

        public MockBlockchain(Kernel kernel) {
            super(kernel);
        }

        @Override
        public XAmount getReward(long nmain) {
            XAmount start = getStartAmount(nmain);
            long nanoStart = start.toXAmount().toLong();
            return XAmount.ofXAmount(nanoStart >> (nmain >> 4));
        }
    }


}
