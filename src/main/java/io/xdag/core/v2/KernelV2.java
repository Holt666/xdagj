package io.xdag.core.v2;

import io.xdag.Wallet;
import io.xdag.config.Config;
import io.xdag.core.v2.db.DatabaseFactory;
import io.xdag.core.v2.db.LeveldbDatabase;
import io.xdag.utils.TimeUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.hyperledger.besu.crypto.KeyPair;

@Slf4j
@Getter
public class KernelV2 {
    public enum State {
        STOPPED, BOOTING, RUNNING, STOPPING
    }

    protected State state = State.STOPPED;

    protected Config config;
    protected Genesis genesis;

    protected Wallet wallet;
    protected KeyPair coinbase;

    protected DatabaseFactory dbFactory;
    protected Dagchain dagchain;
    protected XdagPeerClient client;

    protected XdagChannelManager channelMgr;
    protected PendingManager pendingMgr;
    protected XdagNodeManager nodeMgr;

    protected XdagPeerServer p2p;

    protected Thread consThread;
    protected XdagFastSync sync;
    protected XdagNewPow pow;

    public KernelV2(Config config, Genesis genesis, Wallet wallet, KeyPair coinbase) {
        this.config = config;
        this.genesis = genesis;
        this.wallet = wallet;
        this.coinbase = coinbase;
    }

    /**
     * Start the kernel.
     */
    public synchronized void start() {
        if (state != State.STOPPED) {
            return;
        } else {
            state = State.BOOTING;
        }

        // ====================================
        // print system info
        // ====================================
        log.info(config.getClientId());
        log.info("System booting up: network = {}, networkVersion = {}, coinbase = {}", config.getNetwork(),
                config.getNodeSpec().getNetworkVersion(),
                coinbase);

        TimeUtils.startNtpProcess();

        // ====================================
        // initialize blockchain database
        // ====================================
        dbFactory = new LeveldbDatabase.LeveldbFactory(config.getChainSpec().getChainDir());
        dagchain = new DagchainImpl(config, genesis, dbFactory);
        Block lastMainBlock = dagchain.getLatestMainBlock();
        log.info("Latest block number = {}, hash = {}", lastMainBlock.getNumber(), Hex.toHexString(lastMainBlock.getHash()));

        // ====================================
        // set up client
        // ====================================
        client = new XdagPeerClient(config, coinbase);

        // ====================================
        // start channel/pending/node manager
        // ====================================
        channelMgr = new XdagChannelManager(this);
        pendingMgr = new PendingManager(this);
        nodeMgr = new XdagNodeManager(this);

        pendingMgr.start();
        nodeMgr.start();

        // ====================================
        // start p2p module
        // ====================================
        p2p = new XdagPeerServer(this);
        p2p.start();

        // ====================================
        // start sync/consensus
        // ====================================
        sync = new XdagFastSync(config, dagchain, channelMgr);
        pow = new XdagNewPow(this);

        consThread = new Thread(pow::start, "consensus");
        consThread.start();

        state = State.RUNNING;
    }

}
