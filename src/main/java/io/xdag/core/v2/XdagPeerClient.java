package io.xdag.core.v2;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultMessageSizeEstimator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.xdag.config.Config;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.hyperledger.besu.crypto.KeyPair;

import java.util.concurrent.ThreadFactory;

import static io.xdag.crypto.Keys.toBytesAddress;
import static io.xdag.utils.WalletUtils.toBase58;

@Slf4j
@Getter
@Setter
public class XdagPeerClient {

    private static final ThreadFactory factory = new BasicThreadFactory.Builder()
            .namingPattern("XdagClient-thread-%d")
            .daemon(true)
            .build();

    private final String ip;
    private final int port;
    private final KeyPair coinbase;
    private final EventLoopGroup workerGroup;
    private final Config config;
    private XdagNodeManager.Node node;

    /**
     * Constructor for PeerClient
     * @param config Network configuration
     * @param coinbase Keypair for node identity
     */
    public XdagPeerClient(Config config, KeyPair coinbase) {
        this.config = config;
        this.ip = config.getNodeSpec().getNodeIp();
        this.port = config.getNodeSpec().getNodePort();
        this.coinbase = coinbase;
        this.node = new XdagNodeManager.Node(ip, port);
        this.workerGroup = new NioEventLoopGroup(0, factory);
    }

    /**
     * Get peer ID derived from coinbase key
     */
    public String getPeerId() {
        return toBase58(toBytesAddress(coinbase));
    }

    /**
     * Connect to a remote node
     * @param remoteNode Target node to connect to
     * @param xdagChannelInitializer Channel initializer
     * @return ChannelFuture for the connection
     */
    public ChannelFuture connect(XdagNodeManager.Node remoteNode, XdagChannelInitializerV2 xdagChannelInitializer) {
        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.TCP_NODELAY, true);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR, DefaultMessageSizeEstimator.DEFAULT);
        b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getNodeSpec().getConnectionTimeout());
        b.remoteAddress(remoteNode.toAddress());
        b.handler(xdagChannelInitializer);
        return b.connect();
    }

    /**
     * Gracefully shutdown the client
     */
    public void close() {
        log.debug("Shutdown XdagClient");
        workerGroup.shutdownGracefully();
        workerGroup.terminationFuture().syncUninterruptibly();
    }

}
