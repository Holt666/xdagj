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
package io.xdag.net;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.xdag.Kernel;
import io.xdag.Network;
import io.xdag.config.Config;
import io.xdag.config.spec.NodeSpec;
import io.xdag.consensus.XdagPow;
import io.xdag.consensus.XdagSync;
import io.xdag.core.*;
import io.xdag.net.NodeManager.Node;
import io.xdag.net.message.Message;
import io.xdag.net.message.MessageQueue;
import io.xdag.net.message.ReasonCode;
import io.xdag.net.message.p2p.*;
import io.xdag.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.hyperledger.besu.crypto.SecureRandomProvider;

import static io.xdag.net.message.p2p.NodesMessage.MAX_NODES;

@Slf4j
public class XdagP2pHandler extends SimpleChannelInboundHandler<Message> {

    private static final ScheduledExecutorService exec = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactory() {
                private final AtomicInteger cnt = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "node-" + cnt.getAndIncrement());
                }
            });

    private final Channel channel;
    private final Config config;
    private final NodeSpec nodeSpec;
    private final Blockchain chain;

    private final ChannelManager channelMgr;
    private final NodeManager nodeMgr;
    private final PeerClient client;
    private final XdagSync sync;
    private final XdagPow pow;
    private final MessageQueue msgQueue;

    private final AtomicBoolean isHandshakeDone = new AtomicBoolean(false);

    private volatile ScheduledFuture<?> pingPong = null;
    private volatile ScheduledFuture<?> getNodes = null;

    private byte[] secret = SecureRandomProvider.publicSecureRandom().generateSeed(InitMessage.SECRET_LENGTH);
    private long timestamp = TimeUtils.currentTimeMillis();
    private long lastPing;


    public XdagP2pHandler(Channel channel, Kernel kernel) {
        this.channel = channel;
        this.config = kernel.getConfig();
        this.nodeSpec = kernel.getConfig().getNodeSpec();

        this.chain = kernel.getBlockchain();
        this.channelMgr = kernel.getChannelMgr();
        this.nodeMgr = kernel.getNodeMgr();
        this.client = kernel.getClient();

        this.sync = kernel.getSync();
        this.pow = kernel.getPow();
        this.msgQueue = channel.getMessageQueue();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.debug("Node handler active, remoteIp = {}, remotePort = {}", channel.getRemoteIp(), channel.getRemotePort());

        // activate message queue
        msgQueue.activate(ctx);

        // disconnect if too many connections
        if (channel.isInbound() && channelMgr.size() >= config.getNodeSpec().getNetMaxInboundConnections()) {
            msgQueue.disconnect(ReasonCode.TOO_MANY_PEERS);
            return;
        }

        if (channel.isInbound()) {
            msgQueue.sendMessage(new InitMessage(secret, timestamp));
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.debug("Node handler inactive, remoteIp = {}", channel.getRemoteIp());

        // deactivate the message queue
        msgQueue.deactivate();

        // stop scheduled workers
        if (getNodes != null) {
            getNodes.cancel(false);
            getNodes = null;
        }

        if (pingPong != null) {
            pingPong.cancel(false);
            pingPong = null;
        }

        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.debug("Exception in Node handler, remoteIp = {}, remotePort = {}", channel.getRemoteIp(), channel.getRemotePort(), cause);

        // close connection on exception
        ctx.close();
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, Message msg) {
        log.trace("Received message: {}", msg);

        switch (msg.getCode()) {
            /* node */
            case DISCONNECT -> onDisconnect(ctx, (DisconnectMessage) msg);
            case PING -> onPing();
            case PONG -> onPong();
            case HANDSHAKE_INIT -> onHandshakeInit((InitMessage) msg);
            case HANDSHAKE_HELLO -> onHandshakeHello((HelloMessage) msg);
            case HANDSHAKE_WORLD -> onHandshakeWorld((WorldMessage) msg);
            case GET_NODES -> onGetNodes();
            case NODES -> onNodes((NodesMessage) msg);

            /* sync */
            case NEW_BLOCK -> onXdag(msg);
            case SYNCBLOCK_REQUEST,
                 SYNCBLOCK_REPLY -> onSync(msg);


        /* pow */

        default -> ctx.fireChannelRead(msg);
        }
    }

    protected void onDisconnect(ChannelHandlerContext ctx, DisconnectMessage msg) {
        ReasonCode reason = msg.getReason();
        log.info("Received a DISCONNECT message: reason = {}, remoteIP = {}",
                reason, channel.getRemoteIp());

        ctx.close();
    }

    protected void onPing() {
        PongMessage pong = new PongMessage();
        msgQueue.sendMessage(pong);
        lastPing = TimeUtils.currentTimeMillis();
    }

    protected void onPong() {
        if (lastPing > 0) {
            long latency = TimeUtils.currentTimeMillis() - lastPing;
            channel.getRemotePeer().setLatency(latency);
        }
    }

    protected void onGetNodes() {
        if (!isHandshakeDone.get()) {
            return;
        }

        List<InetSocketAddress> activeAddresses = new ArrayList<>(channelMgr.getActiveAddresses());
        Collections.shuffle(activeAddresses); // shuffle the list to balance the load on nodes
        NodesMessage nodesMsg = new NodesMessage(activeAddresses.stream().limit(MAX_NODES).map(Node::new).collect(Collectors.toList()));
        msgQueue.sendMessage(nodesMsg);
    }

    protected void onNodes(NodesMessage msg) {
        if (!isHandshakeDone.get()) {
            return;
        }

        if (msg.validate()) {
            nodeMgr.addNodes(msg.getNodes());
        }
    }

    protected void onHandshakeInit(InitMessage msg) {
        // unexpected
        if (channel.isInbound()) {
            return;
        }

        // check message
        if (!msg.validate()) {
            this.msgQueue.disconnect(ReasonCode.INVALID_HANDSHAKE);
            return;
        }

        // record the secret
        this.secret = msg.getSecret();
        this.timestamp = msg.getTimestamp();

        // send the HELLO message
        this.msgQueue.sendMessage(new HelloMessage(nodeSpec.getNetwork(), nodeSpec.getNetworkVersion(),
                client.getPeerId(), client.getPort(), config.getClientId(),
                config.getClientCapabilities().toArray(), chain.getLatestMainBlockNumber(),
                secret, client.getCoinbase()));
    }

    protected void onHandshakeHello(HelloMessage msg) {
        // unexpected
        if (channel.isOutbound()) {
            return;
        }
        Peer peer = msg.getPeer(channel.getRemoteIp());

        // check peer
        ReasonCode code = checkPeer(peer);
        if (code != null) {
            msgQueue.disconnect(code);
            return;
        }

        // check message
        if (!Arrays.equals(secret, msg.getSecret()) || !msg.validate(config)) {
            msgQueue.disconnect(ReasonCode.INVALID_HANDSHAKE);
            return;
        }

        // Send the WORLD message
        log.debug("Sending WORLD message to: {}", channel.getRemoteAddress());
        this.msgQueue.sendMessage(new WorldMessage(nodeSpec.getNetwork(), nodeSpec.getNetworkVersion(), client.getPeerId(),
                client.getPort(), config.getClientId(), config.getClientCapabilities().toArray(),
                chain.getLatestMainBlockNumber(),
                msg.getSecret(), client.getCoinbase()));

        // Handshake done
        onHandshakeDone(peer);
    }

    protected void onHandshakeWorld(WorldMessage msg) {
        // unexpected
        if (channel.isInbound()) {
            return;
        }
        Peer peer = msg.getPeer(channel.getRemoteIp());

        // check peer
        ReasonCode code = checkPeer(peer);
        if (code != null) {
            msgQueue.disconnect(code);
            return;
        }

        // check message
        if (!Arrays.equals(secret, msg.getSecret()) || !msg.validate(config)) {
            msgQueue.disconnect(ReasonCode.INVALID_HANDSHAKE);
            return;
        }

        // handshake done
        onHandshakeDone(peer);
    }

    protected void onSync(Message msg) {
        if (!isHandshakeDone.get()) {
            return;
        }

        sync.onMessage(channel, msg);
    }

    protected void onXdag(Message msg) {
        if (!isHandshakeDone.get()) {
            return;
        }

        pow.onMessage(channel, msg);
    }

    /**
     * Check whether the peer is valid to connect.
     */
    private ReasonCode checkPeer(Peer peer) {
        // has to be same network
        if (!nodeSpec.getNetwork().equals(peer.getNetwork())) {
            return ReasonCode.BAD_NETWORK;
        }

        // has to be compatible version
        if (nodeSpec.getNetworkVersion() != peer.getNetworkVersion()) {
            return ReasonCode.BAD_NETWORK_VERSION;
        }

        // not connected
        if (client.getPeerId().equals(peer.getPeerId()) || channelMgr.isActivePeer(peer.getPeerId())) {
            return ReasonCode.DUPLICATED_PEER_ID;
        }

        if (nodeSpec.getSeedNodesAddresses().contains(peer.getPeerId()) // is a validator
                && channelMgr.isActiveIP(channel.getRemoteIp()) // already connected
                && config.getNetwork() == Network.MAINNET) { // on main net
            return ReasonCode.SEED_IP_LIMITED;
        }

        return null;
    }

    private void onHandshakeDone(Peer peer) {
        if (isHandshakeDone.compareAndSet(false, true)) {
            // Register into channel manager
            channelMgr.onChannelActive(channel, peer);

            // start peers exchange
            getNodes = exec.scheduleAtFixedRate(() -> msgQueue.sendMessage(new GetNodesMessage()),
                    channel.isInbound() ? 2 : 0, 2, TimeUnit.MINUTES);

            // start ping pong
            pingPong = exec.scheduleAtFixedRate(() -> msgQueue.sendMessage(new PingMessage()),
                    channel.isInbound() ? 1 : 0, 1, TimeUnit.MINUTES);
        } else {
            msgQueue.disconnect(ReasonCode.HANDSHAKE_EXISTS);
        }
    }

}
