package io.xdag.core.v2;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.xdag.net.*;
import io.xdag.net.message.MessageQueue;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

@Slf4j
@Getter
@Setter
public class XdagChannel {

    private SocketChannel socket;
    private boolean isInbound;
    private InetSocketAddress remoteAddress;
    private Peer remotePeer;
    private MessageQueue msgQueue;
    private boolean isActive;
    private XdagP2pHandlerV2 p2pHandler;

    /**
     * Creates a new channel instance with the given socket
     *
     * @param socket The socket channel for network communication
     */
    public XdagChannel(SocketChannel socket) {
        this.socket = socket;
    }

    /**
     * Initializes the channel with pipeline handlers and network settings
     *
     * @param pipe Pipeline to add handlers to
     * @param isInbound Whether this is an inbound connection
     * @param remoteAddress Remote peer's address
     * @param kernel Reference to the main kernel
     */
    public void init(ChannelPipeline pipe, boolean isInbound, InetSocketAddress remoteAddress, KernelV2 kernel) {
        this.isInbound = isInbound;
        this.remoteAddress = remoteAddress;
        this.remotePeer = null;

        this.msgQueue = new MessageQueue(kernel.getConfig());

        // Register channel handlers
        if (isInbound) {
            pipe.addLast("inboundLimitHandler",
                    new ConnectionLimitHandler(kernel.getConfig().getNodeSpec().getNetMaxInboundConnectionsPerIp()));
        }
        pipe.addLast("readTimeoutHandler", new ReadTimeoutHandler(kernel.getConfig().getNodeSpec().getNetChannelIdleTimeout(), TimeUnit.MILLISECONDS));
        pipe.addLast("xdagFrameHandler", new XdagFrameHandler(kernel.getConfig()));
        pipe.addLast("xdagMessageHandler", new XdagMessageHandler(kernel.getConfig()));
        p2pHandler = new XdagP2pHandlerV2(this, kernel);
        pipe.addLast("xdagP2pHandler", p2pHandler);
    }

    /**
     * Closes the socket connection
     */
    public void close() {
        socket.close();
    }

    /**
     * Gets the message queue for this channel
     */
    public MessageQueue getMessageQueue() {
        return msgQueue;
    }

    /**
     * Checks if this is an inbound connection
     */
    public boolean isInbound() {
        return isInbound;
    }

    /**
     * Checks if this is an outbound connection
     */
    public boolean isOutbound() {
        return !isInbound();
    }

    /**
     * Checks if the channel is active
     */
    public boolean isActive() {
        return isActive;
    }

    /**
     * Activates the channel with the given remote peer
     *
     * @param remotePeer The remote peer to activate with
     */
    public void setActive(Peer remotePeer) {
        this.remotePeer = remotePeer;
        this.isActive = true;
    }

    /**
     * Deactivates the channel
     */
    public void setInactive() {
        this.isActive = false;
    }

    /**
     * Gets the remote peer's IP address
     */
    public String getRemoteIp() {
        return remoteAddress.getAddress().getHostAddress();
    }

    /**
     * Gets the remote peer's port number
     */
    public int getRemotePort() {
        return remoteAddress.getPort();
    }

    @Override
    public String toString() {
        return "Channel [" + (isInbound ? "Inbound" : "Outbound") + ", remotePeer = " + remotePeer + "]";
    }

}
