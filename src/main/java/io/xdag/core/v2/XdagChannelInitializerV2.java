package io.xdag.core.v2;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.socket.SocketChannel;
import io.xdag.net.Frame;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;

@Slf4j
public class XdagChannelInitializerV2 extends ChannelInitializer<SocketChannel> {

    private final KernelV2 kernel;
    private final XdagChannelManager channelMgr;
    private final XdagNodeManager.Node remoteNode;
    @Getter
    private final boolean discoveryMode;

    public XdagChannelInitializerV2(KernelV2 kernel, XdagNodeManager.Node remoteNode, boolean discoveryMode) {
        this.kernel = kernel;
        this.channelMgr = kernel.getChannelMgr();

        this.remoteNode = remoteNode;
        this.discoveryMode = discoveryMode;
    }

    public XdagChannelInitializerV2(KernelV2 kernel, XdagNodeManager.Node remoteNode) {
        this(kernel, remoteNode, false);
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        try {
            InetSocketAddress address = isServerMode() ? ch.remoteAddress() : remoteNode.toAddress();
            log.debug("New {} channel: remoteAddress = {}:{}", isServerMode() ? "inbound" : "outbound",
                    address.getAddress().getHostAddress(), address.getPort());

//            if (!channelMgr.isAcceptable(address)) {
//                log.debug("Rejecting inbound connection: {}", address);
//                ch.disconnect();
//                return;
//            }

            int bufferSize = Frame.HEADER_SIZE + kernel.getConfig().getNodeSpec().getNetMaxFrameBodySize();
            ch.config().setRecvByteBufAllocator(new FixedRecvByteBufAllocator(bufferSize));
            ch.config().setOption(ChannelOption.SO_RCVBUF, bufferSize);
            ch.config().setOption(ChannelOption.TCP_NODELAY, true);

            XdagChannel channel = new XdagChannel(ch);
            channel.init(ch.pipeline(), isServerMode(), address, kernel);
            if (!isDiscoveryMode()) {
                channelMgr.add(channel);
            }

            // notify disconnection to channel manager
            ch.closeFuture().addListener(future -> {
                if (!isDiscoveryMode()) {
                    channelMgr.remove(channel);
                }
            });
        } catch (Exception e) {
            log.error("Unexpected error: [{}]", e.getMessage(), e);
        }
    }

    public boolean isServerMode() {
        return remoteNode == null;
    }

}
