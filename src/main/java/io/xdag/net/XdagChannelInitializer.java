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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.socket.SocketChannel;
import io.xdag.Kernel;
import io.xdag.net.NodeManager.Node;

import java.net.InetSocketAddress;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class XdagChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final Kernel kernel;
    private final ChannelManager channelMgr;
    private final Node remoteNode;
    @Getter
    private final boolean discoveryMode;

    public XdagChannelInitializer(Kernel kernel, Node remoteNode, boolean discoveryMode) {
        this.kernel = kernel;
        this.channelMgr = kernel.getChannelMgr();

        this.remoteNode = remoteNode;
        this.discoveryMode = discoveryMode;
    }

    public XdagChannelInitializer(Kernel kernel, Node remoteNode) {
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
            
            Channel channel = new Channel(ch);
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
