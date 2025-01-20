package io.xdag.core.v2;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultMessageSizeEstimator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.NettyRuntime;
import io.xdag.core.AbstractXdagLifecycle;
import io.xdag.utils.NettyUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SystemUtils;

@Slf4j
public class XdagPeerServer extends AbstractXdagLifecycle  {

    private final KernelV2 kernel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;
    private final int workerThreadPoolSize = NettyRuntime.availableProcessors() * 2;

    public XdagPeerServer(final KernelV2 kernel) {
        this.kernel = kernel;
    }

    @Override
    protected void doStart() {
        String ip = kernel.getConfig().getNodeSpec().getNodeIp();
        int port = kernel.getConfig().getNodeSpec().getNodePort();

        try {
            // Choose appropriate EventLoopGroup implementation based on OS
            if(org.apache.commons.lang3.SystemUtils.IS_OS_LINUX) {
                bossGroup = new EpollEventLoopGroup(1); // Set boss thread count to 1
                workerGroup = new EpollEventLoopGroup(workerThreadPoolSize);
            } else if(SystemUtils.IS_OS_MAC) {
                bossGroup = new KQueueEventLoopGroup(1); // Set boss thread count to 1
                workerGroup = new KQueueEventLoopGroup(workerThreadPoolSize);
            } else {
                bossGroup = new NioEventLoopGroup(1); // Set boss thread count to 1
                workerGroup = new NioEventLoopGroup(workerThreadPoolSize);
            }

            ServerBootstrap b = NettyUtils.nativeEventLoopGroup(bossGroup, workerGroup);

            // Configure TCP parameters
            b.childOption(ChannelOption.TCP_NODELAY, true);
            b.childOption(ChannelOption.SO_KEEPALIVE, true);
            b.childOption(ChannelOption.MESSAGE_SIZE_ESTIMATOR, DefaultMessageSizeEstimator.DEFAULT);
            b.childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, kernel.getConfig().getNodeSpec().getConnectionTimeout());

            // Add logging handler
            b.handler(new LoggingHandler());
            b.childHandler(new XdagChannelInitializerV2(kernel, null));

            log.debug("Xdag Node start host:[{}:{}].", ip, port);
            channelFuture = b.bind(ip, port).sync();
        } catch (Exception e) {
            log.error("Xdag Node start error:{}.", e.getMessage(), e);
            // Release resources on exception
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
            }
        }
    }

    @Override
    protected void doStop() {
        if (channelFuture != null && channelFuture.channel().isOpen()) {
            try {
                channelFuture.channel().close().sync();
                if (workerGroup != null) {
                    workerGroup.shutdownGracefully();
                }
                if (bossGroup != null) {
                    bossGroup.shutdownGracefully();
                }
                log.debug("Xdag Node closed.");
            } catch (Exception e) {
                log.error("Xdag Node close error:{}", e.getMessage(), e);
            }
        }
    }

}
