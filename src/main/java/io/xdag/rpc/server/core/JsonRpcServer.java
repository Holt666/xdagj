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
package io.xdag.rpc.server.core;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.xdag.config.spec.RPCSpec;
import io.xdag.rpc.api.XdagApi;
import io.xdag.rpc.server.handler.CorsHandler;
import io.xdag.rpc.server.handler.JsonRequestHandler;
import io.xdag.rpc.server.handler.JsonRpcHandler;
import io.xdag.rpc.server.handler.JsonRpcRequestHandler;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * JSON-RPC server implementation using Netty framework.
 * Handles incoming JSON-RPC requests and routes them to appropriate handlers.
 */
public class JsonRpcServer {
    // Configuration for RPC server
    private final RPCSpec rpcSpec;
    // API implementation for handling Xdag specific requests
    private final XdagApi xdagApi;
    // Netty channel for server communication
    private Channel channel;
    // Event loop group for accepting connections
    private EventLoopGroup bossGroup;
    // Event loop group for handling connections
    private EventLoopGroup workerGroup;

    /**
     * Constructs a new JsonRpcServer instance.
     * @param rpcSpec RPC configuration specifications
     * @param xdagApi Xdag API implementation
     */
    public JsonRpcServer(final RPCSpec rpcSpec, final XdagApi xdagApi) {
        this.rpcSpec = rpcSpec;
        this.xdagApi = xdagApi;
    }

    /**
     * Starts the JSON-RPC server.
     * Initializes handlers, SSL context, and starts listening for incoming connections.
     * @throws RuntimeException if server fails to start
     */
    public void start() {
        try {
            // Create request handlers
            List<JsonRpcRequestHandler> handlers = new ArrayList<>();
            handlers.add(new JsonRequestHandler(xdagApi));

            // Create SSL context (if HTTPS is enabled)
            final SslContext sslCtx;
            if (rpcSpec.isRpcEnableHttps()) {
                File certFile = new File(rpcSpec.getRpcHttpsCertFile());
                File keyFile = new File(rpcSpec.getRpcHttpsKeyFile());
                if (!certFile.exists() || !keyFile.exists()) {
                    throw new RuntimeException("SSL certificate or key file not found");
                }
                sslCtx = SslContextBuilder.forServer(certFile, keyFile).build();
            } else {
                sslCtx = null;
            }

            // Create event loop groups with configured thread counts
            bossGroup = new NioEventLoopGroup(rpcSpec.getRpcHttpBossThreads());
            workerGroup = new NioEventLoopGroup(rpcSpec.getRpcHttpWorkerThreads());

            // Configure server bootstrap
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            
                            // Add SSL handler if HTTPS is enabled
                            if (sslCtx != null) {
                                p.addLast(sslCtx.newHandler(ch.alloc()));
                            }

                            // Add HTTP codec for encoding/decoding HTTP messages
                            p.addLast(new HttpServerCodec());
                            // Add aggregator for combining HTTP message fragments
                            p.addLast(new HttpObjectAggregator(rpcSpec.getRpcHttpMaxContentLength()));
                            // Add CORS handler for cross-origin requests
                            p.addLast(new CorsHandler(rpcSpec.getRpcHttpCorsOrigins()));
                            // Add JSON-RPC handler for processing requests
                            p.addLast(new JsonRpcHandler(rpcSpec, handlers));
                        }
                    });

            // Bind to configured host and port
            channel = b.bind(InetAddress.getByName(rpcSpec.getRpcHttpHost()), rpcSpec.getRpcHttpPort()).sync().channel();
        } catch (Exception e) {
            stop();
            throw new RuntimeException("Failed to start JSON-RPC server", e);
        }
    }

    /**
     * Stops the JSON-RPC server.
     * Closes the channel and shuts down event loop groups gracefully.
     */
    public void stop() {
        if (channel != null) {
            channel.close();
            channel = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            bossGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }
    }
}
