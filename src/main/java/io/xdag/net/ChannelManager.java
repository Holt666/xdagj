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

import io.xdag.Kernel;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.xdag.core.v2.KernelV2;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChannelManager {

//    private final Kernel kernel;
    private final KernelV2 kernelv2;

    protected final ConcurrentHashMap<InetSocketAddress, Channel> channels = new ConcurrentHashMap<>();

    protected ConcurrentHashMap<String, Channel> activeChannels = new ConcurrentHashMap<>();

    public ChannelManager(Kernel kernel) {
//        this.kernel = kernel;
        kernelv2 = null;
    }

    public ChannelManager(KernelV2 kernelv2) {
        this.kernelv2 = kernelv2;
    }

    public boolean isActiveIP(String ip) {
        for (Channel c : activeChannels.values()) {
            if (c.getRemoteIp().equals(ip)) {
                return true;
            }
        }

        return false;
    }

    public boolean isActivePeer(String peerId) {
        return activeChannels.containsKey(peerId);
    }

    public int size() {
        return channels.size();
    }

    /**
     * Close all open channels
     */
    private void closeAllChannels() {
        for (Channel channel : channels.values()) {
            try {
                channel.close();
            } catch (Exception e) {
                log.warn("Failed to close channel: {}", channel.getRemoteAddress(), e);
            }
        }
        channels.clear();
    }

    public void add(Channel ch) {
        log.debug("Channel added: remoteAddress = {}:{}", ch.getRemoteIp(), ch.getRemotePort());
        channels.put(ch.getRemoteAddress(), ch);
    }

    public void remove(Channel ch) {
        log.debug("Channel removed: remoteAddress = {}:{}", ch.getRemoteIp(), ch.getRemotePort());

        channels.remove(ch.getRemoteAddress());
        if (ch.isActive()) {
            activeChannels.remove(ch.getRemotePeer().getPeerId());
            ch.setInactive();
        }
    }

    /**
     * Activate a channel with peer information
     */
    public void onChannelActive(Channel channel, Peer peer) {
        channel.setActive(peer);
        activeChannels.put(peer.getPeerId(), channel);
    }

    public List<Peer> getActivePeers() {
        List<Peer> list = new ArrayList<>();

        for (Channel c : activeChannels.values()) {
            list.add(c.getRemotePeer());
        }

        return list;
    }

    public Set<InetSocketAddress> getActiveAddresses() {
        Set<InetSocketAddress> activeAddresses = new HashSet<>();
        for (Channel channel : channels.values()) {
            if (channel.isActive() && channel.getRemotePeer() != null) {
                Peer peer = channel.getRemotePeer();
                activeAddresses.add(new InetSocketAddress(peer.getIp(), peer.getPort()));
            }
        }
        return activeAddresses;
    }

    public List<Channel> getActiveChannels() {
        return new ArrayList<>(activeChannels.values());
    }

    public List<Channel> getActiveChannels(List<String> peerIds) {
        List<Channel> list = new ArrayList<>();

        for (String peerId : peerIds) {
            if (activeChannels.containsKey(peerId)) {
                list.add(activeChannels.get(peerId));
            }
        }

        return list;
    }

    public List<Channel> getIdleChannels() {
        List<Channel> list = new ArrayList<>();

        for (Channel c : activeChannels.values()) {
            if (c.getMessageQueue().isIdle()) {
                list.add(c);
            }
        }

        return list;
    }
}
