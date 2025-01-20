package io.xdag.core.v2;

import io.xdag.Kernel;
import io.xdag.net.Channel;
import io.xdag.net.Peer;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class XdagChannelManager {

    private final KernelV2 kernelv2;

    protected final ConcurrentHashMap<InetSocketAddress, XdagChannel> channels = new ConcurrentHashMap<>();

    protected ConcurrentHashMap<String, XdagChannel> activeChannels = new ConcurrentHashMap<>();

    public XdagChannelManager(KernelV2 kernelv2) {
        this.kernelv2 = kernelv2;
    }

    public boolean isActiveIP(String ip) {
        for (XdagChannel c : activeChannels.values()) {
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
        for (XdagChannel channel : channels.values()) {
            try {
                channel.close();
            } catch (Exception e) {
                log.warn("Failed to close channel: {}", channel.getRemoteAddress(), e);
            }
        }
        channels.clear();
    }

    public void add(XdagChannel ch) {
        log.debug("Channel added: remoteAddress = {}:{}", ch.getRemoteIp(), ch.getRemotePort());
        channels.put(ch.getRemoteAddress(), ch);
    }

    public void remove(XdagChannel ch) {
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
    public void onChannelActive(XdagChannel channel, Peer peer) {
        channel.setActive(peer);
        activeChannels.put(peer.getPeerId(), channel);
    }

    public List<Peer> getActivePeers() {
        List<Peer> list = new ArrayList<>();

        for (XdagChannel c : activeChannels.values()) {
            list.add(c.getRemotePeer());
        }

        return list;
    }

    public Set<InetSocketAddress> getActiveAddresses() {
        Set<InetSocketAddress> activeAddresses = new HashSet<>();
        for (XdagChannel channel : channels.values()) {
            if (channel.isActive() && channel.getRemotePeer() != null) {
                Peer peer = channel.getRemotePeer();
                activeAddresses.add(new InetSocketAddress(peer.getIp(), peer.getPort()));
            }
        }
        return activeAddresses;
    }

    public List<XdagChannel> getActiveChannels() {
        return new ArrayList<>(activeChannels.values());
    }

    public List<XdagChannel> getActiveChannels(List<String> peerIds) {
        List<XdagChannel> list = new ArrayList<>();

        for (String peerId : peerIds) {
            if (activeChannels.containsKey(peerId)) {
                list.add(activeChannels.get(peerId));
            }
        }

        return list;
    }

    public List<XdagChannel> getIdleChannels() {
        List<XdagChannel> list = new ArrayList<>();

        for (XdagChannel c : activeChannels.values()) {
            if (c.getMessageQueue().isIdle()) {
                list.add(c);
            }
        }

        return list;
    }
}
