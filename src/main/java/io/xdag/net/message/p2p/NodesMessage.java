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
package io.xdag.net.message.p2p;

import io.xdag.net.message.Message;
import io.xdag.net.message.MessageCode;
import io.xdag.net.node.NodeInfo;
import io.xdag.utils.SimpleDecoder;
import io.xdag.utils.SimpleEncoder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Getter
public class NodesMessage extends Message {

    private final List<NodeInfo> nodes;
    private final Set<String> authorizedAddresses;

    /**
     * Create a NODES message with a list of nodes and authorized addresses
     *
     * @param nodes List of nodes to include in the message
     * @param authorizedAddresses Set of authorized addresses (only included by seed nodes)
     */
    public NodesMessage(List<NodeInfo> nodes, Set<String> authorizedAddresses) {
        super(MessageCode.NODES, null);

        this.nodes = nodes;
        this.authorizedAddresses = authorizedAddresses != null ? authorizedAddresses : new HashSet<>();

        SimpleEncoder enc = new SimpleEncoder();
        // Write nodes
        enc.writeInt(nodes.size());
        for (NodeInfo node : nodes) {
            enc.writeBytes(node.toBytes());
        }
        
        // Write authorized addresses
        enc.writeInt(this.authorizedAddresses.size());
        for (String address : this.authorizedAddresses) {
            enc.writeString(address);
        }
        
        this.body = enc.toBytes();
    }

    /**
     * Create a NODES message with only nodes list
     */
    public NodesMessage(List<NodeInfo> nodes) {
        this(nodes, null);
    }

    /**
     * Create a message from encoded data
     */
    public NodesMessage(byte[] body) {
        super(MessageCode.NODES, null);

        SimpleDecoder dec = new SimpleDecoder(body);
        
        // Read nodes
        int nodesSize = dec.readInt();
        this.nodes = new ArrayList<>();
        for (int i = 0; i < nodesSize; i++) {
            byte[] nodeBytes = dec.readBytes();
            NodeInfo node = NodeInfo.fromBytes(nodeBytes);
            if (node != null) {
                nodes.add(node);
            }
        }

        // Read authorized addresses
        int authorizedSize = dec.readInt();
        this.authorizedAddresses = new HashSet<>();
        for (int i = 0; i < authorizedSize; i++) {
            authorizedAddresses.add(dec.readString());
        }

        this.body = body;
    }

    @Override
    public String toString() {
        return "NodesMessage{" +
                "nodes=" + nodes.size() +
                ", authorizedAddresses=" + authorizedAddresses.size() +
                '}';
    }
} 