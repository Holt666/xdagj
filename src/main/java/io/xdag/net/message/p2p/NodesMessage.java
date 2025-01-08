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

import io.xdag.net.NodeManager.Node;
import io.xdag.net.message.Message;
import io.xdag.net.message.MessageCode;
import io.xdag.utils.SimpleDecoder;
import io.xdag.utils.SimpleEncoder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class NodesMessage extends Message {

    public static final int MAX_NODES = 256;

    private final List<Node> nodes;

    /**
     * Create a NODES message.
     *
     * @param nodes
     */
    public NodesMessage(List<Node> nodes) {
        super(MessageCode.NODES, null);

        this.nodes = nodes;

        SimpleEncoder enc = new SimpleEncoder();
        enc.writeInt(nodes.size());
        for (Node n : nodes) {
            enc.writeString(n.getIp());
            enc.writeInt(n.getPort());
        }
        this.body = enc.toBytes();
    }

    /**
     * Parse a NODES message from byte array.
     *
     * @param body
     */
    public NodesMessage(byte[] body) {
        super(MessageCode.NODES, null);

        this.nodes = new ArrayList<>();
        SimpleDecoder dec = new SimpleDecoder(body);
        for (int i = 0, size = dec.readInt(); i < size; i++) {
            String host = dec.readString();
            int port = dec.readInt();
            nodes.add(new Node(host, port));
        }

        this.body = body;
    }

    public boolean validate() {
        return nodes != null && nodes.size() <= MAX_NODES;
    }

    public List<Node> getNodes() {
        return nodes;
    }

    @Override
    public String toString() {
        return "NodesMessage [# nodes =" + nodes.size() + "]";
    }
} 