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

/**
 * Request peer to provide a list of known nodes and authorized addresses.
 * The nodeInfo in this message is used to update the authorized address to IP bindings.
 * If the nodeInfo has changed, the previous connection should be closed and the bindings should be updated.
 */
@Getter
public class GetNodesMessage extends Message {

    /**
     * -- GETTER --
     *  Get the node info for updating authorized address bindings
     *
     * @return The node info containing IP and public key
     */
    private final NodeInfo nodeInfo;

    /**
     * Create a new GetNodesMessage with node info for updating authorized address bindings
     *
     * @param nodeInfo The node info containing the latest IP and public key for authorization
     */
    public GetNodesMessage(NodeInfo nodeInfo) {
        super(MessageCode.GET_NODES, NodesMessage.class);
        this.nodeInfo = nodeInfo;
        
        SimpleEncoder enc = encode();
        this.body = enc.toBytes();
    }

    /**
     * Create a GetNodesMessage from received bytes
     *
     * @param body The received message body
     */
    public GetNodesMessage(byte[] body) {
        super(MessageCode.GET_NODES, NodesMessage.class);
        this.body = body;
        
        SimpleDecoder dec = new SimpleDecoder(body);
        this.nodeInfo = NodeInfo.fromBytes(dec.readBytes());
    }

    protected SimpleEncoder encode() {
        SimpleEncoder enc = new SimpleEncoder();
        enc.writeBytes(nodeInfo.toBytes());
        return enc;
    }

}