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

package io.xdag.net.message;

import io.xdag.net.message.consensus.*;
import io.xdag.net.message.consensus.v2.*;
import io.xdag.net.message.consensus.v2.NewBlockMessage;
import io.xdag.net.message.p2p.DisconnectMessage;
import io.xdag.net.message.p2p.GetNodesMessage;
import io.xdag.net.message.p2p.HelloMessage;
import io.xdag.net.message.p2p.InitMessage;
import io.xdag.net.message.p2p.NodesMessage;
import io.xdag.net.message.p2p.PingMessage;
import io.xdag.net.message.p2p.PongMessage;
import io.xdag.net.message.p2p.WorldMessage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageFactory {
    /**
     * Decode a raw message.
     *
     * @param code
     *            The message code
     * @param body
     *            The message body
     * @return The decoded message, or NULL if the message type is unknown
     * @throws MessageException
     *             when the encoding is illegal
     */
    public Message create(byte code, byte[] body) throws MessageException {
        MessageCode c = MessageCode.of(code);
        if (c == null) {
            return null;
        }

        try {
            return switch (c) {
                /* v1 */
                case HANDSHAKE_INIT -> new InitMessage(body);
                case HANDSHAKE_HELLO -> new HelloMessage(body);
                case HANDSHAKE_WORLD -> new WorldMessage(body);
                case DISCONNECT -> new DisconnectMessage(body);
                case PING -> new PingMessage(body);
                case PONG -> new PongMessage(body);
                case GET_NODES -> new GetNodesMessage(body);
                case NODES -> new NodesMessage(body);
                case NEW_BLOCK -> new io.xdag.net.message.consensus.NewBlockMessage(body);
                case SYNCBLOCK_REQUEST -> new SyncBlockRequestMessage(body);
                case SYNCBLOCK_REPLY -> new SyncBlockResponseMessage(body);
                /* v2 */
                case TRANSACTION -> new TransactionMessage(body);
                case NEW_MAIN_BLOCK -> new NewBlockMessage(body);
                case GET_MAIN_BLOCK -> new GetBlockMessage(body);
                case MAIN_BLOCK -> new BlockMessage(body);
                case GET_MAIN_BLOCK_PARTS -> new GetBlockPartsMessage(body);
                case MAIN_BLOCK_PARTS -> new BlockPartsMessage(body);
            };
        } catch (Exception e) {
            throw new MessageException("Failed to decode message", e);
        }
    }

}
