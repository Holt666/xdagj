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

import io.xdag.utils.SimpleEncoder;
import io.xdag.net.message.Message;
import io.xdag.net.message.MessageCode;
import io.xdag.utils.SimpleDecoder;

public class PongMessage extends Message {

    private final long timestamp;
    private final boolean isValid;

    public PongMessage(boolean isValid) {
        super(MessageCode.PONG, null);

        this.timestamp = System.currentTimeMillis();
        this.isValid = isValid;

        SimpleEncoder enc = new SimpleEncoder();
        enc.writeLong(timestamp);
        enc.writeBoolean(isValid);
        this.body = enc.toBytes();
    }

    public PongMessage(byte[] body) {
        super(MessageCode.PONG, null);

        SimpleDecoder dec = new SimpleDecoder(body);
        this.timestamp = dec.readLong();
        this.isValid = dec.readBoolean();

        this.body = body;
    }

    public boolean isValid() {
        return isValid;
    }

    @Override
    public String toString() {
        return "PongMessage [timestamp=" + timestamp + ", isValid=" + isValid + "]";
    }
}
