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
package io.xdag.core.state;

import io.xdag.core.XAmount;

public interface AccountState {

    /**
     * Returns an account if exists.
     */
    Account getAccount(byte[] address);

    /**
     * Increases the nonce of an account.
     */
    long increaseNonce(byte[] address);

    /**
     * Adjusts the available balance of an account.
     */
    void adjustAvailable(byte[] address, XAmount delta);

    /**
     * Adjusts the locked balance of an account.
     */
    void adjustLocked(byte[] address, XAmount delta);

    /**
     * Makes a snapshot and starts tracking further updates.
     */
    AccountState track();

    /**
     * Commits all updates since last snapshot.
     */
    void commit();

    /**
     * Reverts all updates since last snapshot.
     */
    void rollback();

    /**
     * check if an account exists
     * 
     * @param address
     *            address
     * @return exists
     */
    boolean exists(byte[] address);

    /**
     * set the nonce to a given value.
     *
     * @param address
     *            address
     * @param nonce
     *            nonce
     * @return nonce
     */
    long setNonce(byte[] address, long nonce);

    /**
     * Clone this AccountState, including all the uncommitted changes.
     */
    AccountState clone();
}
