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

package io.xdag.wallet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.hyperledger.besu.crypto.KeyPair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.xdag.Wallet;
import io.xdag.config.Config;
import io.xdag.config.Constants;
import io.xdag.config.DevnetConfig;
import io.xdag.crypto.Keys;
import io.xdag.crypto.SampleKeys;
import io.xdag.crypto.Sign;

public class WalletTest {

    private String pwd;
    private Wallet wallet;
    private Config config;

    @Before
    public void setUp() {
        pwd = "password";
        config = new DevnetConfig(Constants.DEFAULT_ROOT_DIR);
        wallet = new Wallet(config);
        wallet.unlock(pwd);
        KeyPair key = KeyPair.create(SampleKeys.SRIVATE_KEY, Sign.CURVE, Sign.CURVE_NAME);
        wallet.setAccounts(Collections.singletonList(key));
        wallet.flush();
        wallet.lock();
    }

    @Test
    public void testGetters() {
        wallet.unlock(pwd);
        assertEquals(pwd, wallet.getPassword());
    }

    @Test
    public void testUnlock() {
        assertFalse(wallet.isUnlocked());

        wallet.unlock(pwd);
        assertTrue(wallet.isUnlocked());

        assertEquals(1, wallet.getAccounts().size());
    }

    @Test
    public void testLock() {
        wallet.unlock(pwd);
        wallet.lock();
        assertFalse(wallet.isUnlocked());
    }

    @Test
    public void testAddAccounts()
            throws InvalidAlgorithmParameterException, NoSuchAlgorithmException, NoSuchProviderException {
        wallet.unlock(pwd);
        wallet.setAccounts(Collections.emptyList());
        KeyPair key1 = Keys.createEcKeyPair();
        KeyPair key2 = Keys.createEcKeyPair();
        wallet.addAccounts(Arrays.asList(key1, key2));
        List<KeyPair> accounts = wallet.getAccounts();
        KeyPair k1 = accounts.get(0);
        KeyPair k2 = accounts.get(1);
        assertEquals(k1, key1);
        assertEquals(k2, key2);
    }

    @Test
    public void testFlush() throws InterruptedException {
        File file = wallet.getFile();
        long sz = wallet.getFile().length();
        Thread.sleep(500);

        wallet.unlock(pwd);
        wallet.setAccounts(Collections.emptyList());
        assertEquals(sz, file.length());

        wallet.flush();
        assertTrue(file.length() < sz);
    }

    @Test
    public void testChangePassword() {
        String pwd2 = "passw0rd2";

        wallet.unlock(pwd);
        wallet.changePassword(pwd2);
        wallet.flush();
        wallet.lock();

        assertFalse(wallet.unlock(pwd));
        assertTrue(wallet.unlock(pwd2));
    }

    @Test
    public void testAddAccountRandom()
            throws InvalidAlgorithmParameterException, NoSuchAlgorithmException, NoSuchProviderException {
        wallet.unlock(pwd);
        int oldAccountSize = wallet.getAccounts().size();
        wallet.addAccountRandom();
        assertEquals(oldAccountSize + 1, wallet.getAccounts().size());
    }

    @Test
    public void testRemoveAccount()
            throws InvalidAlgorithmParameterException, NoSuchAlgorithmException, NoSuchProviderException {
        wallet.unlock(pwd);
        int oldAccountSize = wallet.getAccounts().size();
        KeyPair key = Keys.createEcKeyPair();
        wallet.addAccount(key);
        assertEquals(oldAccountSize + 1, wallet.getAccounts().size());
        wallet.removeAccount(key);
        assertEquals(oldAccountSize, wallet.getAccounts().size());
        wallet.addAccount(key);
        assertEquals(oldAccountSize + 1, wallet.getAccounts().size());
        wallet.removeAccount(Keys.toBytesAddress(key));
        assertEquals(oldAccountSize, wallet.getAccounts().size());
    }

    @Test
    public void testInitializeHdWallet() {
        wallet.initializeHdWallet(SampleKeys.MNEMONIC);
        assertEquals(0, wallet.getNextAccountIndex());
        assertEquals(SampleKeys.MNEMONIC, wallet.getMnemonicPhrase());
    }

    @Test
    public void testAddAccountWithNextHdKey() {
        wallet.unlock(pwd);
        wallet.initializeHdWallet(SampleKeys.MNEMONIC);
        int hdkeyCount = 5;
        for (int i = 0; i < hdkeyCount; i++) {
            wallet.addAccountWithNextHdKey();
        }
        assertEquals(hdkeyCount, wallet.getNextAccountIndex());
    }

    @Test
    public void testHDKeyRecover() {
        wallet.unlock(pwd);
        wallet.initializeHdWallet(SampleKeys.MNEMONIC);
        List<KeyPair> keyPairList1 = Lists.newArrayList();
        int hdkeyCount = 5;
        for (int i = 0; i < hdkeyCount; i++) {
            KeyPair key = wallet.addAccountWithNextHdKey();
            keyPairList1.add(key);
        }

        assertEquals(hdkeyCount, wallet.getNextAccountIndex());
        Wallet wallet2 = new Wallet(config);
        // use different password and same mnemonic
        wallet2.unlock(pwd + pwd);
        wallet2.initializeHdWallet(SampleKeys.MNEMONIC);
        List<KeyPair> keyPairList2 = Lists.newArrayList();
        for (int i = 0; i < hdkeyCount; i++) {
            KeyPair key = wallet2.addAccountWithNextHdKey();
            keyPairList2.add(key);
        }
        assertTrue(CollectionUtils.isEqualCollection(keyPairList1, keyPairList2));
    }

    @After
    public void tearDown() throws IOException {
        wallet.delete();
    }
}