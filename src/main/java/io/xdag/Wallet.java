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

package io.xdag;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.tuweni.bytes.Bytes;
import org.bouncycastle.crypto.generators.BCrypt;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SecureRandomProvider;

import com.google.common.collect.Lists;

import io.xdag.config.Config;
import io.xdag.crypto.Aes;
import io.xdag.crypto.Bip32ECKeyPair;
import io.xdag.crypto.Keys;
import io.xdag.crypto.Sign;
import io.xdag.utils.MnemonicUtils;
import io.xdag.utils.Numeric;
import io.xdag.utils.SimpleDecoder;
import io.xdag.utils.SimpleEncoder;
import io.xdag.utils.WalletUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
public class Wallet {

    public static final Set<PosixFilePermission> POSIX_SECURED_PERMISSIONS = Set.of(OWNER_READ, OWNER_WRITE);
    private static final int VERSION = 4;
    private static final int SALT_LENGTH = 16;
    private static final int BCRYPT_COST = 12;
    private static final String MNEMONIC_PASS_PHRASE = "";
    /**
     * -- GETTER --
     *  Returns the file where the wallet is persisted.
     */
    @Getter
    private final File file;
    private final Config config;

    private final Map<Bytes, KeyPair> accounts = Collections.synchronizedMap(new LinkedHashMap<>());
    private String password;

    // hd wallet key
    private String mnemonicPhrase = "";
    private int nextAccountIndex = 0;

    static {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    /**
     * Creates a new wallet instance.
     */
    public Wallet(Config config) {
        this.file = FileUtils.getFile(config.walletDir());
        this.config = config;
    }

    /**
     * Returns whether the wallet file exists and non-empty.
     */
    public boolean exists() {
        return file.length() > 0;
    }

    /**
     * Deletes the wallet file.
     */
    public void delete() throws IOException {
        Files.delete(file.toPath());
    }

    /**
     * Locks the wallet.
     */
    public void lock() {
        password = null;
        accounts.clear();
    }

    public KeyPair getDefKey() {
        List<KeyPair> accountList = getAccounts();
        if (CollectionUtils.isNotEmpty(accountList)) {
            return accountList.get(0);
        }
        return null;
    }

    /**
     * Unlocks this wallet
     */
    public boolean unlock(String password) {
        if (password == null) {
            throw new IllegalArgumentException("Password can not be null");
        }

        try {
            byte[] key;
            byte[] salt;

            if (exists()) {

                SimpleDecoder dec = new SimpleDecoder(FileUtils.readFileToByteArray(file));
                int version = dec.readInt(); // version

                Set<KeyPair> newAccounts;
                switch (version) {
                    // only version 4
                    case 4 -> {
                        salt = dec.readBytes();
                        key = BCrypt.generate(password.getBytes(UTF_8), salt, BCRYPT_COST);
                        try {
                            newAccounts = readAccounts(key, dec, true, version);
                            readHdSeed(key, dec);
                        } catch (Exception e) {
                            log.warn("Failed to read HD mnemonic phrase");
                            return false;
                        }
                    }
                    default -> throw new RuntimeException("Unknown wallet version.");
                }

                synchronized (accounts) {
                    accounts.clear();
                    for (KeyPair account : newAccounts) {
                        Bytes b = Bytes.wrap(Keys.toBytesAddress(account));
                        accounts.put(b, account);
                    }
                }
            }
            this.password = password;
            return true;
        } catch (Exception e) {
            log.error("Failed to open wallet", e);
        }
        return false;
    }

    /**
     * Reads the account keys.
     */
    protected LinkedHashSet<KeyPair> readAccounts(byte[] key, SimpleDecoder dec, boolean vlq, int version) {
        LinkedHashSet<KeyPair> keys = new LinkedHashSet<>();
        int total = dec.readInt(); // size

        for (int i = 0; i < total; i++) {
            byte[] iv = dec.readBytes(vlq);
            byte[] privateKey = Aes.decrypt(dec.readBytes(vlq), key, iv);
            KeyPair keyPair = KeyPair.create(SECPPrivateKey.create(Numeric.toBigInt(privateKey), Sign.CURVE_NAME), Sign.CURVE, Sign.CURVE_NAME);
            keys.add(keyPair);
        }
        return keys;
    }

    /**
     * Writes the account keys.
     */
    protected void writeAccounts(byte[] key, SimpleEncoder enc) {
        synchronized (accounts) {
            enc.writeInt(accounts.size());
            for (KeyPair keyPair : accounts.values()) {
                byte[] iv = SecureRandomProvider.publicSecureRandom().generateSeed(16);

                enc.writeBytes(iv);
                enc.writeBytes(Aes.encrypt(keyPair.getPrivateKey().getEncoded(), key, iv));
            }
        }
    }

    /**
     * Reads the mnemonic phase and next account index.
     */
    protected void readHdSeed(byte[] key, SimpleDecoder dec) {
        byte[] iv = dec.readBytes();
        byte[] hdSeedEncrypted = dec.readBytes();
        byte[] hdSeedRaw = Aes.decrypt(hdSeedEncrypted, key, iv);

        SimpleDecoder d = new SimpleDecoder(hdSeedRaw);
        mnemonicPhrase = d.readString();
        nextAccountIndex = d.readInt();
    }

    /**
     * Writes the mnemonic phase and next account index.
     */
    protected void writeHdSeed(byte[] key, SimpleEncoder enc) {
        SimpleEncoder e = new SimpleEncoder();
        e.writeString(mnemonicPhrase);
        e.writeInt(nextAccountIndex);

        byte[] iv = SecureRandomProvider.publicSecureRandom().generateSeed(16);
        byte[] hdSeedRaw = e.toBytes();
        byte[] hdSeedEncrypted = Aes.encrypt(hdSeedRaw, key, iv);

        enc.writeBytes(iv);
        enc.writeBytes(hdSeedEncrypted);
    }

    /**
     * Returns if this wallet is unlocked.
     */
    public boolean isUnlocked() {
        return !isLocked();
    }

    /**
     * Returns whether the wallet is locked.
     */
    public boolean isLocked() {
        return password == null;
    }

    /**
     * Returns a copy of the accounts inside this wallet.
     */
    public List<KeyPair> getAccounts() {
        requireUnlocked();
        synchronized (accounts) {
            return Lists.newArrayList(accounts.values());
        }
    }

    /**
     * Sets the accounts inside this wallet.
     */
    public void setAccounts(List<KeyPair> list) {
        requireUnlocked();
        accounts.clear();
        for (KeyPair key : list) {
            addAccount(key);
        }
    }

    /**
     * Returns account by index.
     */
    public KeyPair getAccount(int idx) {
        requireUnlocked();
        synchronized (accounts) {
            return getAccounts().get(idx);
        }
    }

    /**
     * Returns account by address.
     */
    public KeyPair getAccount(byte[] address) {
        requireUnlocked();

        synchronized (accounts) {
            return accounts.get(Bytes.of(address));
        }
    }

    /**
     * Flushes this wallet into the disk.
     */
    public boolean flush() {
        requireUnlocked();

        try {
            SimpleEncoder enc = new SimpleEncoder();
            enc.writeInt(VERSION);

            byte[] salt = SecureRandomProvider.publicSecureRandom().generateSeed(SALT_LENGTH);
            enc.writeBytes(salt);

            byte[] key = BCrypt.generate(password.getBytes(UTF_8), salt, BCRYPT_COST);

            writeAccounts(key, enc);
            writeHdSeed(key, enc);

            if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
                log.error("Failed to create the directory for wallet");
                return false;
            }

            // set posix permissions
            if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix") && !file.exists()) {
                Files.createFile(file.toPath());
                Files.setPosixFilePermissions(file.toPath(), POSIX_SECURED_PERMISSIONS);
            }

            FileUtils.writeByteArrayToFile(file, enc.toBytes());
            return true;
        } catch (IOException e) {
            log.error("Failed to write wallet to disk", e);
        }
        return false;
    }


    private void requireUnlocked() {
        if (!isUnlocked()) {
            throw new RuntimeException("Wallet is Locked!");
        }
    }

    /**
     * Adds a new account to the wallet.
     */
    public boolean addAccount(KeyPair newKey) {
        requireUnlocked();

        synchronized (accounts) {
            Bytes b = Bytes.wrap(Keys.toBytesAddress(newKey));
            if (accounts.containsKey(b)) {
                return false;
            }

            accounts.put(b, newKey);
            return true;
        }
    }

    /**
     * Add an account with randomly generated key.
     */
    public KeyPair addAccountRandom()
            throws InvalidAlgorithmParameterException, NoSuchAlgorithmException, NoSuchProviderException {
        KeyPair key = Keys.createEcKeyPair();
        addAccount(key);
        return key;
    }

    /**
     * Adds a list of accounts to the wallet.
     */
    public int addAccounts(List<KeyPair> accounts) {
        requireUnlocked();

        int n = 0;
        for (KeyPair acc : accounts) {
            n += addAccount(acc) ? 1 : 0;
        }
        return n;
    }

    /**
     * Deletes an account in the wallet.
     */
    public boolean removeAccount(KeyPair key) {
        return removeAccount(Keys.toBytesAddress(key));
    }

    /**
     * Deletes an account in the wallet.
     */
    public boolean removeAccount(byte[] address) {
        requireUnlocked();
        synchronized (accounts) {
            return accounts.remove(Bytes.of(address)) != null;
        }
    }

    /**
     * Changes the password of the wallet.
     */
    public void changePassword(String newPassword) {
        requireUnlocked();

        if (newPassword == null) {
            throw new IllegalArgumentException("Password can not be null");
        }

        this.password = newPassword;
    }

    // ================
    // HD wallet
    // ================

    /**
     * Returns whether the HD seed is initialized.
     *
     * @return true if set, otherwise false
     */
    public boolean isHdWalletInitialized() {
        requireUnlocked();
        return mnemonicPhrase != null && !mnemonicPhrase.isEmpty();
    }

    /**
     * Initialize the HD wallet.
     *
     * @param mnemonicPhrase the mnemonic word list
     */
    public void initializeHdWallet(String mnemonicPhrase) {
        this.mnemonicPhrase = mnemonicPhrase;
        this.nextAccountIndex = 0;
    }

    /**
     * Returns the HD seed.
     */
    public byte[] getSeed() {
        return MnemonicUtils.generateSeed(this.mnemonicPhrase, MNEMONIC_PASS_PHRASE);
    }

    /**
     * Derives a key based on the current HD account index, and put it into the
     * wallet.
     */
    public KeyPair addAccountWithNextHdKey() {
        requireUnlocked();
        requireHdWalletInitialized();

        synchronized (accounts) {
            byte[] seed = getSeed();
            Bip32ECKeyPair masterKeypair = Bip32ECKeyPair.generateKeyPair(seed);
            Bip32ECKeyPair bip44Keypair = WalletUtils.generateBip44KeyPair(masterKeypair, nextAccountIndex++);
            Bytes b = Bytes.wrap(Keys.toBytesAddress(bip44Keypair.getKeyPair()));
            accounts.put(b, bip44Keypair.getKeyPair());
            return bip44Keypair.getKeyPair();
        }
    }

    private void requireHdWalletInitialized() {
        if (!isHdWalletInitialized()) {
            throw new IllegalArgumentException("HD Seed is not initialized");
        }
    }

}
