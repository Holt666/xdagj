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

package io.xdag.utils;

import io.xdag.Wallet;
import io.xdag.crypto.Base58;
import io.xdag.crypto.Bip32ECKeyPair;
import io.xdag.crypto.Keys;
import io.xdag.utils.exception.AddressFormatException;
import lombok.extern.slf4j.Slf4j;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.crypto.KeyPair;

import static io.xdag.crypto.Bip32ECKeyPair.HARDENED_BIT;

/**
 * Utility class for wallet operations including BIP44 key generation, address encoding/decoding and validation
 */
@Slf4j
public class WalletUtils {

    /**
     * XDAG coin type according to SLIP-0044
     * @see <a href="https://github.com/satoshilabs/slips/blob/master/slip-0044.md">SLIP-0044</a>
     */
    public static final int XDAG_BIP44_CION_TYPE = 586;

    /**
     * Prompt message for wallet password input
     */
    public static final String WALLET_PASSWORD_PROMPT = "Please Enter Wallet Password: ";

    /**
     * Generates a BIP44 compliant key pair for XDAG
     * Path: m/44'/586'/0'/0/index
     *
     * @param master Master key pair
     * @param index Account index
     * @return Derived key pair
     */
    public static Bip32ECKeyPair generateBip44KeyPair(Bip32ECKeyPair master, int index) {
        final int[] path = {44 | HARDENED_BIT, XDAG_BIP44_CION_TYPE | HARDENED_BIT, 0 | HARDENED_BIT, 0, index};
        return Bip32ECKeyPair.deriveKeyPair(master, path);
    }

    /**
     * Imports a wallet from mnemonic phrase
     *
     * @param wallet Wallet instance
     * @param password Wallet password
     * @param mnemonic Mnemonic phrase
     * @param index Account index
     * @return Generated key pair
     */
    public static Bip32ECKeyPair importMnemonic(Wallet wallet, String password, String mnemonic, int index) {
        wallet.unlock(password);
        byte[] seed = MnemonicUtils.generateSeed(mnemonic, password);
        Bip32ECKeyPair masterKeypair = Bip32ECKeyPair.generateKeyPair(seed);
        return generateBip44KeyPair(masterKeypair, index);
    }

    /**
     * Encodes a hash160 byte array to Base58 format
     *
     * @param hash160 Hash160 byte array
     * @return Base58 encoded string
     */
    public static String toBase58(byte[] hash160) {
        return Base58.encodeChecked(hash160);
    }

    public static String toBase58(KeyPair keyPair) {
        return toBase58(Keys.toBytesAddress(keyPair));
    }

    /**
     * Decodes a Base58 string to byte array
     *
     * @param base58 Base58 encoded string
     * @return Decoded byte array
     * @throws AddressFormatException if the input is invalid
     */
    public static byte[] fromBase58(String base58) throws AddressFormatException {
        byte[] bytes = Base58.decodeChecked(base58);
        if (bytes.length != 20)
            throw new AddressFormatException.InvalidDataLength("Wrong number of bytes: " + bytes.length);
        return bytes;
    }

    /**
     * Validates a Base58 encoded address
     *
     * @param base58 Base58 encoded address
     * @return true if valid, false otherwise
     */
    public static boolean checkAddress(String base58) {
        return Base58.checkAddress(base58);
    }

    /**
     * Validates an address using its hash
     *
     * @param hashlow Lower 32 bytes of address hash
     * @return true if valid, false otherwise
     */
    public static boolean checkAddress(Bytes32 hashlow) {
        return hashlow.slice(28, 4).toInt() == 0;
    }
}