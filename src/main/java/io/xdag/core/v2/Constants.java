package io.xdag.core.v2;

import io.xdag.crypto.Keys;
import io.xdag.crypto.Sign;
import io.xdag.utils.WalletUtils;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.crypto.KeyPair;

import java.security.spec.InvalidKeySpecException;

public final class Constants {

    /**
     * The public-private key pair for signing coinbase transactions.
     */
    public static final KeyPair COINBASE_KEY;

    /**
     * Address bytes of {@link this#COINBASE_KEY}. This is stored as a cache to
     * avoid redundant h160 calls.
     */
    public static final byte[] COINBASE_ADDRESS;

    /**
     * The public-private key pair of the genesis validator.
     */
    public static final KeyPair DEVNET_KEY;

    static {
        Bytes32 b = Bytes32.fromHexString("302e020100300506032b657004220420acdd12174cbc3fa6e4076cb1e270989cf4d47b0de8942c8542fe6a3bed34d7bf");
        COINBASE_KEY = Sign.SECP256K1.createKeyPair(Sign.SECP256K1.createPrivateKey(b));

        COINBASE_ADDRESS = Keys.toBytesAddress(COINBASE_KEY);

        Bytes32 b2 = Bytes32.fromHexString("302e020100300506032b657004220420acbd5f2cb2b6053f704376d12df99f2aa163d267a755c7f1d9fe55d2a2dc5405");
        DEVNET_KEY = Sign.SECP256K1.createKeyPair(Sign.SECP256K1.createPrivateKey(b2));
    }
}
