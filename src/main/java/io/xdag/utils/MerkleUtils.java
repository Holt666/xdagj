package io.xdag.utils;

import io.xdag.core.v2.Transaction;
import io.xdag.core.v2.TransactionResult;
import io.xdag.crypto.Hash;

import java.util.ArrayList;
import java.util.List;

public final class MerkleUtils {

    public static byte[] computeTransactionsRoot(List<Transaction> txs) {
        List<byte[]> hashes = new ArrayList<>();
        for (Transaction tx : txs) {
            hashes.add(tx.getHash());
        }
        return new MerkleTree(hashes).getRootHash();
    }

    public static byte[] computeResultsRoot(List<TransactionResult> results) {
        List<byte[]> hashes = new ArrayList<>();
        for (TransactionResult tx : results) {
            hashes.add(Hash.h256(tx.toBytesForMerkle()));
        }
        return new MerkleTree(hashes).getRootHash();
    }

}
