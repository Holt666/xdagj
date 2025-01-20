package io.xdag.core.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.xdag.Network;
import io.xdag.core.XAmount;
import io.xdag.utils.BytesUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Slf4j
@Getter
public class Genesis extends Block {

    @JsonDeserialize(keyUsing = ByteArray.ByteArrayKeyDeserializer.class)
    private final Map<ByteArray, Snapshot> snapshots;

    public Genesis(BlockHeader header, Map<ByteArray, Snapshot> snapshots) {
        super(header, Collections.emptyList(), Collections.emptyList());
        this.snapshots = snapshots;
    }

    @JsonCreator
    public static Genesis jsonCreator(
            @JsonProperty("number") long number,
            @JsonProperty("coinbase") String coinbase,
            @JsonProperty("preHash") String preHash,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("nbits") String nbits,
            @JsonProperty("nonce") String nonce,
            @JsonProperty("transactionsRoot") String transactionsRoot,
            @JsonProperty("resultsRoot") String resultsRoot,
            @JsonProperty("data") String data,
            @JsonProperty("snapshot") List<Snapshot> snapshotList) {

        // load premines
        Map<ByteArray, Snapshot> snapshotMap = new HashMap<>();
        for (Snapshot premine : snapshotList) {
            snapshotMap.put(new ByteArray(premine.getAddress()), premine);
        }

        byte[] coinbaseBytes = Hex.decode(coinbase);
        byte[] preHashBytes = Hex.decode(preHash);
        long timestampLong = timestamp;
        byte[] nbitsBytes = Hex.decode(nbits);
        byte[] nonceBytes = Hex.decode(nonce);
        List<byte[]> witnessBlockHashes = List.of(BytesUtils.EMPTY_HASH);
        byte[] transactionsRootBytes = Hex.decode(transactionsRoot);
        byte[] resultsRootBytes = Hex.decode(resultsRoot);
        byte[] dataBytes = Hex.decode(data);


        // load block header
        BlockHeader header = new BlockHeader(number, coinbaseBytes, preHashBytes, timestampLong, transactionsRootBytes, resultsRootBytes,
                BytesUtils.EMPTY_HASH, dataBytes, nbitsBytes, nonceBytes,
                witnessBlockHashes);

        return new Genesis(header, snapshotMap);
    }

    /**
     * Loads the genesis file.
     *
     * @return
     */
    public static Genesis load(Network network) {
        try {
            InputStream in = Genesis.class.getResourceAsStream("/genesis/" + network.label() + ".json");

            if (in != null) {
                return new ObjectMapper().readValue(in, Genesis.class);
            }
        } catch (IOException e) {
            log.error("Failed to load genesis file", e);
        }

        SystemUtils.exitAsync(SystemUtils.Code.FAILED_TO_LOAD_GENESIS);
        return null;
    }

    public static class Snapshot {
        private final byte[] address;
        private final XAmount amount;
        private final String note;

        public Snapshot(byte[] address, XAmount amount, String note) {
            this.address = address;
            this.amount = amount;
            this.note = note;
        }

        @JsonCreator
        public Snapshot(@JsonProperty("address") String address, @JsonProperty("amount") long amount,
                       @JsonProperty("note") String note) {
            this(Hex.decode(address), XAmount.of(amount), note);
        }

        public byte[] getAddress() {
            return address;
        }

        public XAmount getAmount() {
            return amount;
        }

        public String getNote() {
            return note;
        }
    }
}
