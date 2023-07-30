package io.xdag.defi;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.google.common.collect.Maps;

public class XRC20Protocol {
    static final Map<Integer, String> FIELD_NAMES = Maps.newHashMap();
    static {
        FIELD_NAMES.put(0, "Protocol");
        FIELD_NAMES.put(1, "TokenName");
        FIELD_NAMES.put(2, "Operation");
        FIELD_NAMES.put(3, "Max");
        FIELD_NAMES.put(4, "Lim");
        FIELD_NAMES.put(5, "Dec");
        FIELD_NAMES.put(6, "Permissions");
    }

    public static byte[] encodeXRC20(int protocol, String tokenName, int operation, long max, long lim, int dec, int permissions) {
        ByteBuffer remark = ByteBuffer.allocate(32);
        // 0 - Protocol(1 byte)
        remark.put((byte)protocol);

        // 1 - Token Name
        byte[] tmCharArray = tokenName.getBytes(StandardCharsets.US_ASCII);
        for(int i = 0; i < NumberUtils.min(5, tmCharArray.length); i++) {
            remark.put(tmCharArray[i]);
        }

        if(tmCharArray.length < 5) {
            for(int i = 0; i < (5 - tmCharArray.length); i++) {
                remark.put((byte)0);
            }
        }

        // 2 - Operation
        remark.put((byte)operation);

        // 3 - Max
        remark.putLong(max);

        // 4 - Lim
        remark.putLong(lim);

        // 5 - Dec
        remark.put((byte)dec);

        // 6 - Permissions
        remark.put((byte)permissions);

        return remark.array();
    }

    public static Map<String, String> decodeXRC20(byte[] remark) {
        Map<String, String> result = Maps.newHashMap();
        ByteBuffer buffer = ByteBuffer.wrap(remark);

        // 0 - Protocol (1 byte)
        byte protocol = buffer.get();
        if(protocol != (byte)0 && protocol != (byte)1) {
            return null;
        }

        result.put(FIELD_NAMES.get(0), String.valueOf(protocol));

        // 1 - Token Name (5 bytes)
        byte[] tn = new byte[5];
        buffer.get(tn);
        StringBuilder sbd = new StringBuilder();
        for (byte b : tn) {
            char c = (char) b;
            if (CharUtils.isAsciiPrintable(c)) {
                sbd.append(c);
            }
        }
        if(!StringUtils.isAsciiPrintable(sbd.toString())) {
            return null;
        }
        result.put(FIELD_NAMES.get(1), sbd.toString());

        // 2 - Operation (1 byte)
        byte operation = buffer.get();
        if(operation != (byte)1 && operation != (byte)2) {
            return null;
        }
        result.put(FIELD_NAMES.get(2), String.valueOf(operation));

        // 3 - Max (8 bytes)
        long max =  buffer.getLong();
        result.put(FIELD_NAMES.get(3), String.valueOf(max));


        // 4 - Lim (8 bytes)
        long lim = buffer.getLong();
        result.put(FIELD_NAMES.get(4), String.valueOf(lim));

        // 5 - Dec (1 byte)
        int dec = buffer.get();
        result.put(FIELD_NAMES.get(5), String.valueOf(dec));

        // 6 - permissions (1 byte)
        int prem  = Byte.toUnsignedInt(buffer.get());
        if(prem != 0 && prem != 1) {
            return null;
        }
        result.put(FIELD_NAMES.get(6), String.valueOf(prem));
        return result;
    }
}