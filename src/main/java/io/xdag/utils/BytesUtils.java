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

import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedLong;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes32;
import org.apache.tuweni.units.bigints.UInt64;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Utility class for byte array operations and conversions
 */
public class BytesUtils {

    /**
     * Empty byte array.
     */
    public static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * Empty address.
     */
    public static final byte[] EMPTY_ADDRESS = new byte[20];

    /**
     * Empty 256-bit hash.
     * <p>
     * Note: this is not the hash of empty byte array.
     */
    public static final byte[] EMPTY_HASH = new byte[32];

    /**
     * Converts an integer to a byte array
     * @param value The integer value to convert
     * @param littleEndian If true, uses little-endian byte order
     * @return Byte array representation of the integer
     */
    public static byte[] intToBytes(int value, boolean littleEndian) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        if (littleEndian) {
            buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        buffer.putInt(value);
        return buffer.array();
    }

    /**
     * Converts 4 bytes from a byte array to an integer
     * @param input Source byte array
     * @param offset Starting position in the array
     * @param littleEndian If true, uses little-endian byte order
     * @return The integer value
     */
    public static int bytesToInt(byte[] input, int offset, boolean littleEndian) {
        ByteBuffer buffer = ByteBuffer.wrap(input, offset, 4);
        if (littleEndian) {
            buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        return buffer.getInt();
    }

    /**
     * Converts a long to a byte array
     * @param value The long value to convert
     * @param littleEndian If true, uses little-endian byte order
     * @return Byte array representation of the long
     */
    public static byte[] longToBytes(long value, boolean littleEndian) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        if (littleEndian) {
            buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        buffer.putLong(value);
        return buffer.array();
    }

    /**
     * Converts 8 bytes from a byte array to a long
     * @param input Source byte array
     * @param offset Starting position in the array
     * @param littleEndian If true, uses little-endian byte order
     * @return The long value
     */
    public static long bytesToLong(byte[] input, int offset, boolean littleEndian) {
        ByteBuffer buffer = ByteBuffer.wrap(input, offset, 8);
        if (littleEndian) {
            buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        return buffer.getLong();
    }

    /**
     * Converts a short to a byte array
     * @param value The short value to convert
     * @param littleEndian If true, uses little-endian byte order
     * @return Byte array representation of the short
     */
    public static byte[] shortToBytes(short value, boolean littleEndian) {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        if (littleEndian) {
            buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        buffer.putShort(value);
        return buffer.array();
    }

    /**
     * Converts 2 bytes from a byte array to a short
     * @param input Source byte array
     * @param offset Starting position in the array
     * @param littleEndian If true, uses little-endian byte order
     * @return The short value
     */
    public static short bytesToShort(byte[] input, int offset, boolean littleEndian) {
        ByteBuffer buffer = ByteBuffer.wrap(input, offset, 2);
        if (littleEndian) {
            buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        return buffer.getShort();
    }

    /**
     * Converts a byte array to a hex string
     * @param data The byte array to convert
     * @return Lowercase hex string representation
     */
    public static String toHexString(byte[] data) {
        return data == null ? "" : BaseEncoding.base16().lowerCase().encode(data);
    }

    /**
     * Converts a BigInteger to a byte array of specified length
     * @param b The BigInteger to convert
     * @param numBytes Desired length of resulting byte array
     * @return Byte array representation of the BigInteger
     */
    public static byte[] bigIntegerToBytes(BigInteger b, int numBytes) {
        if (b == null) {
            return null;
        }
        byte[] bytes = new byte[numBytes];
        byte[] biBytes = b.toByteArray();
        int start = (biBytes.length == numBytes + 1) ? 1 : 0;
        int length = Math.min(biBytes.length, numBytes);
        System.arraycopy(biBytes, start, bytes, numBytes - length, length);
        return bytes;
    }

    /**
     * Converts a UInt64 to a byte array of specified length
     * @param b The UInt64 to convert
     * @param numBytes Desired length of resulting byte array
     * @return Byte array representation of the UInt64
     */
    public static byte[] bigIntegerToBytes(UInt64 b, int numBytes) {
        if (b == null) {
            return null;
        }
        byte[] bytes = new byte[numBytes];
        byte[] biBytes = b.toBytes().toArray();
        int start = (biBytes.length == numBytes + 1) ? 1 : 0;
        int length = Math.min(biBytes.length, numBytes);
        System.arraycopy(biBytes, start, bytes, numBytes - length, length);
        return bytes;
    }

    /**
     * Converts a BigInteger to a byte array with endianness control
     * @param b The BigInteger to convert
     * @param numBytes Desired length of resulting byte array
     * @param littleEndian If true, returns array in little-endian order
     * @return Byte array representation of the BigInteger
     */
    public static byte[] bigIntegerToBytes(BigInteger b, int numBytes, boolean littleEndian) {
        byte[] bytes = bigIntegerToBytes(b, numBytes);
        if (littleEndian) {
            arrayReverse(bytes);
        }
        return bytes;
    }

    /**
     * Merges multiple byte arrays into one
     * @param arrays Arrays to merge
     * @return Combined byte array
     */
    public static byte[] merge(byte[]... arrays) {
        int count = 0;
        for (byte[] array : arrays) {
            count += array.length;
        }

        byte[] mergedArray = new byte[count];
        int start = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, mergedArray, start, array.length);
            start += array.length;
        }
        return mergedArray;
    }

    /**
     * Merges a single byte with a byte array
     * @param b1 Single byte to prepend
     * @param b2 Byte array to append
     * @return Combined byte array
     */
    public static byte[] merge(byte b1, byte[] b2) {
        byte[] res = new byte[1 + b2.length];
        res[0] = b1;
        System.arraycopy(b2, 0, res, 1, b2.length);
        return res;
    }

    public static byte[] merge(byte b1, byte[] b2, byte[] b3) {
        byte[] res = new byte[1 + b2.length + b3.length];
        res[0] = b1;
        System.arraycopy(b2, 0, res, 1, b2.length);
        System.arraycopy(b3, 0, res, 1 + b2.length, b3.length);
        return res;
    }

    /**
     * Extracts a subarray from a byte array
     * @param arrays Source byte array
     * @param index Starting index
     * @param length Length of subarray
     * @return Extracted subarray
     */
    public static byte[] subArray(byte[] arrays, int index, int length) {
        byte[] arrayOfByte = new byte[length];
        int i = 0;
        while (true) {
            if (i >= length) {
                return arrayOfByte;
            }
            arrayOfByte[i] = arrays[(i + index)];
            i += 1;
        }
    }

    /**
     * Converts a hex string to a byte array
     * @param hexString Hex string to convert
     * @return Byte array representation
     */
    public static byte[] hexStringToBytes(String hexString) {
        if (hexString == null || hexString.isEmpty()) {
            return null;
        }
        hexString = hexString.toUpperCase();
        int length = hexString.length() / 2;
        char[] hexChars = hexString.toCharArray();
        byte[] d = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return d;
    }

    /**
     * Converts a hex character to its byte value
     */
    private static byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }

    /**
     * Reverses a byte array in place
     * @param origin Array to reverse
     */
    public static void arrayReverse(byte[] origin) {
        byte temp;
        for (int i = 0; i < origin.length / 2; i++) {
            temp = origin[i];
            origin[i] = origin[origin.length - i - 1];
            origin[origin.length - i - 1] = temp;
        }
    }

    /**
     * Creates a single-element byte array
     * @param b Byte value
     * @return Single-element byte array
     */
    public static byte[] of(byte b) {
        return new byte[]{b};
    }

    public static byte[] of(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    public static long toLong(byte[] bytes) {
        return ((bytes[0] & 0xffL) << 56)
                | ((bytes[1] & 0xffL) << 48)
                | ((bytes[2] & 0xffL) << 40)
                | ((bytes[3] & 0xffL) << 32)
                | ((bytes[4] & 0xffL) << 24)
                | ((bytes[5] & 0xffL) << 16)
                | ((bytes[6] & 0xffL) << 8)
                | (bytes[7] & 0xff);
    }

    public static int toInt(byte[] bytes) {
        return ((bytes[0] & 0xff) << 24)
                | ((bytes[1] & 0xff) << 16)
                | ((bytes[2] & 0xff) << 8)
                | (bytes[3] & 0xff);
    }

    public static byte[] of(long i) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) ((i >> 56) & 0xff);
        bytes[1] = (byte) ((i >> 48) & 0xff);
        bytes[2] = (byte) ((i >> 40) & 0xff);
        bytes[3] = (byte) ((i >> 32) & 0xff);
        bytes[4] = (byte) ((i >> 24) & 0xff);
        bytes[5] = (byte) ((i >> 16) & 0xff);
        bytes[6] = (byte) ((i >> 8) & 0xff);
        bytes[7] = (byte) (i & 0xff);
        return bytes;
    }

    /**
     * Gets first byte from a byte array
     * @param bytes Source array
     * @return First byte
     */
    public static byte toByte(byte[] bytes) {
        return bytes[0];
    }

    /**
     * Checks if a byte array starts with another byte array
     * @param key Full array
     * @param part Prefix to check
     * @return True if key starts with part
     */
    public static boolean keyStartsWith(byte[] key, byte[] part) {
        if (part.length > key.length) {
            return false;
        }
        for (int i = 0; i < part.length; i++) {
            if (key[i] != part[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if two byte arrays are equal
     * @param b1 First array
     * @param b2 Second array
     * @return True if arrays are equal
     */
    public static boolean equalBytes(byte[] b1, byte[] b2) {
        return b1.length == b2.length && compareTo(b1, 0, b1.length, b2, 0, b2.length) == 0;
    }

    /**
     * Lexicographically compares portions of two byte arrays
     * @param b1 First array
     * @param s1 Start index in first array
     * @param l1 Length in first array
     * @param b2 Second array
     * @param s2 Start index in second array
     * @param l2 Length in second array
     * @return Comparison result
     */
    public static int compareTo(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return LexicographicalComparerHolder.BEST_COMPARER.compareTo(b1, s1, l1, b2, s2, l2);
    }

    private static Comparer<byte[]> lexicographicalComparerJavaImpl() {
        return LexicographicalComparerHolder.PureJavaComparer.INSTANCE;
    }

    private interface Comparer<T> {
        int compareTo(T buffer1, int offset1, int length1, T buffer2, int offset2, int length2);
    }

    /**
     * Holder class for lexicographical comparison implementation
     */
    private static class LexicographicalComparerHolder {
        static final String UNSAFE_COMPARER_NAME = LexicographicalComparerHolder.class.getName() + "$UnsafeComparer";
        static final Comparer<byte[]> BEST_COMPARER = getBestComparer();

        static Comparer<byte[]> getBestComparer() {
            try {
                Class<?> theClass = Class.forName(UNSAFE_COMPARER_NAME);
                @SuppressWarnings("unchecked")
                Comparer<byte[]> comparer = (Comparer<byte[]>) theClass.getEnumConstants()[0];
                return comparer;
            } catch (Throwable t) {
                return lexicographicalComparerJavaImpl();
            }
        }

        private enum PureJavaComparer implements Comparer<byte[]> {
            INSTANCE;

            @Override
            public int compareTo(
                    byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {
                if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
                    return 0;
                }
                int end1 = offset1 + length1;
                int end2 = offset2 + length2;
                for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
                    int a = (buffer1[i] & 0xff);
                    int b = (buffer2[j] & 0xff);
                    if (a != b) {
                        return a - b;
                    }
                }
                return length1 - length2;
            }
        }
    }

    /**
     * Converts a byte array to MutableBytes32
     * @param value Source byte array
     * @return MutableBytes32 representation
     */
    public static MutableBytes32 arrayToByte32(byte[] value){
        MutableBytes32 mutableBytes32 = MutableBytes32.wrap(new byte[32]);
        mutableBytes32.set(8, Bytes.wrap(value));
        return mutableBytes32;
    }

    /**
     * Extracts a byte array from MutableBytes32
     * @param value Source MutableBytes32
     * @return Extracted byte array
     */
    public static byte[] byte32ToArray(MutableBytes32 value){
        return value.mutableCopy().slice(8,20).toArray();
    }

    /**
     * Converts a long to UnsignedLong
     * @param number Source long value
     * @return UnsignedLong representation
     */
    public static UnsignedLong long2UnsignedLong(long number) {
        return UnsignedLong.valueOf(toHexString((ByteBuffer.allocate(8).putLong(number).array())),16);
    }
}
