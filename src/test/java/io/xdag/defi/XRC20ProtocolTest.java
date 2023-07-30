package io.xdag.defi;

import static io.xdag.defi.XRC20Protocol.encodeXRC20;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Map;

import org.junit.Test;

public class XRC20ProtocolTest {
    @Test
    public void testParseNormal() {
        byte[] remark = encodeXRC20(0, "btc", 1, 21000000, 10000, 8, 0);
        Map<String, String> result = XRC20Protocol.decodeXRC20(remark);
        assertNotNull(result);
        assertEquals("0", result.get("Protocol"));
        assertEquals("btc", result.get("TokenName"));
        assertEquals("1", result.get("Operation"));
        assertEquals("21000000", result.get("Max"));
        assertEquals("10000", result.get("Lim"));
        assertEquals("8", result.get("Dec"));
        assertEquals("0", result.get("Permissions"));
    }

    @Test
    public void testParseWith5LengthTokenName() {
        byte[] remark = encodeXRC20(0, "abcde", 1, 21000000, 10000, 8, 0);
        Map<String, String> result = XRC20Protocol.decodeXRC20(remark);
        assertNotNull(result);
        assertEquals("abcde", result.get("TokenName"));
    }

    @Test
    public void testParseWithMoreThen5LengthTokenName() {
        byte[] remark = encodeXRC20(0, "testtoken", 2, 21000000, 10000, 8, 0);
        Map<String, String> result = XRC20Protocol.decodeXRC20(remark);
        assertNotNull(result);
        assertEquals("testt", result.get("TokenName"));
    }

    @Test
    public void testParseInvalidProtocol() {
        byte[] remark = encodeXRC20(3, "btc", 3, 21000000, 10000, 8, 0);
        Map<String, String> result = XRC20Protocol.decodeXRC20(remark);
        assertNull(result);
    }

    @Test
    public void testParseInvalidOperation() {
        byte[] remark = encodeXRC20(0, "btc", 3, 21000000, 10000, 8, 0);
        Map<String, String> result = XRC20Protocol.decodeXRC20(remark);
        assertNull(result);
    }

}
