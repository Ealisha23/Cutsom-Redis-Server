package Components.Service;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RespSerializerTest {
    private final RespSerializer respSerializer = new RespSerializer();

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void deserializesPing() {
        List<String[]> commands = respSerializer.deserialize(bytes("*1\r\n$4\r\nPING\r\n"));
        assertEquals(1, commands.size());
        assertEquals(1, commands.get(0).length);
        assertEquals("PING", commands.get(0)[0]);
    }

    @Test
    void deserializesPipelinedCommands() {
        String twoSets = "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
                + "*3\r\n$3\r\nset\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n";
        List<String[]> commands = respSerializer.deserialize(bytes(twoSets));

        assertEquals(2, commands.size());
        assertEquals("key", commands.get(0)[1]);
        assertEquals("value", commands.get(0)[2]);
        assertEquals("key2", commands.get(1)[1]);
        assertEquals("value2", commands.get(1)[2]);
    }

    @Test
    void ignoresAnIncompleteTrailingCommand() {
        List<String[]> commands = respSerializer.deserialize(bytes("*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$3\r\nhe"));
        assertEquals(1, commands.size());
    }

    @Test
    void emptyInputGivesNoCommands() {
        assertEquals(0, respSerializer.deserialize(new byte[0]).size());
    }

    @Test
    void bulkStringLengthCountsBytes() {
        assertEquals("$9\r\ntestValue\r\n", respSerializer.serializeBulkString("testValue"));
        assertEquals("$6\r\nh\u00e9llo\r\n", respSerializer.serializeBulkString("h\u00e9llo"));
        assertEquals("$0\r\n\r\n", respSerializer.serializeBulkString(""));
    }

    @Test
    void nullBulkString() {
        assertEquals("$-1\r\n", respSerializer.serializeBulkString(null));
    }

    @Test
    void simpleStringErrorAndInteger() {
        assertEquals("+OK\r\n", respSerializer.serializeSimpleString("OK"));
        assertEquals("-ERR boom\r\n", respSerializer.serializeError("ERR boom"));
        assertEquals(":42\r\n", respSerializer.respInteger(42));
        assertEquals(":-7\r\n", respSerializer.respInteger(-7));
    }

    @Test
    void arrayOfBulkStrings() {
        assertEquals("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n",
                respSerializer.respArray(new String[] {"REPLCONF", "GETACK", "*"}));
    }

    @Test
    void arrayOfEncodedElements() {
        assertEquals("*2\r\n+OK\r\n:1\r\n", respSerializer.respArray(List.of("+OK\r\n", ":1\r\n")));
        assertEquals("*0\r\n", respSerializer.respArray(List.of()));
    }

    @Test
    void encodedCommandRoundTrips() {
        String[] original = {"SET", "k\u00e9y", "va lue"};
        List<String[]> decoded = respSerializer.deserialize(respSerializer.encodeCommand(original));
        assertEquals(1, decoded.size());
        assertEquals(original[1], decoded.get(0)[1]);
        assertEquals(original[2], decoded.get(0)[2]);
    }
}
