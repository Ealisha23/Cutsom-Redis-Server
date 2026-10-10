package Components.Service;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RespParserTest {

    private static RespParser parserFor(String text) {
        return new RespParser(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)));
    }

    /** Delivers one byte per read() call, like a very slow network. */
    private static class TrickleInputStream extends InputStream {
        private final byte[] data;
        private int pos = 0;

        TrickleInputStream(byte[] data) {
            this.data = data;
        }

        @Override
        public int read() {
            return pos < data.length ? (data[pos++] & 0xFF) : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (len == 0) {
                return 0;
            }
            int value = read();
            if (value == -1) {
                return -1;
            }
            b[off] = (byte) value;
            return 1;
        }
    }

    @Test
    void readsSimpleCommand() throws IOException {
        String[] command = parserFor("*1\r\n$4\r\nPING\r\n").readCommand();
        assertEquals(1, command.length);
        assertEquals("PING", command[0]);
    }

    @Test
    void readsCommandArrivingOneByteAtATime() throws IOException {
        byte[] bytes = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n".getBytes(StandardCharsets.UTF_8);
        String[] command = new RespParser(new TrickleInputStream(bytes)).readCommand();
        assertEquals(3, command.length);
        assertEquals("SET", command[0]);
        assertEquals("key", command[1]);
        assertEquals("value", command[2]);
    }

    @Test
    void readsPipelinedCommandsAndReturnsNullAtCleanEnd() throws IOException {
        RespParser parser = parserFor("*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n");
        assertEquals("PING", parser.readCommand()[0]);
        String[] second = parser.readCommand();
        assertEquals("ECHO", second[0]);
        assertEquals("hi", second[1]);
        assertNull(parser.readCommand());
    }

    @Test
    void countsBytesPerCommand() throws IOException {
        String ping = "*1\r\n$4\r\nPING\r\n";
        String echo = "*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n";
        RespParser parser = parserFor(ping + echo);

        parser.readCommand();
        assertEquals(ping.length(), parser.bytesConsumed());
        parser.readCommand();
        assertEquals(ping.length() + echo.length(), parser.bytesConsumed());
    }

    @Test
    void bulkLengthIsMeasuredInBytesNotCharacters() throws IOException {
        // "héllo" is 5 characters but 6 bytes in UTF-8
        String[] command = parserFor("*2\r\n$4\r\nECHO\r\n$6\r\nh\u00e9llo\r\n").readCommand();
        assertEquals("h\u00e9llo", command[1]);
    }

    @Test
    void valuesMayContainCrlfAndNulBytes() throws IOException {
        String[] command = parserFor("*2\r\n$4\r\nECHO\r\n$6\r\na\r\nb\u0000c\r\n").readCommand();
        assertEquals("a\r\nb\u0000c", command[1]);
    }

    @Test
    void readsInlineCommand() throws IOException {
        String[] command = parserFor("SET  a   b\r\n").readCommand();
        assertEquals(3, command.length);
        assertEquals("b", command[2]);
    }

    @Test
    void readLineStripsCrlf() throws IOException {
        RespParser parser = parserFor("+PONG\r\n+OK\r\n");
        assertEquals("+PONG", parser.readLine());
        assertEquals("+OK", parser.readLine());
        assertNull(parser.readLine());
    }

    @Test
    void rdbPayloadHasNoTrailingCrlfSoTheNextCommandIsNotEaten() throws IOException {
        RespParser parser = parserFor("$5\r\nREDIS*1\r\n$4\r\nPING\r\n");
        byte[] rdb = parser.readRdbPayload();
        assertEquals("REDIS", new String(rdb, StandardCharsets.UTF_8));
        assertEquals("PING", parser.readCommand()[0]);
    }

    @Test
    void endOfStreamInsideACommandIsAnError() {
        assertThrows(EOFException.class, () -> parserFor("*2\r\n$4\r\nECHO\r\n$5\r\nhe").readCommand());
    }

    @Test
    void garbageInsideAnArrayIsAProtocolError() {
        assertThrows(RespParser.ProtocolException.class, () -> parserFor("*1\r\nPING\r\n").readCommand());
        assertThrows(RespParser.ProtocolException.class, () -> parserFor("*1\r\n$abc\r\n").readCommand());
        assertThrows(RespParser.ProtocolException.class, () -> parserFor("*1\r\n$-5\r\n").readCommand());
    }
}
