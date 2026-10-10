package Components.Service;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Streaming RESP reader. It pulls exactly the bytes it needs from the stream, so
 * commands that arrive split across several TCP reads (or several commands in one
 * read) are handled correctly, and it keeps a running count of consumed bytes,
 * which a replica needs for its replication offset.
 *
 * Not thread-safe: one parser per connection, used by one thread.
 */
public class RespParser {

    /** The peer sent something that is not valid RESP. */
    public static class ProtocolException extends IOException {
        private static final long serialVersionUID = 1L;

        public ProtocolException(String message) {
            super(message);
        }
    }

    private static final int MAX_LINE_LENGTH = 64 * 1024;
    private static final int MAX_BULK_LENGTH = 512 * 1024 * 1024;
    private static final int MAX_ARRAY_LENGTH = 1024 * 1024;

    private final InputStream in;
    private long consumed = 0;

    public RespParser(InputStream in) {
        this.in = in;
    }

    /** Total number of bytes read from the stream so far. */
    public long bytesConsumed() {
        return consumed;
    }

    /**
     * Reads the next command (an array of bulk strings, or an inline command).
     *
     * @return the command, or null when the stream ended cleanly between commands
     * @throws EOFException      if the stream ended in the middle of a command
     * @throws ProtocolException if the data is not valid RESP
     */
    public String[] readCommand() throws IOException {
        while (true) {
            int first = next();
            if (first == -1) {
                return null;
            }
            if (first == '\r' || first == '\n') {
                continue; // stray blank line between commands
            }

            if (first != '*') {
                // inline command, e.g. typed over telnet: "PING\r\n"
                String line = (char) first + requireLine();
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                return trimmed.split("\\s+");
            }

            int count = parseLength(requireLine());
            if (count > MAX_ARRAY_LENGTH) {
                throw new ProtocolException("invalid multibulk length");
            }
            if (count <= 0) {
                continue; // empty array: nothing to execute
            }

            List<String> args = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int type = nextRequired();
                if (type != '$') {
                    throw new ProtocolException("expected '$', got '" + (char) type + "'");
                }
                args.add(new String(readBulkBytes(), StandardCharsets.UTF_8));
            }
            return args.toArray(new String[0]);
        }
    }

    /** Reads one line (without the trailing CRLF), e.g. "+PONG". Returns null on clean EOF. */
    public String readLine() throws IOException {
        int b = next();
        if (b == -1) {
            return null;
        }
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        while (b != '\n') {
            buf.write(b);
            if (buf.size() > MAX_LINE_LENGTH) {
                throw new ProtocolException("line too long");
            }
            b = nextRequired();
        }
        byte[] bytes = buf.toByteArray();
        int length = bytes.length;
        if (length > 0 && bytes[length - 1] == '\r') {
            length--;
        }
        return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }

    /**
     * Reads the RDB file a master sends after FULLRESYNC: "$&lt;length&gt;\r\n" followed by
     * the raw bytes. Unlike a bulk string there is no trailing CRLF.
     */
    public byte[] readRdbPayload() throws IOException {
        int type = nextRequired();
        if (type != '$') {
            throw new ProtocolException("expected RDB payload, got '" + (char) type + "'");
        }
        int length = parseLength(requireLine());
        if (length < 0 || length > MAX_BULK_LENGTH) {
            throw new ProtocolException("invalid RDB length");
        }
        return readExactly(length);
    }

    // ------------------------------------------------------------------ internals

    private byte[] readBulkBytes() throws IOException {
        int length = parseLength(requireLine());
        if (length < 0 || length > MAX_BULK_LENGTH) {
            throw new ProtocolException("invalid bulk length");
        }
        byte[] data = readExactly(length);
        if (nextRequired() != '\r' || nextRequired() != '\n') {
            throw new ProtocolException("bulk string not terminated by CRLF");
        }
        return data;
    }

    private byte[] readExactly(int length) throws IOException {
        byte[] data = new byte[length];
        int offset = 0;
        while (offset < length) {
            int n = in.read(data, offset, length - offset);
            if (n == -1) {
                throw new EOFException("stream ended inside a bulk string");
            }
            offset += n;
            consumed += n;
        }
        return data;
    }

    private String requireLine() throws IOException {
        String line = readLine();
        if (line == null) {
            throw new EOFException("stream ended inside a command");
        }
        return line;
    }

    private int parseLength(String s) throws ProtocolException {
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            throw new ProtocolException("invalid length '" + s + "'");
        }
    }

    private int next() throws IOException {
        int b = in.read();
        if (b != -1) {
            consumed++;
        }
        return b;
    }

    private int nextRequired() throws IOException {
        int b = next();
        if (b == -1) {
            throw new EOFException("stream ended inside a command");
        }
        return b;
    }
}
