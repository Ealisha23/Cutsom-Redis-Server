package Components.Service;

import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Encodes replies and decodes requests in the Redis serialization protocol (RESP). */
@Component
public class RespSerializer {
    private static final Logger logger = Logger.getLogger(RespSerializer.class.getName());

    public static final String NULL_BULK_STRING = "$-1\r\n";
    public static final String NULL_ARRAY = "*-1\r\n";
    public static final String EMPTY_ARRAY = "*0\r\n";

    /** Bulk string; a null argument produces the RESP null bulk string. Length is in bytes. */
    public String serializeBulkString(String s) {
        if (s == null) {
            return NULL_BULK_STRING;
        }
        int length = s.getBytes(StandardCharsets.UTF_8).length;
        return "$" + length + "\r\n" + s + "\r\n";
    }

    public String serializeSimpleString(String s) {
        return "+" + s + "\r\n";
    }

    public String serializeError(String message) {
        return "-" + message + "\r\n";
    }

    public String respInteger(long value) {
        return ":" + value + "\r\n";
    }

    /** Array of bulk strings, e.g. a command to send to a replica. */
    public String respArray(String[] elements) {
        StringBuilder sb = new StringBuilder("*").append(elements.length).append("\r\n");
        for (String element : elements) {
            sb.append(serializeBulkString(element));
        }
        return sb.toString();
    }

    /** Array whose elements are already RESP-encoded (used for EXEC results). */
    public String respArray(List<String> encodedElements) {
        StringBuilder sb = new StringBuilder("*").append(encodedElements.size()).append("\r\n");
        for (String element : encodedElements) {
            sb.append(element);
        }
        return sb.toString();
    }

    /** The exact bytes of a command as it is sent to a replica (also what counts toward offsets). */
    public byte[] encodeCommand(String[] command) {
        return respArray(command).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Decodes every complete command in the buffer. An incomplete or malformed tail is
     * ignored; for live connections use {@link RespParser} on the socket stream instead.
     */
    public List<String[]> deserialize(byte[] data) {
        List<String[]> commands = new ArrayList<>();
        RespParser parser = new RespParser(new ByteArrayInputStream(data));
        try {
            String[] command;
            while ((command = parser.readCommand()) != null) {
                commands.add(command);
            }
        } catch (IOException e) {
            logger.log(Level.FINE, "Stopped parsing: {0}", e.getMessage());
        }
        return commands;
    }
}
