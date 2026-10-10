package Components.Server;

import Components.Infra.Client;
import Components.Infra.ConnectionPool;
import Components.Service.CommandHandler;
import Components.Service.RespParser;
import Components.Service.RespSerializer;
import Components.Service.ResponseDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.Socket;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A replica: serves clients (read-only) and keeps a connection to its master, performing the
 * replication handshake and then applying the write stream the master sends.
 */
@Component
public class SlaveTcpServer extends BaseTcpServer {
    private static final Logger log = Logger.getLogger(SlaveTcpServer.class.getName());
    private static final long MAX_RETRY_DELAY_MILLIS = 5000;

    private volatile Socket masterSocket;

    @Autowired
    public SlaveTcpServer(RespSerializer respSerializer,
                          CommandHandler commandHandler,
                          RedisConfig redisConfig,
                          ConnectionPool connectionPool) {
        super(respSerializer, commandHandler, redisConfig, connectionPool);
    }

    @Override
    public void startServer() {
        Thread.ofVirtual().name("replication").start(this::replicationLoop);
        acceptLoop();
    }

    @Override
    public void stop() {
        super.stop();
        try {
            Socket socket = masterSocket;
            if (socket != null) {
                socket.close(); // unblocks the replication thread
            }
        } catch (IOException e) {
            log.log(Level.FINE, "Error closing master link: {0}", e.getMessage());
        }
    }

    /** Connects to the master and re-connects with a growing delay if the link drops. */
    private void replicationLoop() {
        long delay = 500;
        while (isRunning()) {
            try {
                replicateFromMaster();
                delay = 500; // the link worked, so start the back-off over
            } catch (IOException | RuntimeException e) {
                log.log(Level.WARNING, "Replication link failed: {0}", e.getMessage());
            }
            if (!isRunning()) {
                return;
            }
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            delay = Math.min(delay * 2, MAX_RETRY_DELAY_MILLIS);
        }
    }

    private void replicateFromMaster() throws IOException {
        try (Socket socket = new Socket(redisConfig.getMasterHost(), redisConfig.getMasterPort())) {
            masterSocket = socket;
            socket.setTcpNoDelay(true);
            Client master = new Client(socket, -1);
            master.setMasterLink(true);
            RespParser parser = master.getParser();

            handshake(master, parser);
            log.log(Level.INFO, "Replication handshake complete, applying stream from {0}:{1}",
                    new Object[] {redisConfig.getMasterHost(), String.valueOf(redisConfig.getMasterPort())});

            // Apply the master's write stream. The offset advances by exactly the number of
            // bytes each command took on the wire, counted by the parser.
            while (isRunning()) {
                long before = parser.bytesConsumed();
                String[] command = parser.readCommand();
                if (command == null) {
                    log.log(Level.WARNING, "Master closed the connection");
                    return;
                }
                long size = parser.bytesConsumed() - before;

                ResponseDto response = commandHandler.execute(command, master);

                // The master only expects an answer to REPLCONF GETACK, never to writes.
                if (isGetAck(command) && response != null) {
                    master.send(response);
                }
                redisConfig.addToMasterReplOffset(size);
            }
        }
    }

    private void handshake(Client master, RespParser parser) throws IOException {
        // 1. PING
        master.send(respSerializer.encodeCommand(new String[] {"PING"}));
        expectSimpleReply(parser, "PING");

        // 2. REPLCONF listening-port / capa
        master.send(respSerializer.encodeCommand(
                new String[] {"REPLCONF", "listening-port", String.valueOf(redisConfig.getPort())}));
        expectSimpleReply(parser, "REPLCONF listening-port");
        master.send(respSerializer.encodeCommand(new String[] {"REPLCONF", "capa", "psync2"}));
        expectSimpleReply(parser, "REPLCONF capa");

        // 3. PSYNC ? -1, answered with +FULLRESYNC <replid> <offset> and then the RDB file
        master.send(respSerializer.encodeCommand(new String[] {"PSYNC", "?", "-1"}));
        String reply = expectSimpleReply(parser, "PSYNC");
        String[] parts = reply.split(" ");
        if (!parts[0].equalsIgnoreCase("+FULLRESYNC") || parts.length < 3) {
            throw new IOException("Unexpected PSYNC reply: " + reply);
        }
        redisConfig.setMasterReplId(parts[1]);
        redisConfig.setMasterReplOffset(Long.parseLong(parts[2]));

        parser.readRdbPayload(); // an empty dataset; nothing to load
    }

    private String expectSimpleReply(RespParser parser, String step) throws IOException {
        String line = parser.readLine();
        if (line == null) {
            throw new IOException("Master closed the connection during " + step);
        }
        if (!line.startsWith("+")) {
            throw new IOException("Master rejected " + step + ": " + line);
        }
        return line;
    }

    private boolean isGetAck(String[] command) {
        return command.length >= 2
                && command[0].equalsIgnoreCase("REPLCONF")
                && command[1].toUpperCase(Locale.ROOT).equals("GETACK");
    }
}
