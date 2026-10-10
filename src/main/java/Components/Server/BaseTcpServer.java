package Components.Server;

import Components.Infra.Client;
import Components.Infra.ConnectionPool;
import Components.Service.CommandHandler;
import Components.Service.RespParser;
import Components.Service.RespSerializer;
import Components.Service.ResponseDto;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Accept loop and per-client read loop shared by the master and replica servers.
 * Every client gets its own virtual thread, so many blocked connections never starve each other.
 */
public abstract class BaseTcpServer {
    protected static final Logger logger = Logger.getLogger(BaseTcpServer.class.getName());

    protected final RespSerializer respSerializer;
    protected final CommandHandler commandHandler;
    protected final RedisConfig redisConfig;
    protected final ConnectionPool connectionPool;

    private final AtomicInteger nextClientId = new AtomicInteger();
    private volatile ServerSocket serverSocket;
    private volatile boolean running = true;

    protected BaseTcpServer(RespSerializer respSerializer,
                            CommandHandler commandHandler,
                            RedisConfig redisConfig,
                            ConnectionPool connectionPool) {
        this.respSerializer = respSerializer;
        this.commandHandler = commandHandler;
        this.redisConfig = redisConfig;
        this.connectionPool = connectionPool;
    }

    /** Starts serving and blocks until {@link #stop()} is called. */
    public abstract void startServer();

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            logger.log(Level.FINE, "Error closing server socket: {0}", e.getMessage());
        }
    }

    protected boolean isRunning() {
        return running;
    }

    protected void acceptLoop() {
        int port = redisConfig.getPort();
        try (ServerSocket server = new ServerSocket()) {
            // the option only has an effect if it is set before binding
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress(port));
            serverSocket = server;
            logger.log(Level.INFO, "Listening on port {0} as {1}", new Object[] {String.valueOf(port), redisConfig.getRole()});

            while (running) {
                Socket socket = server.accept();
                socket.setTcpNoDelay(true);
                Client client = new Client(socket, nextClientId.incrementAndGet());
                Thread.ofVirtual().name("client-" + client.getId()).start(() -> handleClient(client));
            }
        } catch (SocketException e) {
            if (running) {
                logger.log(Level.SEVERE, "Server socket failed: {0}", e.getMessage());
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Server failed: {0}", e.getMessage());
        }
    }

    private void handleClient(Client client) {
        connectionPool.addClient(client);
        try {
            RespParser parser = client.getParser();
            String[] command;
            // read() returns -1 (parser returns null) when the peer closes the connection
            while ((command = parser.readCommand()) != null) {
                ResponseDto response = commandHandler.execute(command, client);
                client.send(response);
            }
        } catch (RespParser.ProtocolException e) {
            logger.log(Level.FINE, "Protocol error from {0}: {1}", new Object[] {client.describe(), e.getMessage()});
            try {
                client.send(respSerializer.serializeError("ERR Protocol error: " + e.getMessage()));
            } catch (IOException ignored) {
                // connection is going away anyway
            }
        } catch (IOException e) {
            logger.log(Level.FINE, "Connection {0} closed: {1}", new Object[] {client.describe(), e.getMessage()});
        } catch (RuntimeException e) {
            logger.log(Level.SEVERE, "Unexpected error serving " + client.describe(), e);
        } finally {
            connectionPool.removeClient(client);
            connectionPool.removeSlave(client);
            client.close();
        }
    }
}
