package Components.Infra;

import Components.Service.RespParser;
import Components.Service.ResponseDto;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One connection: the socket, its parser, and per-connection state (MULTI queue and
 * what the peer said during the replication handshake).
 *
 * Writes are serialized by a lock because a client can be written to by its own thread
 * (replies) and by other threads (propagated commands, GETACK) at the same time.
 */
public class Client {
    private final Socket socket; // may be null in unit tests
    private final OutputStream outputStream;
    private final RespParser parser;
    private final int id;
    private final ReentrantLock writeLock = new ReentrantLock();

    // connection to our master (we are the replica); commands on it are not "from a user"
    private volatile boolean masterLink = false;

    // sent by a replica during the handshake, kept until PSYNC turns the client into a Slave
    private volatile int listeningPort = -1;
    private final List<String> capabilities = new CopyOnWriteArrayList<>();

    // MULTI/EXEC state; only touched by the thread serving this client
    private boolean inTransaction = false;
    private List<String[]> queuedCommands = new ArrayList<>();

    public Client(Socket socket, int id) throws IOException {
        this(socket, socket.getInputStream(), socket.getOutputStream(), id);
    }

    public Client(Socket socket, InputStream inputStream, OutputStream outputStream, int id) {
        this.socket = socket;
        this.outputStream = outputStream;
        this.parser = new RespParser(new BufferedInputStream(inputStream));
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public Socket getSocket() {
        return socket;
    }

    public RespParser getParser() {
        return parser;
    }

    public boolean isMasterLink() {
        return masterLink;
    }

    public void setMasterLink(boolean masterLink) {
        this.masterLink = masterLink;
    }

    public int getListeningPort() {
        return listeningPort;
    }

    public void setListeningPort(int listeningPort) {
        this.listeningPort = listeningPort;
    }

    public List<String> getCapabilities() {
        return capabilities;
    }

    public String describe() {
        if (socket != null && socket.getRemoteSocketAddress() != null) {
            return "client#" + id + " (" + socket.getRemoteSocketAddress() + ")";
        }
        return "client#" + id;
    }

    // ------------------------------------------------------------- transactions

    public boolean isInTransaction() {
        return inTransaction;
    }

    /** @return false if a transaction is already open */
    public boolean beginTransaction() {
        if (inTransaction) {
            return false;
        }
        inTransaction = true;
        queuedCommands = new ArrayList<>();
        return true;
    }

    public void queueCommand(String[] command) {
        queuedCommands.add(command);
    }

    /** Closes the transaction and returns the commands that were queued in it. */
    public List<String[]> endTransaction() {
        List<String[]> queued = queuedCommands;
        queuedCommands = new ArrayList<>();
        inTransaction = false;
        return queued;
    }

    // ------------------------------------------------------------------ writing

    public void send(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            return;
        }
        writeLock.lock();
        try {
            outputStream.write(data);
            outputStream.flush();
        } finally {
            writeLock.unlock();
        }
    }

    public void send(String text) throws IOException {
        if (text != null && !text.isEmpty()) {
            send(text.getBytes(StandardCharsets.UTF_8));
        }
    }

    /** Sends the text and the raw bytes in one write so nothing can slip in between. */
    public void send(ResponseDto response) throws IOException {
        if (response == null) {
            return;
        }
        byte[] text = response.response == null ? new byte[0] : response.response.getBytes(StandardCharsets.UTF_8);
        byte[] data = response.data == null ? new byte[0] : response.data;
        byte[] all = new byte[text.length + data.length];
        System.arraycopy(text, 0, all, 0, text.length);
        System.arraycopy(data, 0, all, text.length, data.length);
        send(all);
    }

    public void close() {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
            // already closed
        }
    }
}
