package Components.Infra;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** A replica that has completed the handshake and is receiving our write stream. */
public class Slave {
    private final Client connection;
    private final List<String> capabilities = new CopyOnWriteArrayList<>();
    // highest replication offset this replica has confirmed (REPLCONF ACK)
    private volatile long ackedOffset;

    public Slave(Client connection, long initialOffset) {
        this.connection = connection;
        this.ackedOffset = initialOffset;
    }

    public Client getConnection() {
        return connection;
    }

    public List<String> getCapabilities() {
        return capabilities;
    }

    public long getAckedOffset() {
        return ackedOffset;
    }

    void setAckedOffset(long offset) {
        if (offset > ackedOffset) {
            ackedOffset = offset;
        }
    }

    public void send(byte[] bytes) throws IOException {
        connection.send(bytes);
    }
}
