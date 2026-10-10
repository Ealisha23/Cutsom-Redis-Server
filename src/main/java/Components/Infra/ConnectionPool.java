package Components.Infra;

import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tracks connected clients and replicas and owns the replication stream state.
 *
 * {@link #propagate(byte[])} must be called while holding the command lock (see
 * CommandHandler) so replicas see writes in exactly the order they were applied.
 */
@Component
public class ConnectionPool {
    private static final Logger logger = Logger.getLogger(ConnectionPool.class.getName());

    private final Set<Client> clients = ConcurrentHashMap.newKeySet();
    private final Map<Client, Slave> slaves = new ConcurrentHashMap<>();

    // total bytes of the replication stream sent to replicas = master_repl_offset
    private final AtomicLong propagatedBytes = new AtomicLong();
    private final Object ackMonitor = new Object();

    // ------------------------------------------------------------------ clients

    public void addClient(Client client) {
        if (client != null) {
            clients.add(client);
        }
    }

    public void removeClient(Client client) {
        clients.remove(client);
    }

    public Set<Client> getClients() {
        return clients;
    }

    // ------------------------------------------------------------------ replicas

    public void addSlave(Slave slave) {
        if (slave != null) {
            slaves.put(slave.getConnection(), slave);
        }
    }

    public Slave getSlave(Client client) {
        return slaves.get(client);
    }

    public void removeSlave(Client client) {
        if (slaves.remove(client) != null) {
            synchronized (ackMonitor) {
                ackMonitor.notifyAll(); // a waiting WAIT should recount
            }
        }
    }

    public Collection<Slave> getSlaves() {
        return slaves.values();
    }

    public int getSlaveCount() {
        return slaves.size();
    }

    // ------------------------------------------------------------- replication

    public long propagatedBytes() {
        return propagatedBytes.get();
    }

    /**
     * Sends bytes to every replica and advances the replication offset. Does nothing when
     * there are no replicas (the offset only counts what was actually streamed).
     */
    public void propagate(byte[] data) {
        if (slaves.isEmpty()) {
            return;
        }
        propagatedBytes.addAndGet(data.length);
        for (Slave slave : slaves.values()) {
            try {
                slave.send(data);
            } catch (IOException e) {
                logger.log(Level.WARNING, "Dropping replica {0}: {1}",
                        new Object[] {slave.getConnection().describe(), e.getMessage()});
                removeSlave(slave.getConnection());
                slave.getConnection().close();
            }
        }
    }

    /** Called when a replica answers REPLCONF GETACK. */
    public void slaveAck(Slave slave, long offset) {
        synchronized (ackMonitor) {
            slave.setAckedOffset(offset);
            ackMonitor.notifyAll();
        }
    }

    /** Number of replicas that have confirmed at least the given offset. */
    public int countAcked(long targetOffset) {
        int count = 0;
        for (Slave slave : slaves.values()) {
            if (slave.getAckedOffset() >= targetOffset) {
                count++;
            }
        }
        return count;
    }

    /**
     * Blocks until at least {@code required} replicas have acknowledged {@code targetOffset}
     * or the timeout passes (0 waits forever). Returns how many replicas acknowledged.
     */
    public int awaitAcks(long targetOffset, int required, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        synchronized (ackMonitor) {
            while (true) {
                int acked = countAcked(targetOffset);
                if (acked >= required) {
                    return acked;
                }
                if (timeoutMillis == 0) {
                    ackMonitor.wait();
                } else {
                    long remainingNanos = deadline - System.nanoTime();
                    if (remainingNanos <= 0) {
                        return acked;
                    }
                    ackMonitor.wait(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
                }
            }
        }
    }
}
