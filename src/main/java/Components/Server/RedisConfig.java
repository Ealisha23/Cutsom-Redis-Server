package Components.Server;

import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** Runtime settings shared by the servers and the command handler. */
@Component
public class RedisConfig {

    public static final String ROLE_MASTER = "master";
    public static final String ROLE_SLAVE = "slave";

    private volatile String role = ROLE_MASTER;
    private volatile int port = 6379;
    private volatile String masterHost;
    private volatile int masterPort;
    private volatile String masterReplId = generateReplId();
    private final AtomicLong masterReplOffset = new AtomicLong();

    private static String generateReplId() {
        // 40 hex characters, like a real Redis replication id
        return (UUID.randomUUID().toString().replace("-", "")
                + UUID.randomUUID().toString().replace("-", "")).substring(0, 40);
    }

    public boolean isReplica() {
        return ROLE_SLAVE.equals(role);
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public void setMasterHost(String masterHost) {
        this.masterHost = masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public void setMasterPort(int masterPort) {
        this.masterPort = masterPort;
    }

    public String getMasterReplId() {
        return masterReplId;
    }

    public void setMasterReplId(String masterReplId) {
        this.masterReplId = masterReplId;
    }

    /** On a replica: how many bytes of the master's stream have been processed. */
    public long getMasterReplOffset() {
        return masterReplOffset.get();
    }

    public void setMasterReplOffset(long offset) {
        masterReplOffset.set(offset);
    }

    public void addToMasterReplOffset(long bytes) {
        masterReplOffset.addAndGet(bytes);
    }
}
